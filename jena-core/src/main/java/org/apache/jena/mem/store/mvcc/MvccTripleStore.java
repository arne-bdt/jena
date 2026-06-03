/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *   SPDX-License-Identifier: Apache-2.0
 */

package org.apache.jena.mem.store.mvcc;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.IndexingStrategy;
import org.apache.jena.mem.store.TripleStore;
import org.apache.jena.mem.store.indexed.TripleSet;
import org.apache.jena.mem.store.mvcc.strategies.MvccEagerStoreStrategy;
import org.apache.jena.mem.store.mvcc.strategies.MvccManualStoreStrategy;
import org.apache.jena.mem.store.mvcc.strategies.MvccMinimalStoreStrategy;
import org.apache.jena.mem.store.mvcc.strategies.MvccStoreStrategy;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.SingletonIterator;
import org.apache.jena.util.iterator.WrappedIterator;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * The shared, version-stamped triple store underlying the MVCC transactional
 * graph. A single instance is shared by every transaction on a graph (and, for a
 * dataset, all per-graph stores share one {@link MvccVersionControl} timeline).
 *
 * <h2>Storage</h2>
 * Triples live in append-only parallel arrays, each slot identified by a stable
 * index: {@code keys[i]} (the triple), {@code since[i]} (version it became
 * visible) and {@code till[i]} ({@link #ALIVE} while live, else the version it was
 * deleted). A slot is visible at version {@code v} iff
 * {@code since[i] <= v && v < till[i]}. {@code since}/{@code till} are accessed
 * through a {@link VarHandle} in opaque mode to avoid word tearing.
 *
 * <h2>Publication</h2>
 * The published state is a single {@code volatile} {@link Gen} record carrying the
 * arrays, the {@code count}/{@code liveCount}/{@code version}, <em>and</em> the
 * auxiliary index. Bundling the index into the generation means one volatile read
 * gives a reader matching arrays + index even across a vacuum that renumbers slots.
 *
 * <h2>Writing (deferred apply)</h2>
 * A {@link MvccWriteTxn} buffers changes and applies them at
 * {@link #commit(MvccWriteTxn)} under the writer lock, then publishes a fresh
 * {@link Gen}; {@code abort} discards the overlay and touches nothing.
 *
 * <h2>Vacuum</h2>
 * Deletes are logical and re-adds append new slots, so the arrays and index lists
 * accumulate dead entries. {@link #vacuum()} (and an automatic trigger fused into
 * commit) compacts: it keeps only slots with {@code till > cutoff} where
 * {@code cutoff = }{@link MvccVersionControl#minActiveReadVersion()}, renumbers
 * survivors, rebuilds the index, and publishes a new generation. Old readers keep
 * their old generation (arrays + index) and are unaffected.
 *
 * <h2>Non-transactional facade</h2>
 * The class also implements the plain {@link TripleStore} contract, backing the
 * non-transactional {@link org.apache.jena.mem.GraphMemMvcc} graph: each
 * {@link #add}/{@link #remove}/{@link #clear} is its own atomic commit (applied
 * directly to the live generation, with no per-call write overlay) and reads run
 * against the latest committed generation. These convenience methods are
 * independent of the transactional {@code openWriteTxn}/{@code openReadView} API
 * above and add no overhead to it.
 */
public final class MvccTripleStore implements TripleStore {

    /** {@code till} value of a live slot: visible to every version. */
    public static final long ALIVE = Long.MAX_VALUE;

    /** Match-everything pattern, used by the no-argument {@link #stream()}. */
    private static final Triple ANY = Triple.create(Node.ANY, Node.ANY, Node.ANY);

    private static final int INITIAL_CAPACITY = 16;

    /** Below this slot count, never auto-vacuum (not worth it). */
    private static final int VACUUM_MIN_COUNT = 1024;

    /** Element-wise opaque access to the {@code long} version arrays. */
    private static final VarHandle LONGS = MethodHandles.arrayElementVarHandle(long[].class);

    /**
     * Immutable published snapshot: the dense arrays, the committed
     * {@code count}/{@code liveCount}/{@code version}, the auxiliary pattern
     * {@code index} and the full-triple {@code spo} index. The arrays and both
     * indices are shared with the live store between vacuums (the writer appends
     * beyond {@code count} and into the indices in place); only indices
     * {@code [0, count)} are meaningful to a reader holding this generation, and a
     * vacuum publishes an entirely fresh generation.
     */
    public record Gen(long version, Triple[] keys, long[] since, long[] till,
                      int count, int liveCount, MvccStoreStrategy index, MvccTripleIndex spo) {}

    private final MvccVersionControl vc;
    private final IndexingStrategy indexingStrategy;

    /** Published snapshot. Volatile: published by commit, acquired by begin(READ). */
    private volatile Gen gen;

    /** Create a store with its own version timeline. */
    public MvccTripleStore(IndexingStrategy indexingStrategy) {
        this(indexingStrategy, new MvccVersionControl());
    }

    /**
     * Create a store on a shared version timeline (used by a dataset so all graphs
     * commit on one clock).
     */
    public MvccTripleStore(IndexingStrategy indexingStrategy, MvccVersionControl vc) {
        this.vc = vc;
        this.indexingStrategy = indexingStrategy;
        final long[] since = new long[INITIAL_CAPACITY];
        final long[] till = new long[INITIAL_CAPACITY];
        Arrays.fill(since, ALIVE); // unused slots are invisible to everyone
        Arrays.fill(till, ALIVE);
        this.gen = new Gen(0L, new Triple[INITIAL_CAPACITY], since, till, 0, 0,
                createIndex(indexingStrategy), new MvccTripleIndex());
    }

    private MvccStoreStrategy createIndex(IndexingStrategy s) {
        return switch (s) {
            case EAGER -> new MvccEagerStoreStrategy();
            case MINIMAL -> new MvccMinimalStoreStrategy();
            case MANUAL -> new MvccManualStoreStrategy();
            case LAZY, LAZY_PARALLEL -> throw new UnsupportedOperationException(
                    "LAZY indexing is not supported by the MVCC store (read-triggered "
                    + "index builds are unsafe for lock-free readers); use EAGER, MINIMAL or MANUAL");
        };
    }

    /** @return the shared version control. */
    public MvccVersionControl versionControl() {
        return vc;
    }

    /** @return the indexing strategy this store was created with. */
    public IndexingStrategy getIndexingStrategy() {
        return indexingStrategy;
    }

    /** @return whether the auxiliary index is built and serving lookups. */
    public boolean isIndexInitialized() {
        return gen.index().isIndexInitialized();
    }

    /**
     * Build the auxiliary index over the currently-live committed slots, upgrading
     * a {@code MANUAL} or {@code MINIMAL} strategy to serve lookups from the index
     * (a no-op for {@code EAGER}, which is always indexed, or when the index is
     * already built). Must be called with the writer lock held. The build mutates
     * the current generation's index in place (published via the strategy's own
     * volatile), so no new generation is needed. Mirrors
     * {@link org.apache.jena.mem.store.roaring.RoaringTripleStore#initializeIndex()}.
     */
    public void initializeIndex() {
        buildAndInstallIndex(false);
    }

    /**
     * Like {@link #initializeIndex()} but builds the index in parallel: the
     * subject/predicate/object dimensions are populated on separate threads, so
     * every {@link MvccIndexList} still has a single writer and its append contract
     * holds. Must be called with the writer lock held.
     */
    public void initializeIndexParallel() {
        buildAndInstallIndex(true);
    }

    private void buildAndInstallIndex(boolean parallel) {
        final Gen g = gen;
        final MvccStoreStrategy idx = g.index();
        if (idx.isIndexInitialized()) {
            return;                             // EAGER, or already built
        }
        final MvccEagerStoreStrategy eager = buildEager(g, parallel);
        if (idx instanceof MvccManualStoreStrategy manual) {
            manual.install(eager);
        } else if (idx instanceof MvccMinimalStoreStrategy minimal) {
            minimal.install(eager);
        }
    }

    /**
     * Drop a built {@code MANUAL}/{@code MINIMAL} index and revert to that
     * strategy's un-built behaviour (MANUAL throws on partial patterns, MINIMAL
     * dense-scans). A no-op for {@code EAGER}, which stays fully indexed. Must be
     * called with the writer lock held. Mirrors
     * {@link org.apache.jena.mem.store.roaring.RoaringTripleStore#clearIndex()}.
     */
    public void clearIndex() {
        final MvccStoreStrategy idx = gen.index();
        if (idx instanceof MvccManualStoreStrategy manual) {
            manual.clear();
        } else if (idx instanceof MvccMinimalStoreStrategy minimal) {
            minimal.clear();
        }
        // EAGER: always indexed; clearing then re-indexing is observably a no-op,
        // so the eager index is left in place (isIndexInitialized stays true).
    }

    /**
     * Build a fresh eager index over the slots live at {@code g}'s version.
     * Sequential, or — when {@code parallel} — one dimension per thread. Each
     * {@link MvccIndexList} is therefore appended to by a single thread (subjects,
     * predicates and objects use disjoint maps), so the list's single-writer
     * append contract is preserved; mirrors the copy-on-write/Roaring 3-way split.
     */
    private MvccEagerStoreStrategy buildEager(Gen g, boolean parallel) {
        final MvccEagerStoreStrategy eager = new MvccEagerStoreStrategy();
        final Triple[] keys = g.keys();
        final int count = g.count();
        final long v = g.version();
        if (!parallel) {
            for (int slot = 0; slot < count; slot++) {
                if (visible(g, slot, v)) {
                    eager.onCommitAdd(keys[slot], slot);
                }
            }
            return eager;
        }
        final CompletableFuture<Void> subjects = CompletableFuture.runAsync(() -> {
            for (int slot = 0; slot < count; slot++) {
                if (visible(g, slot, v)) {
                    eager.appendSubject(keys[slot].getSubject(), slot);
                }
            }
        });
        final CompletableFuture<Void> predicates = CompletableFuture.runAsync(() -> {
            for (int slot = 0; slot < count; slot++) {
                if (visible(g, slot, v)) {
                    eager.appendPredicate(keys[slot].getPredicate(), slot);
                }
            }
        });
        for (int slot = 0; slot < count; slot++) {              // objects on this thread
            if (visible(g, slot, v)) {
                eager.appendObject(keys[slot].getObject(), slot);
            }
        }
        CompletableFuture.allOf(subjects, predicates).join();
        return eager;
    }

    // ---- Read views ----------------------------------------------------------

    /**
     * Open a read view pinned at the latest committed version, registered for
     * vacuum tracking. {@link MvccVersionControl#pinReader()} registers the version
     * race-free against a concurrent commit/vacuum; the generation is then read,
     * so it is always consistent with (or newer than) the pinned version.
     *
     * @return a lock-free, registered read view
     */
    public MvccReadView openReadView() {
        final long v = vc.pinReader();
        final Gen g = gen;
        return new MvccReadView(this, g, v, true);
    }

    /**
     * A transient, unregistered read view over the latest committed generation,
     * for reads outside any transaction. Needs no {@code close()}: it captures a
     * complete generation (arrays + index), so it is safe even if a later vacuum
     * compacts the store.
     */
    public MvccReadView transientReadView() {
        final Gen g = gen;
        return new MvccReadView(this, g, g.version(), false);
    }

    /** @return the current published generation (for the writer's committed view). */
    public Gen currentGen() {
        return gen;
    }

    /**
     * Diagnostic: the number of physical slots in the current generation (live
     * plus retained-dead). Shrinks after a {@link #vacuum()} reclaims dead slots.
     *
     * @return the physical slot count
     */
    public int physicalSlotCount() {
        return gen.count();
    }

    // ---- Version-parameterised reads against the latest generation -------------

    /** @return iterator over triples matching the pattern, visible at {@code version}. */
    public ExtendedIterator<Triple> findAt(long version, Triple match) {
        return find(gen, version, match);
    }

    /** @return whether some triple matches the pattern at {@code version}. */
    public boolean containsAt(long version, Triple match) {
        return contains(gen, version, match);
    }

    /** @return stream over triples matching the pattern, visible at {@code version}. */
    public Stream<Triple> streamAt(long version, Triple match) {
        return stream(gen, version, match);
    }

    /** @return the number of triples visible at {@code version}. */
    public int countAt(long version) {
        return countLive(gen, version);
    }

    /** @return whether the store has any triple visible at {@code version}. */
    public boolean isEmptyAt(long version) {
        final Gen g = gen;
        if (version >= g.version()) {
            return g.liveCount() == 0;
        }
        for (int i = 0; i < g.count(); i++) {
            if (visible(g, i, version)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Open a write transaction at {@code committedVersion + 1}, capturing the
     * current committed generation as its read-your-writes base. The caller must
     * hold the writer lock (see {@link MvccVersionControl#lockWriter()}).
     */
    public MvccWriteTxn openWriteTxn() {
        return new MvccWriteTxn(this, vc.nextWriteVersion(), gen);
    }

    // ---- Non-transactional TripleStore facade (auto-commit) --------------------
    //
    // These implement the plain TripleStore contract so the store can back a
    // non-transactional GraphMem (org.apache.jena.mem.GraphMemMvcc) and be
    // exercised by AbstractTripleStoreTest. Each mutation is its own atomic
    // commit: it grabs the writer slot, applies one change directly to the live
    // generation (no per-call write-overlay), publishes a new version and
    // releases. Reads run against the latest published generation, so they need
    // no transaction. The transactional commit/read-view paths above are not
    // touched by any of this.

    /**
     * {@inheritDoc}
     * <p>
     * Auto-commits a single add. Idempotent: a triple already committed-live is
     * left untouched (no new version is published).
     */
    @Override
    public void add(final Triple triple) {
        vc.lockWriter();
        try {
            if (committedContains(triple)) {
                return;                         // already live: nothing to do
            }
            applyAdd(triple, vc.nextWriteVersion());
        } finally {
            vc.unlockWriter();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Auto-commits a single (logical) delete. A triple that is absent or already
     * deleted is a no-op and publishes no new version.
     */
    @Override
    public void remove(final Triple triple) {
        vc.lockWriter();
        try {
            if (!committedContains(triple)) {
                return;                         // absent or already deleted
            }
            applyRemove(triple, vc.nextWriteVersion());
        } finally {
            vc.unlockWriter();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Publishes a fresh, empty generation. Readers holding an earlier generation
     * are unaffected (they keep their own arrays + index), so snapshot isolation
     * is preserved exactly as for a vacuum.
     */
    @Override
    public void clear() {
        vc.lockWriter();
        try {
            if (gen.count() == 0) {
                return;                         // already empty: no version bump
            }
            final long v = vc.nextWriteVersion();
            final long[] since = new long[INITIAL_CAPACITY];
            final long[] till = new long[INITIAL_CAPACITY];
            Arrays.fill(since, ALIVE);
            Arrays.fill(till, ALIVE);
            gen = new Gen(v, new Triple[INITIAL_CAPACITY], since, till, 0, 0,
                    createIndex(indexingStrategy), new MvccTripleIndex());
            vc.publish(v);
        } finally {
            vc.unlockWriter();
        }
    }

    /** {@inheritDoc} The number of triples live in the latest committed generation. */
    @Override
    public int countTriples() {
        return gen.liveCount();
    }

    @Override
    public boolean isEmpty() {
        return gen.liveCount() == 0;
    }

    @Override
    public boolean contains(final Triple tripleMatch) {
        final Gen g = gen;
        return contains(g, g.version(), tripleMatch);
    }

    @Override
    public ExtendedIterator<Triple> find(final Triple tripleMatch) {
        final Gen g = gen;
        return find(g, g.version(), tripleMatch);
    }

    @Override
    public Stream<Triple> stream() {
        final Gen g = gen;
        return stream(g, g.version(), ANY);
    }

    @Override
    public Stream<Triple> stream(final Triple tripleMatch) {
        final Gen g = gen;
        return stream(g, g.version(), tripleMatch);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns an independent store (its own version timeline) holding the
     * currently-live triples, built under a single write transaction. The
     * indexing strategy is preserved; the dead slots and version history of this
     * store are not carried over.
     */
    @Override
    public MvccTripleStore copy() {
        final Gen g = gen;
        final long v = g.version();
        final MvccTripleStore c = new MvccTripleStore(indexingStrategy);
        c.vc.lockWriter();
        try {
            final MvccWriteTxn w = c.openWriteTxn();
            for (int slot = 0; slot < g.count(); slot++) {
                if (visible(g, slot, v)) {
                    w.add(g.keys()[slot]);
                }
            }
            if (w.hasChanges()) {
                c.commit(w);
            }
        } finally {
            c.vc.unlockWriter();
        }
        return c;
    }

    /**
     * Append one triple at a fresh slot and publish a new generation. Mirrors the
     * add branch of {@link #commit(MvccWriteTxn)} for a single triple: stamp
     * {@code till} then {@code since} (written last, so a premature index
     * observation reads the invisible {@code ALIVE} default), update both indices,
     * then release-publish the new generation before advancing the clock. Must be
     * called with the writer lock held and only when {@code triple} is not
     * committed-live (callers check); a re-add of a deleted triple correctly
     * appends a new slot. Adds create no dead slots, so no auto-vacuum is needed.
     */
    private void applyAdd(final Triple triple, final long v) {
        final Gen g = gen;
        Triple[] keys = g.keys();
        long[] since = g.since();
        long[] till = g.till();
        final int count = g.count();

        if (count + 1 > keys.length) {
            int newLen = keys.length;
            while (newLen < count + 1) {
                final int grown = (newLen >> 1) + newLen;
                newLen = grown < 0 ? Integer.MAX_VALUE : grown;
            }
            keys = Arrays.copyOf(keys, newLen);
            since = growVersions(since, newLen);
            till = growVersions(till, newLen);
        }

        keys[count] = triple;
        LONGS.setOpaque(till, count, ALIVE);
        LONGS.setOpaque(since, count, v);
        g.index().onCommitAdd(triple, count);
        g.spo().put(triple, count);             // insert, or update a deleted entry

        gen = new Gen(v, keys, since, till, count + 1, g.liveCount() + 1, g.index(), g.spo());
        vc.publish(v);
    }

    /**
     * Stamp {@code till} on the committed-live slot of {@code triple} and publish
     * a new generation; auto-vacuums afterwards on the same dead-ratio rule as
     * {@link #commit(MvccWriteTxn)}. Must be called with the writer lock held and
     * only when {@code triple} is committed-live (callers check).
     */
    private void applyRemove(final Triple triple, final long v) {
        final Gen g = gen;
        final int slot = g.spo().slotOf(triple);   // known live (caller checked)
        LONGS.setOpaque(g.till(), slot, v);
        final Gen g1 = new Gen(v, g.keys(), g.since(), g.till(), g.count(),
                g.liveCount() - 1, g.index(), g.spo());
        gen = g1;
        vc.publish(v);
        if (shouldAutoVacuum(g1)) {
            gen = compact(g1, vc.minActiveReadVersion());
        }
    }

    // ---- Visibility & scanning -------------------------------------------------

    static boolean visible(Gen g, int slot, long version) {
        final long since = (long) LONGS.getOpaque(g.since(), slot);
        final long till = (long) LONGS.getOpaque(g.till(), slot);
        return since <= version && version < till;
    }

    /**
     * Visibility specialised for a scan whose {@code version >= g.version()} (the
     * common latest-snapshot read, including every read outside a transaction and
     * the writer's committed-base reads). Every slot in {@code [0, count)} was
     * committed at or before {@code g.version() <= version}, so {@code since <= version}
     * is implied and only {@code till} need be read — halving the per-slot opaque
     * reads on the hot path. For older snapshots ({@code version < g.version()})
     * the caller passes {@code latest == false} and the full {@link #visible} check
     * (both {@code since} and {@code till}) is used.
     */
    private static boolean visibleAt(Gen g, int slot, long version, boolean latest) {
        final long till = (long) LONGS.getOpaque(g.till(), slot);
        if (latest) {
            return version < till;
        }
        final long since = (long) LONGS.getOpaque(g.since(), slot);
        return since <= version && version < till;
    }

    /**
     * @return the number of live triples visible at {@code version} within
     *         generation {@code g}. O(1) when {@code version} is at or beyond the
     *         generation's own version (the common case); otherwise an O(count)
     *         scan for an older snapshot.
     */
    int countLive(Gen g, long version) {
        if (version >= g.version()) {
            return g.liveCount();
        }
        int c = 0;
        for (int i = 0; i < g.count(); i++) {
            if (visible(g, i, version)) {
                c++;
            }
        }
        return c;
    }

    /** Version-filtered iterator over a pattern, against a fixed generation/version. */
    ExtendedIterator<Triple> find(Gen g, long version, Triple match) {
        // Fully-concrete fast path: at most one slot of a given triple is visible at
        // any version (a re-add's since is >= the prior delete's till), so the
        // full-triple index resolves the single possible match in O(1). ABSENT
        // yields empty; a visible latest slot yields a singleton; a present but
        // not-visible slot (an older snapshot whose visible instance is an earlier
        // re-add) falls back to the scan, which finds that one instance.
        final Node sm = match.getSubject();
        final Node pm = match.getPredicate();
        final Node om = match.getObject();
        if (MvccStoreStrategy.isConcrete(sm) && MvccStoreStrategy.isConcrete(pm)
                && MvccStoreStrategy.isConcrete(om)) {
            final int slot = g.spo().slotOf(match);
            if (slot == MvccTripleIndex.ABSENT) {
                return NiceIterator.emptyIterator();
            }
            if (slot < g.count() && visibleAt(g, slot, version, version >= g.version())) {
                final Triple t = g.keys()[slot];
                if (t != null) {
                    return new SingletonIterator<>(t);
                }
            }
        }
        final MvccStoreStrategy.Candidates c = g.index().candidates(match);
        if (!c.dense() && c.list() == null) {
            return NiceIterator.emptyIterator();
        }
        return WrappedIterator.create(new SlotIterator(g, version, match, c));
    }

    /**
     * Existence test for a pattern at {@code version}, against a fixed generation.
     * Unlike {@code find(g, version, match).hasNext()} this allocates no iterator:
     * it scans the candidate slots (the shortest concrete index list, or the dense
     * range) directly, applying the version filter and full-pattern match, and
     * returns on the first hit. A pattern whose concrete components are absent from
     * the index is rejected in O(1) by {@link MvccStoreStrategy#candidates}.
     */
    boolean contains(Gen g, long version, Triple match) {
        final Triple[] keys = g.keys();
        final int count = g.count();
        final boolean latest = version >= g.version();
        final Node sm = match.getSubject();
        final Node pm = match.getPredicate();
        final Node om = match.getObject();
        // Fully-concrete fast path: the full-triple index gives the triple's latest
        // slot in O(1). ABSENT is definitive (no surviving slot is visible to any
        // current reader). A present-but-not-visible slot — deleted at the latest
        // version, or an older snapshot whose visible instance is an earlier re-add —
        // falls through to the scan below, which resolves it correctly.
        if (MvccStoreStrategy.isConcrete(sm) && MvccStoreStrategy.isConcrete(pm)
                && MvccStoreStrategy.isConcrete(om)) {
            final int slot = g.spo().slotOf(match);
            if (slot == MvccTripleIndex.ABSENT) {
                return false;
            }
            if (slot < count && visibleAt(g, slot, version, latest)) {
                return true;            // keys[slot] is the triple put with this slot
            }
        }
        final MvccStoreStrategy.Candidates c = g.index().candidates(match);
        if (!c.dense() && c.list() == null) {
            return false;
        }
        // Fold the keyed dimension into the "any" flags so the (implied) term match
        // on the dimension the list is keyed on is skipped (see Candidates#keyed);
        // a one-bound pattern then needs only the version filter.
        final MvccStoreStrategy.Dim keyed = c.keyed();
        final boolean anyS = !MvccStoreStrategy.isConcrete(sm) || keyed == MvccStoreStrategy.Dim.SUBJECT;
        final boolean anyP = !MvccStoreStrategy.isConcrete(pm) || keyed == MvccStoreStrategy.Dim.PREDICATE;
        final boolean anyO = !MvccStoreStrategy.isConcrete(om) || keyed == MvccStoreStrategy.Dim.OBJECT;
        if (c.dense()) {
            for (int slot = 0; slot < count; slot++) {
                if (visibleAt(g, slot, version, latest)) {
                    final Triple t = keys[slot];
                    if (t != null
                            && (anyS || sm.equals(t.getSubject()))
                            && (anyP || pm.equals(t.getPredicate()))
                            && (anyO || om.equals(t.getObject()))) {
                        return true;
                    }
                }
            }
            return false;
        }
        // Scan the chosen (shortest concrete) index list. Entries are clamped to
        // count so a concurrently grown list can never read past this generation's
        // slots, mirroring SlotIterator.
        final MvccIndexList.Snapshot snap = c.list().snapshot();
        final int[] list = snap.array();
        final int len = snap.length();
        for (int i = 0; i < len; i++) {
            final int slot = list[i];
            if (slot < count && visibleAt(g, slot, version, latest)) {
                final Triple t = keys[slot];
                if (t != null
                        && (anyS || sm.equals(t.getSubject()))
                        && (anyP || pm.equals(t.getPredicate()))
                        && (anyO || om.equals(t.getObject()))) {
                    return true;
                }
            }
        }
        return false;
    }

    Stream<Triple> stream(Gen g, long version, Triple match) {
        return Iter.asStream(find(g, version, match));
    }

    /**
     * Lookahead iterator over candidate slots applying the version filter and the
     * full-pattern term match. Operates entirely within {@code [0, g.count)} on the
     * generation's snapshot arrays, clamping list entries so a concurrently grown
     * index list can never cause an out-of-bounds access.
     */
    private final class SlotIterator implements Iterator<Triple> {
        private final Gen g;
        private final long version;
        private final Triple[] keys;
        private final int count;
        private final Node sm, pm, om;
        private final boolean anyS, anyP, anyO;
        private final boolean dense;
        private final boolean latest;
        private final int[] list;       // null when dense
        private final int listLen;
        private int cursor = 0;
        private Triple next;

        SlotIterator(Gen g, long version, Triple match, MvccStoreStrategy.Candidates c) {
            this.g = g;
            this.version = version;
            this.keys = g.keys();
            this.count = g.count();
            // version >= g.version() (the common latest read): since is implied, so
            // visibleAt reads only till. See visibleAt.
            this.latest = version >= g.version();
            this.sm = match.getSubject();
            this.pm = match.getPredicate();
            this.om = match.getObject();
            // Fold the keyed dimension into the "any" flags: every slot in the
            // chosen list already carries the pattern's node for that dimension, so
            // its term comparison is implied (see Candidates#keyed). A one-bound
            // pattern thus needs no term comparison at all, only the version filter.
            final MvccStoreStrategy.Dim keyed = c.keyed();
            this.anyS = !MvccStoreStrategy.isConcrete(sm) || keyed == MvccStoreStrategy.Dim.SUBJECT;
            this.anyP = !MvccStoreStrategy.isConcrete(pm) || keyed == MvccStoreStrategy.Dim.PREDICATE;
            this.anyO = !MvccStoreStrategy.isConcrete(om) || keyed == MvccStoreStrategy.Dim.OBJECT;
            this.dense = c.dense();
            if (dense) {
                this.list = null;
                this.listLen = 0;
            } else {
                final MvccIndexList.Snapshot snap = c.list().snapshot();
                this.list = snap.array();
                this.listLen = snap.length();
            }
            advance();
        }

        private boolean matches(Triple t) {
            return (anyS || sm.equals(t.getSubject()))
                    && (anyP || pm.equals(t.getPredicate()))
                    && (anyO || om.equals(t.getObject()));
        }

        private void advance() {
            next = null;
            if (dense) {
                while (cursor < count) {
                    final int slot = cursor++;
                    if (visibleAt(g, slot, version, latest)) {
                        final Triple t = keys[slot];
                        if (t != null && matches(t)) {
                            next = t;
                            return;
                        }
                    }
                }
            } else {
                while (cursor < listLen) {
                    final int slot = list[cursor++];
                    if (slot < count && visibleAt(g, slot, version, latest)) {
                        final Triple t = keys[slot];
                        if (t != null && matches(t)) {
                            next = t;
                            return;
                        }
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public Triple next() {
            final Triple t = next;
            if (t == null) {
                throw new NoSuchElementException();
            }
            advance();
            return t;
        }
    }

    // ---- Writer-side committed-state queries (writer lock held) ----------------

    boolean committedContains(Triple t) {
        final Gen g = gen;
        final int slot = g.spo().slotOf(t);
        // The full-triple index keeps deleted entries (an MVCC delete only stamps
        // till), so committed-live means the latest slot is still alive.
        return slot >= 0 && slot < g.count()
                && (long) LONGS.getOpaque(g.till(), slot) == ALIVE;
    }

    // ---- Commit application (writer lock held) ---------------------------------

    /**
     * Apply a write transaction's overlay to the shared store, publish a new
     * generation, and auto-vacuum if warranted. Called with the writer lock held.
     */
    public void commit(MvccWriteTxn txn) {
        final long v = txn.version();
        final Graph added = txn.added();
        final TripleSet removed = txn.removed();

        Gen g = gen;
        Triple[] keys = g.keys();
        long[] since = g.since();
        long[] till = g.till();
        int count = g.count();
        int liveCount = g.liveCount();
        final MvccStoreStrategy index = g.index();

        // Grow once, exactly, before any mutation so all stamps/appends land on the
        // final arrays.
        final int needed = count + added.size();
        if (needed > keys.length) {
            int newLen = keys.length;
            while (newLen < needed) {
                final int grown = (newLen >> 1) + newLen;
                newLen = grown < 0 ? Integer.MAX_VALUE : grown;
            }
            keys = Arrays.copyOf(keys, newLen);
            since = growVersions(since, newLen);
            till = growVersions(till, newLen);
        }

        // The full-triple index is shared with the live store between vacuums; the
        // writer appends/updates it in place here, readers see it via the new Gen.
        final MvccTripleIndex spo = g.spo();

        // Deletes: stamp till on the committed slot (monotonic, opaque). The index
        // keeps the entry (re-add will update it); committed-live means still alive.
        final ExtendedIterator<Triple> removedIt = removed.keyIterator();
        while (removedIt.hasNext()) {
            final Triple t = removedIt.next();
            final int slot = spo.slotOf(t);
            if (slot >= 0 && (long) LONGS.getOpaque(till, slot) == ALIVE) {
                LONGS.setOpaque(till, slot, v);
                liveCount--;
            }
        }

        // Adds: append a fresh slot. since is written last so any premature
        // observation of the slot (via an index) reads the invisible ALIVE default.
        final ExtendedIterator<Triple> addedIt = added.find();
        while (addedIt.hasNext()) {
            final Triple t = addedIt.next();
            final int slot = count++;
            keys[slot] = t;
            LONGS.setOpaque(till, slot, ALIVE);
            LONGS.setOpaque(since, slot, v);
            index.onCommitAdd(t, slot);
            spo.put(t, slot);          // insert, or update a previously-deleted entry
            liveCount++;
        }

        // Publish: new generation first (volatile release), then advance the clock.
        final Gen g1 = new Gen(v, keys, since, till, count, liveCount, index, spo);
        gen = g1;
        vc.publish(v);

        // Auto-vacuum on dead-ratio alone (see shouldAutoVacuum). Compacting at
        // minActiveReadVersion() is safe even when readers are active: it only
        // reclaims slots whose till has dropped at or below the oldest reader's
        // pinned version, so nothing any current or future reader can still see is
        // removed. A lagging reader therefore bounds how much is reclaimed, not
        // whether vacuum runs.
        if (shouldAutoVacuum(g1)) {
            gen = compact(g1, vc.minActiveReadVersion());
        }
    }

    private boolean shouldAutoVacuum(Gen g) {
        final int dead = g.count() - g.liveCount();
        return g.count() >= VACUUM_MIN_COUNT
                && dead * 2 >= g.count();   // >= 50% non-live
    }

    /**
     * Force a compaction of the current generation, reclaiming every slot deleted
     * at or before {@link MvccVersionControl#minActiveReadVersion()}. Must be
     * called with the writer lock held.
     */
    public void vacuum() {
        gen = compact(gen, vc.minActiveReadVersion());
    }

    /**
     * Build a compacted generation: keep only slots with {@code till > cutoff},
     * renumber survivors, rebuild the index over them, and remap the dedup table.
     * The old generation (arrays + index) is untouched, so readers holding it are
     * unaffected. Must be called with the writer lock held.
     */
    private Gen compact(Gen g, long cutoff) {
        final int n = g.count();
        final long committed = g.version();
        final Triple[] oldKeys = g.keys();
        final long[] oldSince = g.since();
        final long[] oldTill = g.till();

        int survivors = 0;
        for (int i = 0; i < n; i++) {
            if ((long) LONGS.getOpaque(oldTill, i) > cutoff) {
                survivors++;
            }
        }
        final int cap = Math.max(INITIAL_CAPACITY, survivors);
        final Triple[] nk = new Triple[cap];
        final long[] ns = new long[cap];
        final long[] nt = new long[cap];
        Arrays.fill(ns, ALIVE);
        Arrays.fill(nt, ALIVE);
        final MvccStoreStrategy nidx = freshCompactedIndex(g.index());
        final MvccTripleIndex nspo = new MvccTripleIndex(survivors);

        int w = 0;
        int newLive = 0;
        for (int i = 0; i < n; i++) {
            final long ti = (long) LONGS.getOpaque(oldTill, i);
            if (ti > cutoff) {
                nk[w] = oldKeys[i];
                LONGS.setOpaque(ns, w, (long) LONGS.getOpaque(oldSince, i));
                LONGS.setOpaque(nt, w, ti);
                nidx.onCommitAdd(oldKeys[i], w);  // index every survivor (live + retained-dead)
                // Survivors are visited in ascending old-slot order, so for a triple
                // with several survivors (a retained-dead slot plus a live re-add) the
                // last put wins — the latest (live) slot, exactly as required.
                nspo.put(oldKeys[i], w);
                if (ti > committed) {
                    newLive++;
                }
                w++;
            }
        }

        return new Gen(committed, nk, ns, nt, survivors, newLive, nidx, nspo);
    }

    private MvccStoreStrategy freshCompactedIndex(MvccStoreStrategy old) {
        return switch (indexingStrategy) {
            case EAGER -> new MvccEagerStoreStrategy();
            case MINIMAL -> {
                final MvccMinimalStoreStrategy m = new MvccMinimalStoreStrategy();
                if (old.isIndexInitialized()) {
                    m.install(new MvccEagerStoreStrategy());  // preserve a built index; onCommitAdd repopulates it
                }
                yield m;
            }
            case MANUAL -> {
                final MvccManualStoreStrategy m = new MvccManualStoreStrategy();
                if (old.isIndexInitialized()) {
                    m.install(new MvccEagerStoreStrategy());  // onCommitAdd will populate it
                }
                yield m;
            }
            case LAZY, LAZY_PARALLEL -> throw new UnsupportedOperationException("unreachable");
        };
    }

    private static long[] growVersions(long[] old, int newLen) {
        final long[] grown = Arrays.copyOf(old, newLen);
        Arrays.fill(grown, old.length, newLen, ALIVE);
        return grown;
    }
}

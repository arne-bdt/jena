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
import org.apache.jena.mem.store.indexed.TripleSet;
import org.apache.jena.mem.store.mvcc.strategies.MvccEagerStoreStrategy;
import org.apache.jena.mem.store.mvcc.strategies.MvccManualStoreStrategy;
import org.apache.jena.mem.store.mvcc.strategies.MvccMinimalStoreStrategy;
import org.apache.jena.mem.store.mvcc.strategies.MvccStoreStrategy;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.WrappedIterator;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
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
 */
public final class MvccTripleStore {

    /** {@code till} value of a live slot: visible to every version. */
    public static final long ALIVE = Long.MAX_VALUE;

    private static final int INITIAL_CAPACITY = 16;

    /** Below this slot count, never auto-vacuum (not worth it). */
    private static final int VACUUM_MIN_COUNT = 1024;

    /** Element-wise opaque access to the {@code long} version arrays. */
    private static final VarHandle LONGS = MethodHandles.arrayElementVarHandle(long[].class);

    /**
     * Immutable published snapshot: the dense arrays, the committed
     * {@code count}/{@code liveCount}/{@code version}, and the auxiliary
     * {@code index}. The arrays and index are shared with the live store between
     * vacuums (the writer appends beyond {@code count} and into the index lists in
     * place); only indices {@code [0, count)} are meaningful to a reader holding
     * this generation, and a vacuum publishes an entirely fresh generation.
     */
    public record Gen(long version, Triple[] keys, long[] since, long[] till,
                      int count, int liveCount, MvccStoreStrategy index) {}

    private final MvccVersionControl vc;
    private final IndexingStrategy indexingStrategy;

    /** Published snapshot. Volatile: published by commit, acquired by begin(READ). */
    private volatile Gen gen;

    // Writer-private dedup: the set of committed-live triples, with a parallel
    // primitive int[] mapping each set entry's stable index to its version slot.
    // Reuses the FastHashSet machinery (no boxing) like EagerStoreStrategy's
    // reverse-index arrays. Touched only under the writer lock; never by readers.
    private final TripleSet committedLive = new TripleSet();
    private int[] slotOf;

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
        this.slotOf = new int[committedLive.getInternalKeysLength()];
        committedLive.setOnKeysGrowHook(n -> slotOf = Arrays.copyOf(slotOf, n));
        final long[] since = new long[INITIAL_CAPACITY];
        final long[] till = new long[INITIAL_CAPACITY];
        Arrays.fill(since, ALIVE); // unused slots are invisible to everyone
        Arrays.fill(till, ALIVE);
        this.gen = new Gen(0L, new Triple[INITIAL_CAPACITY], since, till, 0, 0,
                createIndex(indexingStrategy));
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
     * Build the auxiliary index over the currently-live committed slots (a no-op
     * unless the configured strategy is {@code MANUAL} and not yet built). Must be
     * called with the writer lock held. The build mutates the current generation's
     * index in place (published via the strategy's own volatile), so no new
     * generation is needed.
     */
    public void initializeIndex() {
        final Gen g = gen;
        if (g.index() instanceof MvccManualStoreStrategy manual && !manual.isIndexInitialized()) {
            final MvccEagerStoreStrategy eager = new MvccEagerStoreStrategy();
            for (int slot = 0; slot < g.count(); slot++) {
                if (visible(g, slot, g.version())) {
                    eager.onCommitAdd(g.keys()[slot], slot);
                }
            }
            manual.install(eager);
        }
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

    // ---- Visibility & scanning -------------------------------------------------

    static boolean visible(Gen g, int slot, long version) {
        final long since = (long) LONGS.getOpaque(g.since(), slot);
        final long till = (long) LONGS.getOpaque(g.till(), slot);
        return since <= version && version < till;
    }

    /**
     * Term-based pattern match: a non-concrete pattern component is a wildcard; a
     * concrete component must be {@link Object#equals equal}.
     */
    public static boolean matches(Triple pattern, Triple t) {
        final Node s = pattern.getSubject();
        final Node p = pattern.getPredicate();
        final Node o = pattern.getObject();
        return (!MvccStoreStrategy.isConcrete(s) || s.equals(t.getSubject()))
                && (!MvccStoreStrategy.isConcrete(p) || p.equals(t.getPredicate()))
                && (!MvccStoreStrategy.isConcrete(o) || o.equals(t.getObject()));
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
        final MvccStoreStrategy.Candidates c = g.index().candidates(match);
        if (!c.dense() && c.list() == null) {
            return false;
        }
        final Triple[] keys = g.keys();
        final int count = g.count();
        if (c.dense()) {
            for (int slot = 0; slot < count; slot++) {
                if (visible(g, slot, version)) {
                    final Triple t = keys[slot];
                    if (t != null && matches(match, t)) {
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
            if (slot < count && visible(g, slot, version)) {
                final Triple t = keys[slot];
                if (t != null && matches(match, t)) {
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
        private final int[] list;       // null when dense
        private final int listLen;
        private int cursor = 0;
        private Triple next;

        SlotIterator(Gen g, long version, Triple match, MvccStoreStrategy.Candidates c) {
            this.g = g;
            this.version = version;
            this.keys = g.keys();
            this.count = g.count();
            this.sm = match.getSubject();
            this.pm = match.getPredicate();
            this.om = match.getObject();
            this.anyS = !MvccStoreStrategy.isConcrete(sm);
            this.anyP = !MvccStoreStrategy.isConcrete(pm);
            this.anyO = !MvccStoreStrategy.isConcrete(om);
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
                    if (visible(g, slot, version)) {
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
                    if (slot < count && visible(g, slot, version)) {
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
        return committedLive.containsKey(t);
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

        // Deletes: stamp till on the committed slot (monotonic, opaque).
        final ExtendedIterator<Triple> removedIt = removed.keyIterator();
        while (removedIt.hasNext()) {
            final Triple t = removedIt.next();
            final int i = committedLive.removeAndGetIndex(t);
            if (i >= 0) {
                LONGS.setOpaque(till, slotOf[i], v);
                liveCount--;
            }
        }

        // Adds: append a fresh slot. since is written last so any premature
        // observation of the slot (via the index) reads the invisible ALIVE default.
        final ExtendedIterator<Triple> addedIt = added.find();
        while (addedIt.hasNext()) {
            final Triple t = addedIt.next();
            final int slot = count++;
            keys[slot] = t;
            LONGS.setOpaque(till, slot, ALIVE);
            LONGS.setOpaque(since, slot, v);
            index.onCommitAdd(t, slot);
            int i = committedLive.addAndGetIndex(t);
            if (i < 0) {
                i = ~i;
            }
            slotOf[i] = slot;
            liveCount++;
        }

        // Publish: new generation first (volatile release), then advance the clock.
        final Gen g1 = new Gen(v, keys, since, till, count, liveCount, index);
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
        final int[] oldToNew = new int[n];
        Arrays.fill(oldToNew, -1);

        int w = 0;
        int newLive = 0;
        for (int i = 0; i < n; i++) {
            final long ti = (long) LONGS.getOpaque(oldTill, i);
            if (ti > cutoff) {
                oldToNew[i] = w;
                nk[w] = oldKeys[i];
                LONGS.setOpaque(ns, w, (long) LONGS.getOpaque(oldSince, i));
                LONGS.setOpaque(nt, w, ti);
                nidx.onCommitAdd(oldKeys[i], w);  // index every survivor (live + retained-dead)
                if (ti > committed) {
                    newLive++;
                }
                w++;
            }
        }

        // Remap the dedup table: every committed-live triple survived, so its
        // recorded slot is renumbered through oldToNew.
        committedLive.forEachKey((t, i) -> slotOf[i] = oldToNew[slotOf[i]]);

        return new Gen(committed, nk, ns, nt, survivors, newLive, nidx);
    }

    private MvccStoreStrategy freshCompactedIndex(MvccStoreStrategy old) {
        return switch (indexingStrategy) {
            case EAGER -> new MvccEagerStoreStrategy();
            case MINIMAL -> new MvccMinimalStoreStrategy();
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

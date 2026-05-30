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
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.IndexingStrategy;
import org.apache.jena.mem.store.mvcc.strategies.MvccEagerStoreStrategy;
import org.apache.jena.mem.store.mvcc.strategies.MvccManualStoreStrategy;
import org.apache.jena.mem.store.mvcc.strategies.MvccMinimalStoreStrategy;
import org.apache.jena.mem.store.mvcc.strategies.MvccStoreStrategy;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.WrappedIterator;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
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
 * index:
 * <ul>
 *   <li>{@code keys[i]} — the triple;</li>
 *   <li>{@code since[i]} — the version at which slot {@code i} became visible;</li>
 *   <li>{@code till[i]} — the version at which it was deleted ({@link #ALIVE} while
 *       live). {@code till} is the only field mutated after a slot is created, and
 *       only monotonically (live → deleted), so an older reader is never harmed.</li>
 * </ul>
 * A slot is visible at version {@code v} iff {@code since[i] <= v && v < till[i]}.
 * {@code since}/{@code till} are accessed through a {@link VarHandle} in
 * <em>opaque</em> mode to avoid word tearing under the concurrent
 * delete-stamp / read.
 *
 * <h2>Publication</h2>
 * The published state is a single {@code volatile} {@link Gen} record carrying the
 * array references plus the committed {@code count}/{@code liveCount} and the
 * version they correspond to. A reader takes its snapshot with one volatile read
 * of {@link #gen} at {@code begin(READ)} — O(1), no copy. The volatile read/write
 * pair on {@code gen} is the sole cross-thread fence.
 *
 * <h2>Writing (deferred apply)</h2>
 * A {@link MvccWriteTxn} buffers its changes in an overlay and applies them to the
 * shared arrays only at {@link #commit(MvccWriteTxn)}, under the writer lock, then
 * publishes a fresh {@link Gen}. Consequently:
 * <ul>
 *   <li>{@code abort} discards the overlay and touches nothing — no undo log;</li>
 *   <li>concurrent readers, clamped to their snapshot's {@code count}, never
 *       observe a half-applied slot, and the {@code commit} that publishes the new
 *       {@code Gen} establishes happens-before for any later reader;</li>
 *   <li>delete-stamps land on committed slots an older reader may share, but are
 *       version-safe (the delete is in that reader's future).</li>
 * </ul>
 * Writer-private bookkeeping ({@link #committedLive}, the dedup map of committed
 * live triple → slot) is touched only under the writer lock.
 */
public final class MvccTripleStore {

    /** {@code till} value of a live slot: visible to every version. */
    public static final long ALIVE = Long.MAX_VALUE;

    private static final int INITIAL_CAPACITY = 16;

    /** Element-wise opaque access to the {@code long} version arrays. */
    private static final VarHandle LONGS = MethodHandles.arrayElementVarHandle(long[].class);

    /**
     * Immutable published snapshot of the dense storage. {@code keys}/{@code since}/
     * {@code till} are shared with the live store (the writer appends beyond
     * {@code count} and stamps {@code till} in place), but only indices
     * {@code [0, count)} are meaningful to a reader holding this generation, and
     * {@code count}/{@code liveCount}/{@code version} are fixed for it.
     */
    public record Gen(long version, Triple[] keys, long[] since, long[] till, int count, int liveCount) {}

    private final MvccVersionControl vc;
    private final IndexingStrategy indexingStrategy;

    /** Auxiliary index; read concurrently by readers, mutated by the writer at commit. */
    private final MvccStoreStrategy strategy;

    /** Published snapshot. Volatile: published by commit, acquired by begin(READ). */
    private volatile Gen gen;

    /** Writer-private: committed live triple → its slot. Touched only under the writer lock. */
    private final java.util.HashMap<Triple, Integer> committedLive = new java.util.HashMap<>();

    /** Create a store with its own version timeline. */
    public MvccTripleStore(IndexingStrategy indexingStrategy) {
        this(indexingStrategy, new MvccVersionControl());
    }

    /**
     * Create a store on a shared version timeline (used by a dataset so all graphs
     * commit on one clock).
     *
     * @param indexingStrategy the indexing strategy
     * @param vc               the shared version control
     */
    public MvccTripleStore(IndexingStrategy indexingStrategy, MvccVersionControl vc) {
        this.vc = vc;
        this.indexingStrategy = indexingStrategy;
        this.strategy = createStrategy(indexingStrategy);
        final long[] since = new long[INITIAL_CAPACITY];
        final long[] till = new long[INITIAL_CAPACITY];
        java.util.Arrays.fill(since, ALIVE); // unused slots are invisible to everyone
        java.util.Arrays.fill(till, ALIVE);
        this.gen = new Gen(0L, new Triple[INITIAL_CAPACITY], since, till, 0, 0);
    }

    private MvccStoreStrategy createStrategy(IndexingStrategy s) {
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
        return strategy.isIndexInitialized();
    }

    /**
     * Build the auxiliary index over the currently-live committed slots (a no-op
     * unless the configured strategy is {@code MANUAL} and not yet built). Must be
     * called with the writer lock held.
     */
    public void initializeIndex() {
        if (strategy instanceof MvccManualStoreStrategy manual && !manual.isIndexInitialized()) {
            final MvccEagerStoreStrategy eager = new MvccEagerStoreStrategy();
            final Gen g = gen;
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
     * Open a read view pinned at the latest committed version. A single volatile
     * read of {@link #gen} fixes both the snapshot and its version, so reader state
     * is internally consistent without a separate version read.
     *
     * @return a lock-free read view
     */
    public MvccReadView openReadView() {
        final Gen g = gen;                 // volatile acquire
        vc.registerReader(g.version());
        return new MvccReadView(this, g, true);
    }

    /**
     * A transient, unregistered read view over the latest committed generation,
     * for reads outside any transaction. Needs no {@code close()} — it does not
     * participate in vacuum tracking; the returned iterators/streams hold the
     * generation snapshot directly.
     *
     * @return an unregistered read view at the current committed version
     */
    public MvccReadView transientReadView() {
        return new MvccReadView(this, gen, false);
    }

    /** @return the current published generation (for the writer's committed view). */
    public Gen currentGen() {
        return gen;
    }

    /**
     * Open a write transaction at {@code committedVersion + 1}, capturing the
     * current committed generation as its read-your-writes base. The caller must
     * hold the writer lock (see {@link MvccVersionControl#lockWriter()}).
     *
     * @return a new write transaction
     */
    public MvccWriteTxn openWriteTxn() {
        return new MvccWriteTxn(this, vc.nextWriteVersion(), gen);
    }

    // ---- Visibility & scanning (shared by read view and writer committed view) --

    static boolean visible(Gen g, int slot, long version) {
        final long since = (long) LONGS.getOpaque(g.since(), slot);
        final long till = (long) LONGS.getOpaque(g.till(), slot);
        return since <= version && version < till;
    }

    /**
     * Term-based pattern match: a non-concrete pattern component
     * ({@code Node.ANY}, a variable, or {@code null}) is a wildcard; a concrete
     * component must be {@link Object#equals equal}.
     *
     * @param pattern the lookup pattern
     * @param t       a candidate triple
     * @return {@code true} iff {@code t} matches {@code pattern}
     */
    public static boolean matches(Triple pattern, Triple t) {
        final Node s = pattern.getSubject();
        final Node p = pattern.getPredicate();
        final Node o = pattern.getObject();
        return (!MvccStoreStrategy.isConcrete(s) || s.equals(t.getSubject()))
                && (!MvccStoreStrategy.isConcrete(p) || p.equals(t.getPredicate()))
                && (!MvccStoreStrategy.isConcrete(o) || o.equals(t.getObject()));
    }

    /** @return the number of live triples visible at the given generation. */
    int countLive(Gen g) {
        return g.liveCount();
    }

    /** Version-filtered iterator over a pattern, against a fixed generation/version. */
    ExtendedIterator<Triple> find(Gen g, long version, Triple match) {
        final MvccStoreStrategy.Candidates c = strategy.candidates(match);
        if (!c.dense() && c.list() == null) {
            return NiceIterator.emptyIterator();
        }
        return WrappedIterator.create(new SlotIterator(g, version, match, c));
    }

    boolean contains(Gen g, long version, Triple match) {
        // Fully-concrete fast path still routes through the candidate scan so the
        // answer is version-correct (the dedup map reflects only committed state
        // and is writer-private). The scan short-circuits on the first match.
        return find(g, version, match).hasNext();
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
        // pattern terms (null component => wildcard)
        private final Node sm, pm, om;
        private final boolean anyS, anyP, anyO;
        // candidate source: dense range, or a snapshot of an index list
        private final boolean dense;
        private final int[] list;       // null when dense
        private final int listLen;      // valid when !dense
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
     * Apply a write transaction's overlay to the shared store and publish a new
     * generation. Called with the writer lock held.
     *
     * @param txn the write transaction to commit
     */
    public void commit(MvccWriteTxn txn) {
        final long v = txn.version();
        final java.util.Set<Triple> added = txn.added();
        final java.util.Set<Triple> removed = txn.removed();

        Gen g = gen;
        Triple[] keys = g.keys();
        long[] since = g.since();
        long[] till = g.till();
        int count = g.count();
        int liveCount = g.liveCount();

        // Grow once, exactly, to the final needed capacity, before any mutation so
        // all stamps/appends land on the final arrays.
        final int needed = count + added.size();
        if (needed > keys.length) {
            int newLen = keys.length;
            while (newLen < needed) {
                final int grown = (newLen >> 1) + newLen;
                newLen = grown < 0 ? Integer.MAX_VALUE : grown;
            }
            keys = java.util.Arrays.copyOf(keys, newLen);
            since = growVersions(since, newLen);
            till = growVersions(till, newLen);
        }

        // Deletes: stamp till on the committed slot (monotonic, opaque).
        for (Triple t : removed) {
            final Integer slot = committedLive.remove(t);
            if (slot != null) {
                LONGS.setOpaque(till, slot, v);
                liveCount--;
            }
        }

        // Adds: append a fresh slot. since is written last so any premature
        // observation of the slot (via the index) reads the invisible ALIVE default.
        for (Triple t : added) {
            final int slot = count++;
            keys[slot] = t;
            LONGS.setOpaque(till, slot, ALIVE);
            LONGS.setOpaque(since, slot, v);
            strategy.onCommitAdd(t, slot);
            committedLive.put(t, slot);
            liveCount++;
        }

        // Publish: new generation first (volatile release), then advance the clock.
        gen = new Gen(v, keys, since, till, count, liveCount);
        vc.publish(v);
    }

    private static long[] growVersions(long[] old, int newLen) {
        final long[] grown = java.util.Arrays.copyOf(old, newLen);
        java.util.Arrays.fill(grown, old.length, newLen, ALIVE);
        return grown;
    }
}

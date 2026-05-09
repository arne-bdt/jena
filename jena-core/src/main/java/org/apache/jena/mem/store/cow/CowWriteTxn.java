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

package org.apache.jena.mem.store.cow;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem.IndexingStrategy;
import org.apache.jena.mem.store.cow.strategies.CowEagerStoreStrategy;
import org.apache.jena.mem.store.cow.strategies.CowLazyStoreStrategy;
import org.apache.jena.mem.store.cow.strategies.CowStoreStrategy;

/**
 * The mutable side of the copy-on-write triple store: the writer's
 * private working copy. A graph hands one of these to the active write
 * transaction; on commit, {@link #freeze()} packages the writer's state
 * into a fresh {@link CowSnapshot} that the graph CASes into its
 * published slot.
 *
 * <h2>Mutation contract</h2>
 * Inside the writer, mutations may freely append to the COW-shared
 * spines (which are append-only past the source's high-water mark,
 * with tombstones for removed entries) and grow the writer-private
 * arrays (reverse-indices, deleted bitmaps, ownership bitmaps) inline.
 * The strategy's {@code addToIndex} resizes its writer-private arrays
 * directly when the keys array grows; no callback or growth hook is
 * needed because only this writer ever grows the keys array.
 *
 * <h2>Freezing</h2>
 * {@link #freeze()} produces a new {@link CowSnapshot} that aliases the
 * same underlying {@link TxnTripleSet} and strategy. After freezing,
 * the caller must treat <i>this</i> instance as dead: no further
 * mutations are permitted (the caller is the graph; it drops the
 * reference after publishing).
 */
public final class CowWriteTxn extends CowStore {

    /**
     * True if any of {@link #resetIndexStrategy()},
     * {@link #initializeIndex()}, or
     * {@link #initializeIndexParallel()} mutated the strategy slot. The
     * graph reads this at commit to decide whether a strategy upgrade
     * is worth publishing even when no triples were added or removed.
     */
    private volatile boolean strategyChanged = false;

    /**
     * Empty mutable store using {@link IndexingStrategy#EAGER}. Equivalent
     * to {@code new CowSnapshot().forkForWrite()} but skips the empty
     * snapshot allocation; useful for tests and for callers building a
     * graph from scratch outside any transaction lifecycle.
     */
    public CowWriteTxn() {
        this(IndexingStrategy.EAGER);
    }

    /** Empty mutable store using the given indexing strategy. */
    public CowWriteTxn(IndexingStrategy indexingStrategy) {
        // Writer's triples may grow on add; install the keys-grow hook
        // so the eager strategy's reverse-index arrays grow in
        // lock-step and addToIndex can skip a length check on the hot
        // path.
        super(indexingStrategy, /*installGrowHook*/ true);
    }

    CowWriteTxn(IndexingStrategy initialStrategy,
                TxnTripleSet triples,
                CowStoreStrategy strategy) {
        super(initialStrategy, triples, strategy);
    }

    /** Set the initial strategy from outside the constructor (used by the fork path). */
    void installStrategy(CowStoreStrategy s) {
        this.strategy.set(s);
    }

    // -------------------------------------------------------------------
    // Strategy controls (writer-only)

    /**
     * Drop the current strategy and re-install a fresh one of the
     * configured {@link #getIndexingStrategy()} kind. Subsequent lookups
     * follow the initial strategy's rules. Writer-side path: the new
     * eager strategy (if applicable) installs the keys-grow hook.
     */
    public void resetIndexStrategy() {
        strategy.set(buildInitialStrategy(initialStrategy, /*installGrowHook*/ true));
        strategyChanged = true;
    }

    /**
     * Build the eager index sequentially and install it as the current
     * strategy. Writer-side path: installs the keys-grow hook.
     */
    public void initializeIndex() {
        strategy.set(new CowEagerStoreStrategy(triples, false, /*installGrowHook*/ true));
        strategyChanged = true;
    }

    /** Like {@link #initializeIndex()} but builds in parallel. */
    public void initializeIndexParallel() {
        strategy.set(new CowEagerStoreStrategy(triples, true, /*installGrowHook*/ true));
        strategyChanged = true;
    }

    /**
     * @return whether the strategy reference has been mutated since
     * this writer was forked. Used by the transactional graph to detect
     * "writer auto-built the index" (publish-worthy even with no data
     * changes).
     */
    public boolean wasStrategyChanged() {
        return strategyChanged;
    }

    /**
     * Same CAS as the inherited
     * {@link CowStore#tryInstallEagerStrategy(CowLazyStoreStrategy, CowEagerStoreStrategy)},
     * but additionally flips {@link #strategyChanged} on success so the
     * graph treats a writer-side LAZY → EAGER auto-upgrade as
     * publish-worthy at commit even with no data mutations. The snapshot
     * variant doesn't need this flag (snapshots are never published
     * again — they ARE the published view).
     */
    @Override
    public boolean tryInstallEagerStrategy(CowLazyStoreStrategy expected,
                                           CowEagerStoreStrategy built) {
        if (super.tryInstallEagerStrategy(expected, built)) {
            strategyChanged = true;
            return true;
        }
        return false;
    }

    // -------------------------------------------------------------------
    // Mutation

    /** Add a triple. Does nothing if already present. */
    public void add(Triple t) {
        final int idx = triples.addAndGetIndex(t);
        if (idx < 0) return;                 // already present
        strategy.get().addToIndex(t, idx);
    }

    /** Remove a triple. Does nothing if not present. */
    public void remove(Triple t) {
        final int idx = triples.removeAndGetIndex(t);
        if (idx < 0) return;                 // not present
        strategy.get().removeFromIndex(t, idx);
    }

    /** Remove every triple. */
    public void clear() {
        triples.clear();
        strategy.get().clearIndex();
    }

    // -------------------------------------------------------------------
    // Freezing

    /**
     * Hand back a fresh {@link CowSnapshot} that aliases this writer's
     * triple set and strategy. After this call, treat {@code this} as
     * dead — only the returned snapshot is valid.
     * <p>
     * Two strategy-specific patches happen here:
     * <ul>
     *   <li>If the current strategy is a {@link CowLazyStoreStrategy}
     *       its enclosing-store reference is rebound to the new
     *       snapshot so the lazy auto-build CAS targets the snapshot's
     *       strategy slot, not the writer's.
     *   <li>If the current strategy is a {@link CowEagerStoreStrategy}
     *       the writer-only ownership bitmaps inside its three spines
     *       are released. Snapshots only serve reads and never
     *       consult the bitmaps; freeing them avoids carrying the
     *       writer-only tracking memory along with every published
     *       snapshot. Subsequent forks of the snapshot allocate a
     *       fresh bitmap on the new writer's spines via the spine
     *       fork constructor.
     * </ul>
     * Other strategies (manual, minimal) carry no writer-only state
     * and are aliased as-is.
     */
    public CowSnapshot freeze() {
        final CowStoreStrategy s = strategy.get();
        if (s instanceof CowEagerStoreStrategy eager) {
            eager.freeWriterOwnedBitmaps();
        }
        final CowSnapshot snap = new CowSnapshot(initialStrategy, triples, s);
        if (s instanceof CowLazyStoreStrategy lazy) {
            snap.installStrategy(new CowLazyStoreStrategy(snap, lazy.isParallel()));
        }
        return snap;
    }

    /**
     * Convenience: {@link #freeze()} this writer and immediately
     * {@linkplain CowSnapshot#forkForWrite() fork} the resulting
     * snapshot. After the call, both {@code this} and the returned
     * snapshot are forks of the same logical state; the discipline is
     * the same as for any fork — treat {@code this} as frozen and only
     * mutate the returned writer. Provided so test code that wants to
     * exercise the fork machinery does not need to thread an explicit
     * {@code freeze()} call between every fork.
     */
    public CowWriteTxn forkForWrite() {
        return freeze().forkForWrite();
    }

    /** Parallel-fork analogue of {@link #forkForWrite()}. */
    public CowWriteTxn forkForWriteParallel() {
        return freeze().forkForWriteParallel();
    }
}

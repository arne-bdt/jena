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

import org.apache.jena.mem.IndexingStrategy;
import org.apache.jena.mem.store.cow.strategies.CowEagerStoreStrategy;
import org.apache.jena.mem.store.cow.strategies.CowLazyStoreStrategy;
import org.apache.jena.mem.store.cow.strategies.CowStoreStrategy;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Read-only view of a copy-on-write triple store. The graph publishes a
 * {@code CowSnapshot} as the visible state for any number of concurrent
 * readers; mutation requires forking a {@link CowWriteTxn} via
 * {@link #forkForWrite()} or {@link #forkForWriteParallel()} and
 * eventually {@link CowWriteTxn#freeze() freezing} it back into a fresh
 * snapshot to publish.
 *
 * <h2>What "read-only" means</h2>
 * The triple set and the writer-private slices of any spines that back
 * the strategy are treated as immutable from the moment publication
 * happens. The one apparent exception is the strategy slot itself: a
 * lazy strategy may CAS-install a freshly built eager strategy on first
 * lookup. That mutation is semantically a cache: it does not change
 * which triples the snapshot answers for, only how it answers.
 *
 * <h2>Forks</h2>
 * Both {@link #forkForWrite()} and {@link #forkForWriteParallel()}
 * return a fresh {@link CowWriteTxn}. The published snapshot is
 * unaffected by mutations on the writer; the snapshot remains valid for
 * as long as readers hold a reference, even after the writer commits.
 */
public final class CowSnapshot extends CowStore {

    /**
     * The current pluggable strategy. {@code volatile} so that:
     * <ul>
     *   <li>The writer's freeze-time install of (e.g.) a rebound LAZY
     *       strategy is published to every reader along with the
     *       snapshot itself.
     *   <li>Reader-driven LAZY → EAGER auto-upgrades are visible to
     *       all subsequent readers.
     * </ul>
     * No CAS is needed: any two eager strategies racing to install
     * are equivalent (both are pure functions of the same frozen
     * triple set), so last-writer-wins is fine.
     */
    private volatile CowStoreStrategy strategy;

    /** Empty snapshot using {@link IndexingStrategy#EAGER}. */
    public CowSnapshot() {
        this(IndexingStrategy.EAGER);
    }

    /** Empty snapshot using the given indexing strategy. */
    public CowSnapshot(IndexingStrategy indexingStrategy) {
        super(indexingStrategy);
        // Snapshot's triples never grow (snapshots are read-only), so
        // we do not install the writer-side keys-grow hook here.
        this.strategy = buildInitialStrategy(indexingStrategy, /*installGrowHook*/ false);
    }

    /** Internal constructor used by {@link CowWriteTxn#freeze()}. */
    CowSnapshot(IndexingStrategy initialStrategy,
                TxnTripleSet triples,
                CowStoreStrategy strategy) {
        super(initialStrategy, triples);
        this.strategy = strategy;
    }

    @Override
    protected CowStoreStrategy currentStrategy() {
        return strategy;        // volatile read
    }

    @Override
    public void installEagerStrategy(CowEagerStoreStrategy built) {
        // Volatile write: any two equivalent eagers racing to install
        // here are interchangeable (same frozen triple set, same
        // answers), so a plain last-writer-wins write suffices and the
        // volatile field publishes it to all subsequent readers.
        this.strategy = built;
    }

    /**
     * Install (or replace) the strategy slot from outside the
     * constructor. Used by {@link CowWriteTxn#freeze()} to swing a
     * lazy strategy's enclosing-store reference from the writer to the
     * fresh snapshot.
     */
    void installStrategy(CowStoreStrategy s) {
        this.strategy = s;      // volatile write
    }

    // -------------------------------------------------------------------
    // Forking

    /**
     * Cheap fork for a write transaction (sequential). The returned
     * {@link CowWriteTxn} is the only valid mutation target until it is
     * {@linkplain CowWriteTxn#freeze() frozen}; this snapshot is left
     * untouched and remains valid for any reader that captured it.
     */
    public CowWriteTxn forkForWrite() {
        final TxnTripleSet newTriples = this.triples.fork();
        final CowStoreStrategy srcStrategy = this.strategy;
        // The fork is constructed in two steps so the strategy can bind
        // to the new write txn at construction time.
        final CowWriteTxn fork = new CowWriteTxn(initialStrategy, newTriples, null);
        fork.installStrategy(srcStrategy.fork(fork));
        return fork;
    }

    /**
     * Parallel variant of {@link #forkForWrite()}.
     * <p>
     * Drives a two-phase parallelisation:
     * <ol>
     *   <li>Dispatch {@code triples.fork()} to the common fork-join
     *       pool via {@code supplyAsync}.
     *   <li>In parallel, call
     *       {@link CowStoreStrategy#prepareParallelFork()} on the source
     *       strategy. For the eager strategy that returns immediately
     *       after dispatching its own six allocations (three spine
     *       forks plus three reverse-index clones) to the pool. So at
     *       this point seven independent allocations are in flight on
     *       the FJP.
     *   <li>Join the triples future, build the write transaction.
     *   <li>Apply the strategy assembler (which joins its six futures)
     *       to bind the freshly forked strategy to the write txn.
     * </ol>
     * For non-EAGER strategies the assembler has no preparatory work
     * and falls through to a sequential
     * {@link CowStoreStrategy#fork(CowWriteTxn)} at apply time; only
     * the {@code triples.fork()} dispatch hop adds latency, which on
     * small stores can be slower than the sequential path. Pick
     * between sequential and parallel based on workload size.
     */
    public CowWriteTxn forkForWriteParallel() {
        // Dispatch the triples allocation to the fork-join pool.
        final CompletableFuture<TxnTripleSet> fTriples =
                CompletableFuture.supplyAsync(this.triples::fork);
        // In parallel, dispatch the strategy's preparatory work; the
        // returned assembler captures the in-flight futures without
        // joining them, so this call returns immediately and the six
        // strategy-side allocations are now overlapped with the
        // triples allocation above.
        final Function<CowWriteTxn, CowStoreStrategy> assembler =
                this.strategy.prepareParallelFork();
        // Join the triples future and build the write transaction.
        final CowWriteTxn fork = new CowWriteTxn(initialStrategy, fTriples.join(), null);
        // Apply the assembler — this joins the strategy's futures
        // (whatever has not already completed by now) and binds the
        // resulting strategy to the new write txn.
        fork.installStrategy(assembler.apply(fork));
        return fork;
    }
}

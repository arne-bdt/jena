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
import org.apache.jena.mem.store.cow.strategies.CowStoreStrategy;

import java.util.concurrent.CompletableFuture;

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

    /** Empty snapshot using {@link IndexingStrategy#EAGER}. */
    public CowSnapshot() {
        this(IndexingStrategy.EAGER);
    }

    /** Empty snapshot using the given indexing strategy. */
    public CowSnapshot(IndexingStrategy indexingStrategy) {
        super(indexingStrategy);
    }

    /** Internal constructor used by {@link CowWriteTxn#freeze()}. */
    CowSnapshot(IndexingStrategy initialStrategy,
                TxnTripleSet triples,
                CowStoreStrategy strategy) {
        super(initialStrategy, triples, strategy);
    }

    /**
     * Install (or replace) the strategy slot from outside the
     * constructor. Used by {@link CowWriteTxn#freeze()} to swing a
     * lazy strategy's enclosing-store reference from the writer to the
     * fresh snapshot.
     */
    void installStrategy(CowStoreStrategy s) {
        this.strategy.set(s);
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
        final CowStoreStrategy srcStrategy = this.strategy.get();
        // The fork is constructed in two steps so the strategy can bind
        // to the new write txn at construction time.
        final CowWriteTxn fork = new CowWriteTxn(initialStrategy, newTriples, null);
        fork.installStrategy(srcStrategy.fork(fork));
        return fork;
    }

    /**
     * Parallel variant of {@link #forkForWrite()}. The bulk of the work
     * (forking the spines and cloning the reverse-index arrays) lives
     * in the strategy's own fork; only {@code triples.fork()} runs here
     * and is parallelised against the strategy fork via the common
     * fork-join pool. For non-EAGER strategies the strategy fork has
     * essentially no work, so this path is no faster than the
     * sequential one — it remains as a benchmark hook.
     */
    public CowWriteTxn forkForWriteParallel() {
        final CompletableFuture<TxnTripleSet> fTriples =
                CompletableFuture.supplyAsync(this.triples::fork);
        return forkWith(fTriples.join());
    }

    private CowWriteTxn forkWith(TxnTripleSet newTriples) {
        final CowStoreStrategy srcStrategy = this.strategy.get();
        final CowWriteTxn fork = new CowWriteTxn(initialStrategy, newTriples, null);
        fork.installStrategy(srcStrategy.fork(fork));
        return fork;
    }
}

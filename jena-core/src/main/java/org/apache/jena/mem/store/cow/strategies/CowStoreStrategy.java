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

package org.apache.jena.mem.store.cow.strategies;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem.pattern.MatchPattern;
import org.apache.jena.mem.store.cow.CowWriteTxn;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Plug-in interface that controls how the auxiliary
 * subject/predicate/object index of a
 * {@link org.apache.jena.mem.store.cow.CowStore} is maintained and how
 * partial-pattern matches are evaluated.
 *
 * <h2>Relationship to the baseline {@code StoreStrategy}</h2>
 * The baseline {@link org.apache.jena.mem.store.strategies.StoreStrategy}
 * is built around the deep-copy {@code IndexedSetTripleStore}; its
 * implementations work on {@link org.apache.jena.mem.store.indexed.TripleSet}
 * and {@link org.apache.jena.mem.store.indexed.NodesToIndices}, neither of
 * which give the right semantics for snapshot-isolated reads. This
 * interface mirrors the baseline's surface but its implementations work on
 * the COW-aware {@link org.apache.jena.mem.store.cow.TxnTripleSet} and
 * {@link org.apache.jena.mem.store.cow.TxnNodesToIndices}.
 *
 * <h2>Fork semantics</h2>
 * Each strategy must implement {@link #fork(CowWriteTxn)} to produce
 * an equivalent strategy bound to a freshly forked write transaction.
 * Forking is only invoked when transitioning a snapshot to a write
 * transaction (snapshots are read-only and never fork themselves), so
 * the receiver is always a {@link CowWriteTxn}. Fork has two duties:
 * <ul>
 *   <li>For strategies that hold writer-private state (currently only
 *       eager): clone that state. The shared spine arrays inside
 *       {@link org.apache.jena.mem.store.cow.TxnTripleSet} /
 *       {@link org.apache.jena.mem.store.cow.TxnNodesToIndices} are
 *       handled by their own fork constructors.
 *   <li>For strategies that need an enclosing-store reference (currently
 *       only lazy, whose auto-build callback installs a freshly-built
 *       eager strategy onto the enclosing store): re-bind that reference
 *       to the new write transaction.
 * </ul>
 *
 * <h2>Pattern match scope</h2>
 * The match methods only need to handle the partial-pattern cases:
 * {@link MatchPattern#SUB_ANY_ANY}, {@link MatchPattern#ANY_PRE_ANY},
 * {@link MatchPattern#ANY_ANY_OBJ}, {@link MatchPattern#SUB_PRE_ANY},
 * {@link MatchPattern#ANY_PRE_OBJ} and {@link MatchPattern#SUB_ANY_OBJ}.
 * The fully concrete {@link MatchPattern#SUB_PRE_OBJ} and the fully open
 * {@link MatchPattern#ANY_ANY_ANY} are answered by the enclosing store
 * directly from the triple set and never reach the strategy.
 */
public interface CowStoreStrategy {

    /**
     * Notify the strategy that a triple was added to the underlying triple
     * set at the given index. Implementations that maintain an index must
     * update it; implementations without an index are free to no-op.
     */
    void addToIndex(Triple triple, int index);

    /**
     * Notify the strategy that the triple at the given index has been
     * removed from the underlying triple set. Implementations that
     * maintain an index must remove the triple from it; implementations
     * without an index are free to no-op.
     */
    void removeFromIndex(Triple triple, int index);

    /**
     * Discard any auxiliary index data held by the strategy.
     * Implementations without an index may no-op.
     */
    void clearIndex();

    /** True if this strategy has its index built and ready to serve lookups directly. */
    default boolean isIndexInitialized() {
        return false;
    }

    /** True if this strategy throws on pattern lookups (i.e. requires explicit init). */
    default boolean throwsOnUninitializedLookup() {
        return false;
    }

    /** Test whether any triple matches the given pattern. */
    boolean containsMatch(Triple tripleMatch, MatchPattern pattern);

    /** Stream the triples matching the given pattern. */
    Stream<Triple> streamMatch(Triple tripleMatch, MatchPattern pattern);

    /** Iterate the triples matching the given pattern. */
    ExtendedIterator<Triple> findMatch(Triple tripleMatch, MatchPattern pattern);

    /**
     * Build a strategy of the same kind, bound to the given freshly
     * forked write transaction. Called from
     * {@link org.apache.jena.mem.store.cow.CowSnapshot#forkForWrite()}.
     * <p>
     * The returned strategy must reference the new write txn's
     * {@link CowWriteTxn#getTriples() triples} (and any other shared
     * state) — never the source's. State that is logically writer-private
     * (e.g. eager's spines and reverse-index arrays) must be forked so
     * mutations on the new write txn do not corrupt the source's view.
     *
     * @param newWriteTxn the freshly forked write transaction; the
     *                    returned strategy must use its triples and
     *                    (for lazy) install onto its strategy slot
     */
    CowStoreStrategy fork(CowWriteTxn newWriteTxn);

    /**
     * Two-phase parallel fork driven by
     * {@link org.apache.jena.mem.store.cow.CowSnapshot#forkForWriteParallel()}.
     * <p>
     * <b>Phase 1 (this call).</b> Dispatch any parallelisable
     * preparatory work to the common fork-join pool and return
     * <i>immediately</i> with an "assembler" function. The returned
     * function captures the in-flight {@link java.util.concurrent.CompletableFuture}s
     * but does not join them.
     * <p>
     * <b>Phase 2 (calling the returned function).</b> The snapshot
     * applies the assembler with the freshly forked
     * {@link CowWriteTxn}; the assembler joins the in-flight work and
     * returns the strategy bound to that write txn. By this point the
     * snapshot has joined its own {@link org.apache.jena.mem.store.cow.TxnTripleSet#fork()}
     * (needed to construct the write txn), so the strategy's
     * preparatory work has been overlapped with the triples fork.
     * <p>
     * Default has no preparatory work — the assembler just delegates
     * to {@link #fork(CowWriteTxn)} when applied. Strategies with
     * several parallelisable allocations (currently only eager,
     * which has three spine forks and three reverse-index clones)
     * override to dispatch real work in Phase 1.
     *
     * @return an assembler function. Must be invoked exactly once
     * with the freshly forked write transaction; the returned
     * strategy is the one to install on the new write txn.
     */
    default Function<CowWriteTxn, CowStoreStrategy> prepareParallelFork() {
        // No parallelisable preparatory work; assembly is a sequential fork.
        return this::fork;
    }
}

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
import org.apache.jena.mem.store.cow.CowIndexedSetTripleStore;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.stream.Stream;

/**
 * COW counterpart of
 * {@link org.apache.jena.mem.store.strategies.LazyStoreStrategy}: defers
 * index construction until the first pattern lookup. Add/remove are
 * no-ops while the index is absent (the triples themselves are still
 * maintained in the enclosing {@link CowIndexedSetTripleStore}; only the
 * subject/predicate/object index is skipped). On the first
 * {@code containsMatch} / {@code streamMatch} / {@code findMatch} call,
 * a fresh {@link CowEagerStoreStrategy} is built from the enclosing
 * store's triples and atomically installed in the store's strategy slot;
 * the triggering lookup is then forwarded to it.
 *
 * <h2>Concurrent first-lookup race</h2>
 * On a published snapshot held by multiple readers, several threads may
 * concurrently hit a still-pending lazy strategy and each kick off an
 * eager build. The build is a pure function of the triple set (it
 * doesn't mutate {@code triples}) and produces equivalent results across
 * runs, so the race is harmless: one writer wins the
 * {@link CowIndexedSetTripleStore#tryInstallEagerStrategy} compare-and-set
 * and its eager strategy becomes the published view; the losers' builds
 * are GC'd. All threads — winners and losers alike — return their answer
 * via the eager strategy they actually built (the winner's CAS does not
 * affect the answer of a still-running loser, just the cached strategy
 * for future lookups).
 *
 * <h2>Sequential vs parallel build</h2>
 * The {@link #parallel} flag toggles between
 * {@link org.apache.jena.mem.IndexingStrategy#LAZY} (sequential build)
 * and {@link org.apache.jena.mem.IndexingStrategy#LAZY_PARALLEL}
 * (parallel build). The flag is preserved across forks so a fork of a
 * lazy store stays lazy of the same flavour.
 */
public final class CowLazyStoreStrategy implements CowStoreStrategy {

    /** True if the auto-build should populate the eager indices in parallel. */
    private final boolean parallel;

    /**
     * Reference to the enclosing store; the auto-build callback uses it
     * to read the canonical triples and to install the freshly-built
     * eager strategy.
     */
    private final CowIndexedSetTripleStore store;

    public CowLazyStoreStrategy(CowIndexedSetTripleStore store, boolean parallel) {
        this.store = store;
        this.parallel = parallel;
    }

    @Override
    public CowStoreStrategy fork(CowIndexedSetTripleStore newStore) {
        // Lazy itself has no per-store mutable state; just rebind the
        // callback target.
        return new CowLazyStoreStrategy(newStore, parallel);
    }

    @Override public void addToIndex(Triple triple, int index)    { /* no index yet */ }
    @Override public void removeFromIndex(Triple triple, int index) { /* no index yet */ }
    @Override public void clearIndex()                            { /* no index yet */ }

    @Override
    public boolean isIndexInitialized() {
        return false;
    }

    /**
     * Build an eager strategy from the store's current triples and
     * attempt to install it via
     * {@link CowIndexedSetTripleStore#tryInstallEagerStrategy(CowLazyStoreStrategy, CowEagerStoreStrategy)}.
     * If a concurrent caller wins the CAS, this thread's build is
     * discarded (the strategy slot already points at someone else's
     * eager) — but the answer returned to <i>this</i> caller is still
     * computed against the eager strategy this thread just built, which
     * is consistent.
     */
    private CowEagerStoreStrategy upgradeAndAnswer() {
        final CowEagerStoreStrategy mine =
                new CowEagerStoreStrategy(store.getTriples(), parallel);
        store.tryInstallEagerStrategy(this, mine);
        return mine;
    }

    @Override
    public boolean containsMatch(Triple tripleMatch, MatchPattern pattern) {
        return upgradeAndAnswer().containsMatch(tripleMatch, pattern);
    }

    @Override
    public Stream<Triple> streamMatch(Triple tripleMatch, MatchPattern pattern) {
        return upgradeAndAnswer().streamMatch(tripleMatch, pattern);
    }

    @Override
    public ExtendedIterator<Triple> findMatch(Triple tripleMatch, MatchPattern pattern) {
        return upgradeAndAnswer().findMatch(tripleMatch, pattern);
    }

    /** @return whether this lazy strategy uses the parallel build path. */
    public boolean isParallel() {
        return parallel;
    }
}

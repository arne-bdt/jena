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
import org.apache.jena.mem.store.cow.CowSnapshot;
import org.apache.jena.mem.store.cow.CowStore;
import org.apache.jena.mem.store.cow.CowWriteTxn;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.stream.Stream;

/**
 * COW counterpart of
 * {@link org.apache.jena.mem.store.strategies.LazyStoreStrategy}: defers
 * index construction until the first pattern lookup. Add/remove are
 * no-ops while the index is absent (the triples themselves are still
 * maintained in the enclosing {@link CowStore}; only the
 * subject/predicate/object index is skipped). On the first
 * {@code containsMatch} / {@code streamMatch} / {@code findMatch} call,
 * a fresh {@link CowEagerStoreStrategy} is built from the enclosing
 * store's triples and installed in the store's strategy slot via
 * {@link CowStore#installEagerStrategy(CowEagerStoreStrategy)}; the
 * triggering lookup is then forwarded to it.
 *
 * <h2>Concurrent first-lookup race</h2>
 * On a published snapshot held by multiple readers, several threads
 * may concurrently hit a still-pending lazy strategy and each kick
 * off an eager build. The build is a pure function of the (frozen)
 * triple set and produces equivalent results across runs, so the
 * race is harmless: every racer's install is a plain volatile write
 * — last writer wins, the others' eager builds are GC'd. Each
 * caller answers its own triggering lookup against the eager
 * strategy it itself just built, which is consistent.
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
     * Reference to the enclosing store. May be either a
     * {@link org.apache.jena.mem.store.cow.CowSnapshot} (the published
     * read-only view, where readers trigger the auto-build) or a
     * {@link CowWriteTxn} (the writer's working copy, where the writer
     * may trigger the auto-build with its own pattern lookups).
     */
    private final CowStore store;

    public CowLazyStoreStrategy(CowStore store, boolean parallel) {
        this.store = store;
        this.parallel = parallel;
    }

    @Override
    public CowStoreStrategy fork(CowWriteTxn newWriteTxn) {
        // Lazy itself has no per-store mutable state; just rebind the
        // callback target to the new write txn.
        return new CowLazyStoreStrategy(newWriteTxn, parallel);
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
     * install it via {@link CowStore#installEagerStrategy(CowEagerStoreStrategy)}.
     * The build is a pure function of the (frozen) triple set, so
     * any two threads racing here produce equivalent eager strategies;
     * the install is just a publish — last writer wins, no CAS needed.
     * Each caller returns its own freshly built eager from this method,
     * which is the strategy used to answer the triggering lookup.
     * <p>
     * On a published {@link CowSnapshot} the underlying triples cannot
     * grow (only the writer ever appends), so the keys-grow hook is
     * <i>not</i> installed here because it would never fire and
     * concurrent readers would race-write the (plain) hook field on
     * the shared triple set. On a {@link CowWriteTxn}'s working copy
     * this method may also be invoked (the writer hits a partial-
     * pattern lookup mid-transaction); subsequent writer-side appends
     * need the eager strategy's reverse-index arrays to grow with
     * {@code triples.keys[]}, so the hook IS installed — safely,
     * because there is at most one writer at a time.
     */
    private CowEagerStoreStrategy upgradeAndAnswer() {
        final boolean installGrowHook = (store instanceof CowWriteTxn);
        final CowEagerStoreStrategy mine = new CowEagerStoreStrategy(
                store.getTriples(), parallel, installGrowHook);
        store.installEagerStrategy(mine);
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

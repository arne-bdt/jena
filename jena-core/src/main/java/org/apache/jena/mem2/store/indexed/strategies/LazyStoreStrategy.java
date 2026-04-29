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

package org.apache.jena.mem2.store.indexed.strategies;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.pattern.MatchPattern;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * {@link StoreStrategy} that defers index construction until the first
 * pattern lookup. Add/remove are no-ops while the index is absent (the
 * triples are still maintained in the enclosing
 * {@link org.apache.jena.mem2.store.indexed.IndexedSetTripleStore} but no
 * subject/predicate/object index is updated). On the first
 * {@code containsMatch}/{@code streamMatch}/{@code findMatch} call, the
 * supplied callback is invoked to build (and install) an
 * {@link EagerStoreStrategy}; the lookup is then forwarded to it.
 * <p>
 * Used to back both {@link org.apache.jena.mem2.IndexingStrategy#LAZY} and
 * {@link org.apache.jena.mem2.IndexingStrategy#LAZY_PARALLEL}; the
 * sequential / parallel choice is encoded in the supplied callback.
 */
public class LazyStoreStrategy implements StoreStrategy {

    private final Supplier<EagerStoreStrategy> setCurrentStrategyToNewEagerStoreStrategy;

    /**
     * @param setCurrentStrategyToNewEagerStoreStrategy callback that builds
     *        an {@link EagerStoreStrategy}, installs it as the enclosing
     *        store's current strategy, and returns it so this strategy can
     *        delegate the triggering lookup to it
     */
    public LazyStoreStrategy(final Supplier<EagerStoreStrategy> setCurrentStrategyToNewEagerStoreStrategy) {
        this.setCurrentStrategyToNewEagerStoreStrategy = setCurrentStrategyToNewEagerStoreStrategy;
    }

    @Override
    public void addToIndex(final Triple triple, final int index) {
        // No-op, as there is no index to add to.
    }

    @Override
    public void removeFromIndex(final Triple triple, final int index) {
        // No-op, as there is no index to add to.
    }

    @Override
    public void clearIndex() {
        // No-op, as there is no index to add to.
    }

    @Override
    public boolean containsMatch(final Triple tripleMatch, final MatchPattern pattern) {
        return setCurrentStrategyToNewEagerStoreStrategy.get()
                .containsMatch(tripleMatch, pattern);
    }

    @Override
    public Stream<Triple> streamMatch(final Triple tripleMatch, final MatchPattern pattern) {
        return setCurrentStrategyToNewEagerStoreStrategy.get()
                .streamMatch(tripleMatch, pattern);
    }

    @Override
    public ExtendedIterator<Triple> findMatch(final Triple tripleMatch, final MatchPattern pattern) {
        return setCurrentStrategyToNewEagerStoreStrategy.get()
                .findMatch(tripleMatch, pattern);
    }
}

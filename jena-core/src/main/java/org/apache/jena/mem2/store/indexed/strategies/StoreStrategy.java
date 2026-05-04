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

import java.util.stream.Stream;

/**
 * Plug-in interface that controls how the auxiliary subject/predicate/object
 * index of an
 * {@link org.apache.jena.mem2.store.indexed.IndexedSetTripleStore} is
 * maintained and how partial-pattern matches are evaluated.
 * <p>
 * Concrete strategies are
 * {@link EagerStoreStrategy} (always indexed),
 * {@link LazyStoreStrategy} (indexed on first lookup),
 * {@link ManualStoreStrategy} (user-controlled) and
 * {@link MinimalStoreStrategy} (no index, filter every time).
 * <p>
 * The match methods only need to handle the partial-pattern cases:
 * {@link MatchPattern#SUB_ANY_ANY}, {@link MatchPattern#ANY_PRE_ANY},
 * {@link MatchPattern#ANY_ANY_OBJ}, {@link MatchPattern#SUB_PRE_ANY},
 * {@link MatchPattern#ANY_PRE_OBJ} and {@link MatchPattern#SUB_ANY_OBJ}.
 * The fully concrete pattern {@link MatchPattern#SUB_PRE_OBJ} and the
 * fully open pattern {@link MatchPattern#ANY_ANY_ANY} are answered directly
 * from the triple set by the enclosing store and never reach the strategy.
 */
public interface StoreStrategy {
    /**
     * Notify the strategy that a triple was added to the underlying triple
     * set at the given index. Implementations that maintain an index must
     * update it; implementations without an index are free to no-op.
     *
     * @param triple the newly added triple
     * @param index  the stable index it now occupies in the triple set
     */
    void addToIndex(final Triple triple, final int index);

    /**
     * Notify the strategy that the triple at the given index has been
     * removed from the underlying triple set. Implementations that maintain
     * an index must remove the triple from it; implementations without an
     * index are free to no-op.
     *
     * @param triple the removed triple
     * @param index  the index it occupied immediately before removal
     */
    void removeFromIndex(final Triple triple, final int index);

    /**
     * Discard any auxiliary index data held by the strategy. Implementations
     * without an index may no-op.
     */
    void clearIndex();

    /**
     * Check if the index contains a match for the given triple and pattern.
     * This is used to quickly check if a triple matches a given pattern without retrieving the triples.
     *
     * @param tripleMatch the triple to match
     * @param pattern     the pattern to match against
     * @return true if there is a match, false otherwise
     */
    boolean containsMatch(final Triple tripleMatch, final MatchPattern pattern);

    /**
     * Stream the triples that match the given triple and pattern.
     * This is used to retrieve the triples that match a given pattern.
     *
     * @param tripleMatch the triple to match
     * @param pattern     the pattern to match against
     * @return a stream of triples that match the given pattern
     */
    Stream<Triple> streamMatch(final Triple tripleMatch, final MatchPattern pattern);

    /**
     * Find the triples that match the given triple and pattern.
     * This is used to retrieve the triples that match a given pattern as an iterator.
     *
     * @param tripleMatch the triple to match
     * @param pattern     the pattern to match against
     * @return an iterator over the triples that match the given pattern
     */
    ExtendedIterator<Triple> findMatch(final Triple tripleMatch, final MatchPattern pattern);

    /**
     * Check if the index has been initialized and all triples are indexed.
     *
     * @return true if the index is initialized, false otherwise
     */
    default boolean isIndexInitialized() {
        return false;
    }
}

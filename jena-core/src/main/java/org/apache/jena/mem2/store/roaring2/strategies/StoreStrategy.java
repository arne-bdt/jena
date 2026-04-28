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

package org.apache.jena.mem2.store.roaring2.strategies;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.pattern.MatchPattern;
import org.apache.jena.mem2.store.roaring.BlockSet;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.stream.Stream;

/**
 * The store strategy defines how triples are indexed and how matches are found.
 * It is used to implement different indexing strategies like Eager, Lazy, Manual, and Minimal.
 * For the matching operations, only matches for the patterns SUB_ANY_ANY, ANY_PRE_ANY, ANY_ANY_OBJ,
 * SUB_PRE_ANY, ANY_PRE_OBJ, and SUB_ANY_OBJ are supported.
 * The patterns SUB_PRE_OBJ and ANY_ANY_ANY are not supported by the store strategies.
 */
public interface StoreStrategy {
    /**
     * Add a triple to the index if the current strategy supports indexing.
     *
     */
    void addToIndex(final Triple triple, final int index);

    /**
     * Remove a triple from the index if the current strategy supports indexing.
     *
     */
    void removeFromIndex(final Triple triple, final int index);

    /**
     * Clear the index of this store if the current strategy supports indexing.
     * This will remove all triples from the index.
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
}

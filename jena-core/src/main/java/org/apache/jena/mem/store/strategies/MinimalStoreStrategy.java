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

package org.apache.jena.mem.store.strategies;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem.collection.FastHashSet;
import org.apache.jena.mem.pattern.MatchPattern;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.stream.Stream;

/**
 * {@link StoreStrategy} that never builds an index but still answers
 * pattern-match operations - by linearly filtering the triple set. Useful
 * when the dataset is small or when memory is more precious than match-time
 * performance.
 * <p>
 * Used to back {@link org.apache.jena.mem.IndexingStrategy#MINIMAL}. The
 * user can switch to eager indexing at any time by calling
 * {@link org.apache.jena.mem.GraphMemIndexedSet#initializeIndex()}; calling
 * {@code clearIndex} reverts to filtering again.
 */
public class MinimalStoreStrategy implements StoreStrategy {
    private final FastHashSet<Triple> triples;

    /**
     * @param triples the canonical triple set to filter against
     */
    public MinimalStoreStrategy(final FastHashSet<Triple> triples) {
        this.triples = triples;
    }

    @Override
    public void addToIndex(final Triple triple, final int index) {
        // No-op, as we do not store any bitmaps
    }

    @Override
    public void removeFromIndex(final Triple triple, final int index) {
        // No-op, as we do not store any bitmaps
    }

    @Override
    public void clearIndex() {
        // No-op, as we do not store any bitmaps
    }

    @Override
    public boolean containsMatch(final Triple tripleMatch, final MatchPattern pattern) {
        return this.triples.anyMatch(tripleMatch::matches);
    }

    @Override
    public Stream<Triple> streamMatch(final Triple tripleMatch, final MatchPattern pattern) {
        return this.triples.keyStream().filter(tripleMatch::matches);
    }

    @Override
    public ExtendedIterator<Triple> findMatch(final Triple tripleMatch, final MatchPattern pattern) {
        return this.triples.keyIterator().filterKeep(tripleMatch::matches);
    }
}

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

package org.apache.jena.mem2.store.indexed;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.IndexingStrategy;
import org.apache.jena.mem2.pattern.PatternClassifier;
import org.apache.jena.mem2.store.TripleStore;
import org.apache.jena.mem2.store.indexed.strategies.*;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.SingletonIterator;

import java.util.stream.Stream;

/**
 * {@link TripleStore} that stores all triples in a single
 * {@link TripleSet} and delegates pattern-matching to a configurable
 * {@link StoreStrategy}. The strategy is selected via an
 * {@link IndexingStrategy} and may swap itself out at runtime (e.g. a
 * {@link LazyStoreStrategy} replaces itself with an
 * {@link EagerStoreStrategy} as soon as the first pattern lookup is
 * performed).
 * <p>
 * The triples themselves are kept in {@code triples}; each triple has a
 * stable index in that set, which the strategy uses to maintain
 * subject/predicate/object indices of integer indices rather than triple
 * references.
 */
public class IndexedSetTripleStore implements TripleStore {

    private static final String UNKNOWN_PATTERN_CLASSIFIER = "Unknown pattern classifier: %s";
    /** The flat set of stored triples. Each element has a stable integer index. */
    final TripleSet triples; // In this special set, each element has an index
    private StoreStrategy currentStrategy;
    private final IndexingStrategy indexingStrategy;

    /**
     * Creates an indexed store with the {@link IndexingStrategy#EAGER}
     * default indexing strategy.
     */
    public IndexedSetTripleStore() {
        this(IndexingStrategy.EAGER);
    }

    /**
     * Creates an indexed store using the given indexing strategy.
     *
     * @param indexingStrategy the indexing strategy to use
     */
    public IndexedSetTripleStore(final IndexingStrategy indexingStrategy) {
        this.triples = new TripleSet();
        this.indexingStrategy = indexingStrategy;
        this.currentStrategy = createStoreStrategy(indexingStrategy);
    }

    /**
     * Copy constructor used by {@link #copy()}. If the source store has its
     * eager index built, the copy reuses the index data structures (without
     * rebuilding them); otherwise the copy starts from the configured
     * indexing strategy.
     *
     * @param storeToCopy the source store
     */
    private IndexedSetTripleStore(final IndexedSetTripleStore storeToCopy) {
        this.triples = storeToCopy.triples.copy();
        this.indexingStrategy = storeToCopy.indexingStrategy;
        if(storeToCopy.currentStrategy instanceof EagerStoreStrategy eagerStoreStrategy) {
            currentStrategy = new EagerStoreStrategy(triples, eagerStoreStrategy); // Copy the bitmaps from the original strategy
        } else {
            currentStrategy = createStoreStrategy(indexingStrategy);
        }
    }


    private StoreStrategy createStoreStrategy(final IndexingStrategy indexingStrategy) {
        return switch (indexingStrategy) {
            case EAGER
                    -> new EagerStoreStrategy(triples);
            case LAZY
                    -> new LazyStoreStrategy(this::setCurrentStrategyToNewEagerStoreStrategy);
            case LAZY_PARALLEL
                    -> new LazyStoreStrategy(this::setCurrentStrategyToNewEagerStoreStrategyParallel);
            case MANUAL
                    -> new ManualStoreStrategy();
            case MINIMAL
                    -> new MinimalStoreStrategy(triples);
            default
                    -> throw new IllegalArgumentException("Unknown indexing strategy: " + indexingStrategy);
        };
    }

    private EagerStoreStrategy setCurrentStrategyToNewEagerStoreStrategy() {
        final var eagerStoreStrategy= new EagerStoreStrategy(triples, false);
        this.currentStrategy = eagerStoreStrategy;
        return eagerStoreStrategy;
    }

    private EagerStoreStrategy setCurrentStrategyToNewEagerStoreStrategyParallel() {
        final var eagerStoreStrategy= new EagerStoreStrategy(triples, true);
        this.currentStrategy = eagerStoreStrategy;
        return eagerStoreStrategy;
    }

    /**
     * Check if the index of this store is initialized.
     * This will return true if the current strategy is EagerStoreStrategy,
     * which means that the index has been initialized and all triples are indexed.
     *
     * @return true if the index is initialized, false otherwise
     */
    public boolean isIndexInitialized() {
        return currentStrategy instanceof EagerStoreStrategy;
    }

    /**
     * Get the indexing strategy of this store.
     *
     * @return the indexing strategy
     */
    public IndexingStrategy getIndexingStrategy() {
        return indexingStrategy;
    }

    /**
     * Clear the index of this store.
     * This will remove all triples from the index and reset the current strategy to the initial one.
     */
    public void clearIndex() {
        this.currentStrategy = createStoreStrategy(indexingStrategy);
    }

    /**
     * Initialize the index for this store.
     */
    public void initializeIndex() {
        currentStrategy = new EagerStoreStrategy(this.triples, false);
    }

    /**
     * Initialize the index for this store in parallel.
     * This will index all triples in parallel, which can be faster for large datasets.
     */
    public void initializeIndexParallel() {
        currentStrategy = new EagerStoreStrategy(this.triples, true);
    }

    @Override
    public void add(final Triple triple) {
        final var index = triples.addAndGetIndex(triple);
        if (index < 0) { /*triple already exists*/
            return;
        }
        currentStrategy.addToIndex(triple, index);
    }

    @Override
    public void remove(final Triple triple) {
        final var index = triples.removeAndGetIndex(triple);
        if (index < 0) { /*triple does not exist*/
            return;
        }
        currentStrategy.removeFromIndex(triple, index);
    }

    @Override
    public void clear() {
        this.triples.clear();
        this.currentStrategy.clearIndex();
    }

    @Override
    public int countTriples() {
        return this.triples.size();
    }

    @Override
    public boolean isEmpty() {
        return this.triples.isEmpty();
    }

    @Override
    public boolean contains(Triple tripleMatch) {
        final var matchPattern = PatternClassifier.classify(tripleMatch);
        switch (matchPattern) {

            case SUB_ANY_ANY,
                 ANY_PRE_ANY,
                 ANY_ANY_OBJ,
                 SUB_PRE_ANY,
                 ANY_PRE_OBJ,
                 SUB_ANY_OBJ:
                return currentStrategy.containsMatch(tripleMatch, matchPattern);

            case SUB_PRE_OBJ:
                return this.triples.containsKey(tripleMatch);

            case ANY_ANY_ANY:
                return !this.isEmpty();

            default:
                throw new IllegalStateException(String.format(UNKNOWN_PATTERN_CLASSIFIER, matchPattern));
        }
    }

    @Override
    public Stream<Triple> stream() {
        return this.triples.keyStream();
    }

    @Override
    public Stream<Triple> stream(Triple tripleMatch) {
        var pattern = PatternClassifier.classify(tripleMatch);
        switch (pattern) {

            case SUB_PRE_OBJ:
                return this.triples.containsKey(tripleMatch) ? Stream.of(tripleMatch) : Stream.empty();

            case SUB_PRE_ANY,
                 SUB_ANY_OBJ,
                 SUB_ANY_ANY,
                 ANY_PRE_OBJ,
                 ANY_PRE_ANY,
                 ANY_ANY_OBJ:
                return this.currentStrategy.streamMatch(tripleMatch, pattern);

            case ANY_ANY_ANY:
                return this.stream();

            default:
                throw new IllegalStateException("Unknown pattern classifier: " + PatternClassifier.classify(tripleMatch));
        }
    }

    @Override
    public ExtendedIterator<Triple> find(Triple tripleMatch) {
        var pattern = PatternClassifier.classify(tripleMatch);
        switch (pattern) {

            case SUB_PRE_OBJ:
                return this.triples.containsKey(tripleMatch) ? new SingletonIterator<>(tripleMatch) : NiceIterator.emptyIterator();

            case SUB_PRE_ANY,
                 SUB_ANY_OBJ,
                 SUB_ANY_ANY,
                 ANY_PRE_OBJ,
                 ANY_PRE_ANY,
                 ANY_ANY_OBJ:
                return currentStrategy.findMatch(tripleMatch, pattern);

            case ANY_ANY_ANY:
                return this.triples.keyIterator();

            default:
                throw new IllegalStateException("Unknown pattern classifier: " + PatternClassifier.classify(tripleMatch));
        }
    }

    @Override
    public IndexedSetTripleStore copy() {
        return new IndexedSetTripleStore(this);
    }
}

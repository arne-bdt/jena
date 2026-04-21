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

package org.apache.jena.mem2.store.roaring.strategies;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.pattern.MatchPattern;
import org.apache.jena.mem2.pattern.PatternClassifier;
import org.apache.jena.mem2.store.roaring.*;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NullIterator;

import java.util.ConcurrentModificationException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Eager store strategy that indexes all triples immediately.
 * This strategy is used when the indexing strategy is set to EAGER.
 * It builds the index by adding all triples to the index at once.
 */
public class EagerStoreStrategy implements StoreStrategy {
    private static final String UNSUPPORTED_PATTERN_CLASSIFIER = "Unsupported pattern classifier: %s";

    final NodesToIndices[] spoIndices;
    final TripleSet triples;

    /**
     * Create a new EagerStoreStrategy and initialize the index.
     */
    public EagerStoreStrategy(final TripleSet triples, boolean parallel) {
        this(triples);
        if (parallel) {
            indexAllParallel();
        } else {
            indexAll();
        }
    }

    /**
     * Default constructor for EagerStoreStrategy.
     * Initializes the bitmaps for subjects, predicates, and objects.
     * Note: This constructor does not index any triples.
     */
    public EagerStoreStrategy(final TripleSet triples) {
        this.triples = triples;
        this.spoIndices = new NodesToIndices[]{
                new NodesToIndices(), // Subject bitmaps
                new NodesToIndices(), // Predicate bitmaps
                new NodesToIndices()  // Object bitmaps
        };
    }

    /**
     * Copy constructor for EagerStoreStrategy.
     * Creates a new EagerStoreStrategy that is a copy of the given strategy but with a new reference
     * to a set of triples. This set should be a copy of the original set to ensure independence of the new store.
     *
     * @param triples                   the set of triples of the new store
     * @param strategyToCopyBitmapsFrom the strategy to copy bitmaps from
     */
    public EagerStoreStrategy(final TripleSet triples, EagerStoreStrategy strategyToCopyBitmapsFrom) {
        this.triples = triples;
        this.spoIndices = new NodesToIndices[]{
                strategyToCopyBitmapsFrom.spoIndices[0].copy(), // Subject bitmaps
                strategyToCopyBitmapsFrom.spoIndices[1].copy(), // Predicate bitmaps
                strategyToCopyBitmapsFrom.spoIndices[2].copy()  // Object bitmaps
        };
    }

    /**
     * Index all triples in the store.
     * This method will add all triples to the index, creating bitmaps for subjects, predicates, and objects.
     */
    private void indexAll() {
        // Initialize the index by adding all triples to the index
        triples.indexedKeyIterator().forEachRemaining(entry ->
                addToIndex(entry.key(), entry.index()));
    }

    /**
     * Index all triples in the store in parallel.
     * This method will add all triples to the index in parallel,
     * creating bitmaps for subjects, predicates, and objects.
     */
    private void indexAllParallel() {
        final var futureIndexSubjects = CompletableFuture.runAsync(() ->
                triples.indexedKeyIterator().forEachRemaining(entry ->
                        addIndex(0, entry.key().getSubject(), entry.index())));

        final var futureIndexPredicates = CompletableFuture.runAsync(() ->
                triples.indexedKeyIterator().forEachRemaining(entry ->
                        addIndex(1, entry.key().getPredicate(), entry.index())));

        triples.indexedKeyIterator().forEachRemaining(entry ->
                addIndex(2, entry.key().getObject(), entry.index()));

        CompletableFuture.allOf(futureIndexSubjects, futureIndexPredicates).join();
    }

    private void addIndex(final int spoIndex, final Node node, final int tripleIndex) {
        addIndex(spoIndex, node, node.hashCode(), tripleIndex);
    }

        /**
         * Add an index for a given node and index in the specified map.
         * If the node does not exist in the map, it will be created.
         *
         * @param node  the node to add
         * @param tripleIndex, final int the index to add for the node
         */
    private void addIndex(final int spoIndex, final Node node, final int nodeHashCode, final int tripleIndex) {
        final var indices = spoIndices[spoIndex].computeIfAbsent(node, nodeHashCode, IndexList::new);
        var positon = indices.add(tripleIndex);
        this.triples.setListPosition(tripleIndex, spoIndex, positon);
    }

    /**
     * Remove an index for a given node and index in the specified map.
     * If the bitmap for the node becomes empty, the node will be removed from the map.
     *
     * @param node  the node to remove
     * @param index the index to remove for the node
     */
    private void removeIndex(final int spoIndex, final Node node, final int nodeHashCode, final int index) {
        final var indexList = spoIndices[spoIndex].get(node, nodeHashCode);
        var oldPosition = this.triples.getListPosition(index, spoIndex);
        final var switched = indexList.removeAt(oldPosition);
        if (indexList.isEmpty()) {
            spoIndices[spoIndex].removeUnchecked(node, nodeHashCode);
        } else if (-1 < switched) {
            this.triples.setListPosition(switched, spoIndex, oldPosition);
        }
    }

    private void addToIndex(final Triple triple, final int index) {
        addIndex(0, triple.getSubject(), index);
        addIndex(1, triple.getPredicate(), index);
        addIndex(2, triple.getObject(), index);
    }

    @Override
    public void addToIndex(final Triple triple, final int index, final int[] nodeHashCodes) {
        addIndex(0, triple.getSubject(), nodeHashCodes[0], index);
        addIndex(1, triple.getPredicate(), nodeHashCodes[1], index);
        addIndex(2, triple.getObject(), nodeHashCodes[2], index);
    }

    @Override
    public void removeFromIndex(final Triple triple, final int index, final int[] nodeHashCodes) {
        removeIndex(0, triple.getSubject(), nodeHashCodes[0], index);
        removeIndex(1, triple.getPredicate(), nodeHashCodes[1], index);
        removeIndex(2, triple.getObject(), nodeHashCodes[2], index);
    }

    @Override
    public void clearIndex() {
        for (var bitmapMap : spoIndices) {
            bitmapMap.clear();
        }
    }

    @Override
    public boolean containsMatch(final Triple tripleMatch, final MatchPattern pattern) {
        switch (pattern) {

            case SUB_ANY_ANY:
                return spoIndices[0].containsKey(tripleMatch.getSubject());
            case ANY_PRE_ANY:
                return spoIndices[1].containsKey(tripleMatch.getPredicate());
            case ANY_ANY_OBJ:
                return spoIndices[2].containsKey(tripleMatch.getObject());

            case SUB_PRE_ANY: {
                final var subjectBitmap = spoIndices[0].get(tripleMatch.getSubject());
                if (null == subjectBitmap)
                    return false;

                final var predicateBitmap = spoIndices[1].get(tripleMatch.getPredicate());
                if (null == predicateBitmap)
                    return false;

                return triples.intersects(0, subjectBitmap, 1, predicateBitmap);
            }

            case ANY_PRE_OBJ: {
                final var predicateBitmap = spoIndices[1].get(tripleMatch.getPredicate());
                if (null == predicateBitmap)
                    return false;

                final var objectBitmap = spoIndices[2].get(tripleMatch.getObject());
                if (null == objectBitmap)
                    return false;

                return triples.intersects(1, predicateBitmap, 2, objectBitmap);
            }

            case SUB_ANY_OBJ: {
                final var subjectBitmap = spoIndices[0].get(tripleMatch.getSubject());
                if (null == subjectBitmap)
                    return false;

                final var objectBitmap = spoIndices[2].get(tripleMatch.getObject());
                if (null == objectBitmap)
                    return false;

                return triples.intersects(0, subjectBitmap, 2, objectBitmap);
            }

            default:
                throw new IllegalStateException(String.format(UNSUPPORTED_PATTERN_CLASSIFIER,
                        PatternClassifier.classify(tripleMatch)));
        }
    }

    @Override
    public Stream<Triple> streamMatch(final Triple tripleMatch, final MatchPattern pattern) {
        final IndexList indexList;
        switch (pattern) {

            case SUB_ANY_ANY:
                indexList = spoIndices[0].get(tripleMatch.getSubject());
                if(indexList == null) {
                    return Stream.empty();
                }
                return StreamSupport.stream(
                        new IndexListSpliterator(triples, indexList, createConcurrentModificationChecker()),
                        false);

            case ANY_PRE_ANY:
                indexList = spoIndices[1].get(tripleMatch.getPredicate());
                if(indexList == null) {
                    return Stream.empty();
                }
                return StreamSupport.stream(
                        new IndexListSpliterator(triples,  indexList, createConcurrentModificationChecker()),
                        false);

            case ANY_ANY_OBJ:
                indexList = spoIndices[2].get(tripleMatch.getObject());
                if(indexList == null) {
                    return Stream.empty();
                }
                return StreamSupport.stream(
                        new IndexListSpliterator(triples,  indexList, createConcurrentModificationChecker()),
                        false);

            case SUB_PRE_ANY: {
                final var subjectBitmap = spoIndices[0].get(tripleMatch.getSubject());
                if (null == subjectBitmap)
                    return Stream.empty();

                final var predicateBitmap = spoIndices[1].get(tripleMatch.getPredicate());
                if (null == predicateBitmap)
                    return Stream.empty();

                return StreamSupport.stream(
                        new IndexListsSpliterator(triples, subjectBitmap, 0, predicateBitmap, 1,
                        createConcurrentModificationChecker()),
                        false);
            }

            case ANY_PRE_OBJ: {
                final var predicateBitmap = spoIndices[1].get(tripleMatch.getPredicate());
                if (null == predicateBitmap)
                    return Stream.empty();

                final var objectBitmap = spoIndices[2].get(tripleMatch.getObject());
                if (null == objectBitmap)
                    return Stream.empty();

                return StreamSupport.stream(
                        new IndexListsSpliterator(triples, predicateBitmap, 1, objectBitmap, 2,
                        createConcurrentModificationChecker()),
                        false);
            }

            case SUB_ANY_OBJ: {
                final var subjectBitmap = spoIndices[0].get(tripleMatch.getSubject());
                if (null == subjectBitmap)
                    return Stream.empty();

                final var objectBitmap = spoIndices[2].get(tripleMatch.getObject());
                if (null == objectBitmap)
                    return Stream.empty();

                return StreamSupport.stream(
                        new IndexListsSpliterator(triples, subjectBitmap, 0, objectBitmap, 2,
                        createConcurrentModificationChecker()),
                        false);
            }

            default:
                throw new IllegalStateException(String.format(UNSUPPORTED_PATTERN_CLASSIFIER,
                        PatternClassifier.classify(tripleMatch)));
        }
    }

    private Runnable createConcurrentModificationChecker() {
        final var initialSize = triples.size();
        return () -> {
            if (triples.size() != initialSize) throw new ConcurrentModificationException();
        };
    }

    @Override
    public ExtendedIterator<Triple> findMatch(final Triple tripleMatch, final MatchPattern pattern) {
        final IndexList indexList;
        switch (pattern) {

            case SUB_ANY_ANY:
                indexList = spoIndices[0].get(tripleMatch.getSubject());
                if (indexList == null) {
                    return NullIterator.instance();
                }
                return new IndexListIterator(triples, indexList, createConcurrentModificationChecker());

            case ANY_PRE_ANY:
                indexList = spoIndices[1].get(tripleMatch.getPredicate());
                if (indexList == null) {
                    return NullIterator.instance();
                }
                return new IndexListIterator(triples, indexList, createConcurrentModificationChecker());

            case ANY_ANY_OBJ:
                indexList = spoIndices[2].get(tripleMatch.getObject());
                if (indexList == null) {
                    return NullIterator.instance();
                }
                return new IndexListIterator(triples, indexList, createConcurrentModificationChecker());

            case SUB_PRE_ANY: {
                final var subjectBitmap = spoIndices[0].get(tripleMatch.getSubject());
                if (null == subjectBitmap)
                    return NullIterator.instance();

                final var predicateBitmap = spoIndices[1].get(tripleMatch.getPredicate());
                if (null == predicateBitmap)
                    return NullIterator.instance();

                return new IndexListsIterator(triples, subjectBitmap, 0, predicateBitmap, 1,
                        createConcurrentModificationChecker());
            }

            case ANY_PRE_OBJ: {
                final var predicateBitmap = spoIndices[1].get(tripleMatch.getPredicate());
                if (null == predicateBitmap)
                    return NullIterator.instance();

                final var objectBitmap = spoIndices[2].get(tripleMatch.getObject());
                if (null == objectBitmap)
                    return NullIterator.instance();

                return new IndexListsIterator(triples, predicateBitmap, 1, objectBitmap, 2,
                        createConcurrentModificationChecker());
            }

            case SUB_ANY_OBJ: {
                final var subjectBitmap = spoIndices[0].get(tripleMatch.getSubject());
                if (null == subjectBitmap)
                    return NullIterator.instance();

                final var objectBitmap = spoIndices[2].get(tripleMatch.getObject());
                if (null == objectBitmap)
                    return NullIterator.instance();

                return new IndexListsIterator(triples, subjectBitmap, 0, objectBitmap, 2,
                        createConcurrentModificationChecker());
            }

            default:
                throw new IllegalStateException(String.format(UNSUPPORTED_PATTERN_CLASSIFIER,
                        PatternClassifier.classify(tripleMatch)));
        }
    }
}

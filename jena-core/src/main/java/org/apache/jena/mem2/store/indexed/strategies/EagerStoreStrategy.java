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
import org.apache.jena.mem2.pattern.PatternClassifier;
import org.apache.jena.mem2.store.indexed.*;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NullIterator;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * {@link StoreStrategy} that maintains a complete subject/predicate/object
 * index over the triple set at all times.
 * <p>
 * Three node-keyed index maps ({@link NodesToIndices}) hold, for every
 * subject/predicate/object node, an {@link IndexList} of the triple indices
 * that mention it. Three parallel reverse-index arrays
 * ({@code sReverseIndices}, {@code pReverseIndices}, {@code oReverseIndices})
 * store, for every triple slot, its position inside the corresponding
 * {@code IndexList}; this is what makes {@code O(1)} removal possible.
 * <p>
 * The reverse-index arrays are kept the same length as the underlying
 * {@code keys} array of the {@link TripleSet}; whenever the triple set grows
 * its keys array, the {@code growReverseIndices} hook is invoked to grow the
 * reverse arrays too.
 */
public class EagerStoreStrategy implements StoreStrategy {
    private static final String UNSUPPORTED_PATTERN_CLASSIFIER = "Unsupported pattern classifier: %s";

    final TripleSet triples;
    final NodesToIndices sNodeToIndices;
    final NodesToIndices pNodeToIndices;
    final NodesToIndices oNodeToIndices;
    private int[] sReverseIndices;
    private int[] pReverseIndices;
    private int[] oReverseIndices;

    /**
     * Build a new eager strategy over the given triple set, indexing every
     * triple already present.
     *
     * @param triples  the canonical triple set
     * @param parallel if {@code true}, build the three indices concurrently;
     *                 otherwise build them sequentially
     */
    public EagerStoreStrategy(final TripleSet triples, boolean parallel) {
        this.triples = triples;
        this.triples.setOnKeysGrowHook(this::growReverseIndices);
        this.sNodeToIndices = new NodesToIndices();
        this.pNodeToIndices = new NodesToIndices();
        this.oNodeToIndices = new NodesToIndices();
        final var indexSize = triples.getInternalKeysLength();
        this.sReverseIndices = new int[indexSize];
        this.pReverseIndices = new int[indexSize];
        this.oReverseIndices = new int[indexSize];
        if (parallel) {
            indexAllParallel();
        } else {
            indexAll();
        }
    }

    /**
     * Build a new eager strategy and index the triple set sequentially.
     * Equivalent to {@code EagerStoreStrategy(triples, false)}.
     *
     * @param triples the canonical triple set
     */
    public EagerStoreStrategy(final TripleSet triples) {
        this(triples, false);
    }

    /**
     * Copy constructor that reuses an already-built index. Used when copying
     * an {@link IndexedSetTripleStore} whose source already has its eager
     * index built, so that the copy can avoid the cost of rebuilding it.
     * <p>
     * The {@code triples} parameter must be a copy of the original triple
     * set (the indices reference triple slots by index, so the two sets
     * must have identical layouts).
     *
     * @param triples                   the (already-copied) triple set the
     *                                  new strategy will operate on
     * @param strategyToCopyIndicesFrom the strategy whose indices should
     *                                  be cloned
     */
    public EagerStoreStrategy(final TripleSet triples, EagerStoreStrategy strategyToCopyIndicesFrom) {
        this.triples = triples;
        this.triples.setOnKeysGrowHook(this::growReverseIndices);
        this.sNodeToIndices = strategyToCopyIndicesFrom.sNodeToIndices.copy();
        this.pNodeToIndices = strategyToCopyIndicesFrom.pNodeToIndices.copy();
        this.oNodeToIndices = strategyToCopyIndicesFrom.oNodeToIndices.copy();
        this.sReverseIndices = strategyToCopyIndicesFrom.sReverseIndices.clone();
        this.pReverseIndices = strategyToCopyIndicesFrom.pReverseIndices.clone();
        this.oReverseIndices = strategyToCopyIndicesFrom.oReverseIndices.clone();
    }

    /**
     * Sequentially populate the three subject/predicate/object indices with
     * every triple currently in {@code triples}.
     */
    private void indexAll() {
        // Initialize the index by adding all triples to the index
        triples.forEachKey(this::addToIndex);
    }

    /**
     * Populate the three subject/predicate/object indices in parallel.
     * Each of the three indices is touched by exactly one thread, so the
     * indices themselves don't need to be thread-safe; only the read-only
     * iteration over the triple set runs concurrently.
     */
    private void indexAllParallel() {
        final var indexSize = triples.getInternalKeysLength();
        if(indexSize != sReverseIndices.length) {
            sReverseIndices = Arrays.copyOf(sReverseIndices, indexSize);
            pReverseIndices = Arrays.copyOf(pReverseIndices, indexSize);
            oReverseIndices = Arrays.copyOf(oReverseIndices, indexSize);
        }
        final var futureIndexSubjects = CompletableFuture.runAsync(
                () -> triples.forEachKey(this::addSIndex));

        final var futureIndexPredicates = CompletableFuture.runAsync(
                () -> triples.forEachKey(this::addPIndex));

        triples.forEachKey(this::addOIndex);

        CompletableFuture.allOf(futureIndexSubjects, futureIndexPredicates).join();
    }

    private void addSIndex(final Triple triple, final int index) {
        final var indices = sNodeToIndices.computeIfAbsent(triple.getSubject(), IndexList::new);
        sReverseIndices[index] = indices.add(index);
    }

    private void addPIndex(final Triple triple, final int index) {
        final var indices = pNodeToIndices.computeIfAbsent(triple.getPredicate(), IndexList::new);
        pReverseIndices[index] = indices.add(index);
    }

    private void addOIndex(final Triple triple, final int index) {
        final var indices = oNodeToIndices.computeIfAbsent(triple.getObject(), IndexList::new);
        oReverseIndices[index] = indices.add(index);
    }

    private void removeIndexS(final Triple triple, final int index) {
        final var indices = sNodeToIndices.get(triple.getSubject());
        var oldPosition = sReverseIndices[index];
        final var switched = indices.removeAt(oldPosition);
        if (indices.isEmpty()) {
            sNodeToIndices.removeUnchecked(triple.getSubject());
        } else if (-1 < switched) {
            sReverseIndices[switched] = oldPosition;
        }
    }

    private void removeIndexP(final Triple triple, final int index) {
        final var indices = pNodeToIndices.get(triple.getPredicate());
        var oldPosition = pReverseIndices[index];
        final var switched = indices.removeAt(oldPosition);
        if (indices.isEmpty()) {
            pNodeToIndices.removeUnchecked(triple.getPredicate());
        } else if (-1 < switched) {
            pReverseIndices[switched] = oldPosition;
        }
    }

    private void removeIndexO(final Triple triple, final int index) {
        final var indices = oNodeToIndices.get(triple.getObject());
        var oldPosition = oReverseIndices[index];
        final var switched = indices.removeAt(oldPosition);
        if (indices.isEmpty()) {
            oNodeToIndices.removeUnchecked(triple.getObject());
        } else if (-1 < switched) {
            oReverseIndices[switched] = oldPosition;
        }
    }

    private void growReverseIndices(int keysLength) {
        sReverseIndices = Arrays.copyOf(sReverseIndices, keysLength);
        pReverseIndices = Arrays.copyOf(pReverseIndices, keysLength);
        oReverseIndices = Arrays.copyOf(oReverseIndices, keysLength);
    }

    @Override
    public void addToIndex(final Triple triple, final int index) {
        addSIndex(triple, index);
        addPIndex(triple, index);
        addOIndex(triple, index);
    }

    @Override
    public void removeFromIndex(final Triple triple, final int index) {
        removeIndexS(triple, index);
        removeIndexP(triple, index);
        removeIndexO(triple, index);
    }

    @Override
    public void clearIndex() {
        sNodeToIndices.clear();
        pNodeToIndices.clear();
        oNodeToIndices.clear();
        final var indexSize = triples.getInternalKeysLength();
        this.sReverseIndices = new int[indexSize];
        this.pReverseIndices = new int[indexSize];
        this.oReverseIndices = new int[indexSize];
    }

    @Override
    public boolean containsMatch(final Triple tripleMatch, final MatchPattern pattern) {
        switch (pattern) {

            case SUB_ANY_ANY:
                return sNodeToIndices.containsKey(tripleMatch.getSubject());
            case ANY_PRE_ANY:
                return pNodeToIndices.containsKey(tripleMatch.getPredicate());
            case ANY_ANY_OBJ:
                return oNodeToIndices.containsKey(tripleMatch.getObject());

            case SUB_PRE_ANY: {
                final var sIndices = sNodeToIndices.get(tripleMatch.getSubject());
                if (null == sIndices)
                    return false;

                final var pIndices = pNodeToIndices.get(tripleMatch.getPredicate());
                if (null == pIndices)
                    return false;

                return IndexList.intersects(sIndices, sReverseIndices, pIndices, pReverseIndices);
            }

            case ANY_PRE_OBJ: {
                final var pIndices = pNodeToIndices.get(tripleMatch.getPredicate());
                if (null == pIndices)
                    return false;

                final var oIndices = oNodeToIndices.get(tripleMatch.getObject());
                if (null == oIndices)
                    return false;

                return IndexList.intersects(pIndices, pReverseIndices, oIndices, oReverseIndices);
            }

            case SUB_ANY_OBJ: {
                final var sIndices = sNodeToIndices.get(tripleMatch.getSubject());
                if (null == sIndices)
                    return false;

                final var oIndices = oNodeToIndices.get(tripleMatch.getObject());
                if (null == oIndices)
                    return false;

                return IndexList.intersects(sIndices, sReverseIndices, oIndices, oReverseIndices);
            }

            default:
                throw new IllegalStateException(String.format(UNSUPPORTED_PATTERN_CLASSIFIER,
                        PatternClassifier.classify(tripleMatch)));
        }
    }

    @Override
    public Stream<Triple> streamMatch(final Triple tripleMatch, final MatchPattern pattern) {
        switch (pattern) {

            case SUB_ANY_ANY: {
                final IndexList indexList = sNodeToIndices.get(tripleMatch.getSubject());
                if (indexList == null) {
                    return Stream.empty();
                }
                return StreamSupport.stream(
                        new IndexListSpliterator(triples, indexList),
                        false);
            }
            case ANY_PRE_ANY: {
                final IndexList indexList = pNodeToIndices.get(tripleMatch.getPredicate());
                if (indexList == null) {
                    return Stream.empty();
                }
                return StreamSupport.stream(
                        new IndexListSpliterator(triples, indexList),
                        false);
            }
            case ANY_ANY_OBJ: {
                final IndexList indexList = oNodeToIndices.get(tripleMatch.getObject());
                if(indexList == null) {
                    return Stream.empty();
                }
                return StreamSupport.stream(
                        new IndexListSpliterator(triples,  indexList),
                        false);
            }
            case SUB_PRE_ANY: {
                final var sIndices = sNodeToIndices.get(tripleMatch.getSubject());
                if (null == sIndices)
                    return Stream.empty();

                final var pIndices = pNodeToIndices.get(tripleMatch.getPredicate());
                if (null == pIndices)
                    return Stream.empty();

                return StreamSupport.stream(
                        new IndexListsSpliterator(triples,
                                sIndices, sReverseIndices,
                                pIndices, pReverseIndices),
                        false);
            }

            case ANY_PRE_OBJ: {
                final var pIndices = pNodeToIndices.get(tripleMatch.getPredicate());
                if (null == pIndices)
                    return Stream.empty();

                final var oIndices = oNodeToIndices.get(tripleMatch.getObject());
                if (null == oIndices)
                    return Stream.empty();

                return StreamSupport.stream(
                        new IndexListsSpliterator(triples,
                                pIndices, pReverseIndices,
                                oIndices, oReverseIndices),
                        false);
            }

            case SUB_ANY_OBJ: {
                final var sIndices = sNodeToIndices.get(tripleMatch.getSubject());
                if (null == sIndices)
                    return Stream.empty();

                final var oIndices = oNodeToIndices.get(tripleMatch.getObject());
                if (null == oIndices)
                    return Stream.empty();

                return StreamSupport.stream(
                        new IndexListsSpliterator(triples,
                                sIndices, sReverseIndices,
                                oIndices, oReverseIndices),
                        false);
            }

            default:
                throw new IllegalStateException(String.format(UNSUPPORTED_PATTERN_CLASSIFIER,
                        PatternClassifier.classify(tripleMatch)));
        }
    }

    @Override
    public ExtendedIterator<Triple> findMatch(final Triple tripleMatch, final MatchPattern pattern) {
        switch (pattern) {

            case SUB_ANY_ANY: {
                final IndexList indexList = sNodeToIndices.get(tripleMatch.getSubject());
                if (indexList == null) {
                    return NullIterator.instance();
                }
                return new IndexListIterator(triples, indexList);
            }
            case ANY_PRE_ANY: {
                final IndexList indexList = pNodeToIndices.get(tripleMatch.getPredicate());
                if (indexList == null) {
                    return NullIterator.instance();
                }
                return new IndexListIterator(triples, indexList);
            }
            case ANY_ANY_OBJ: {
                final IndexList indexList = oNodeToIndices.get(tripleMatch.getObject());
                if (indexList == null) {
                    return NullIterator.instance();
                }
                return new IndexListIterator(triples, indexList);
            }
            case SUB_PRE_ANY: {
                final var sIndices = sNodeToIndices.get(tripleMatch.getSubject());
                if (null == sIndices)
                    return NullIterator.instance();

                final var pIndices = pNodeToIndices.get(tripleMatch.getPredicate());
                if (null == pIndices)
                    return NullIterator.instance();

                return new IndexListsIterator(triples,
                        sIndices, sReverseIndices,
                        pIndices, pReverseIndices);
            }

            case ANY_PRE_OBJ: {
                final var pIndices = pNodeToIndices.get(tripleMatch.getPredicate());
                if (null == pIndices)
                    return NullIterator.instance();

                final var oIndices = oNodeToIndices.get(tripleMatch.getObject());
                if (null == oIndices)
                    return NullIterator.instance();

                return new IndexListsIterator(triples,
                        pIndices, pReverseIndices,
                        oIndices, oReverseIndices);
            }

            case SUB_ANY_OBJ: {
                final var sIndices = sNodeToIndices.get(tripleMatch.getSubject());
                if (null == sIndices)
                    return NullIterator.instance();

                final var oIndices = oNodeToIndices.get(tripleMatch.getObject());
                if (null == oIndices)
                    return NullIterator.instance();

                return new IndexListsIterator(triples,
                        sIndices, sReverseIndices,
                        oIndices, oReverseIndices);
            }

            default:
                throw new IllegalStateException(String.format(UNSUPPORTED_PATTERN_CLASSIFIER,
                        PatternClassifier.classify(tripleMatch)));
        }
    }
}

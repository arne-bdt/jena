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

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.pattern.MatchPattern;
import org.apache.jena.mem2.pattern.PatternClassifier;
import org.apache.jena.mem2.store.roaring.*;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NullIterator;

import java.util.ConcurrentModificationException;
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

    final NodesToIndices sNodeToIndices;
    final NodesToIndices pNodeToIndices;
    final NodesToIndices oNodeToIndices;
    final BlockSet triples;

    /**
     * Create a new EagerStoreStrategy and initialize the index.
     */
    public EagerStoreStrategy(final BlockSet triples, boolean parallel) {
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
    public EagerStoreStrategy(final BlockSet triples) {
        this.triples = triples;
        this.sNodeToIndices = new NodesToIndices();
        this.pNodeToIndices = new NodesToIndices();
        this.oNodeToIndices = new NodesToIndices();
    }

    /**
     * Copy constructor for EagerStoreStrategy.
     * Creates a new EagerStoreStrategy that is a copy of the given strategy but with a new reference
     * to a set of triples. This set should be a copy of the original set to ensure independence of the new store.
     *
     * @param triples                   the set of triples of the new store
     * @param strategyToCopyIndicesFrom the strategy to copy indices from
     */
    public EagerStoreStrategy(final BlockSet triples, EagerStoreStrategy strategyToCopyIndicesFrom) {
        this.triples = triples;
        this.sNodeToIndices = strategyToCopyIndicesFrom.sNodeToIndices.copy();
        this.pNodeToIndices = strategyToCopyIndicesFrom.pNodeToIndices.copy();
        this.oNodeToIndices = strategyToCopyIndicesFrom.oNodeToIndices.copy();
    }

    /**
     * Index all triples in the store.
     * This method will add all triples to the index, creating bitmaps for subjects, predicates, and objects.
     */
    private void indexAll() {
        // Initialize the index by adding all triples to the index
        triples.forEachRow((row) -> {
            addSIndex(row);
            addPIndex(row);
            addOIndex(row);
        });
    }

    /**
     * Index all triples in the store in parallel.
     * This method will add all triples to the index in parallel,
     * creating bitmaps for subjects, predicates, and objects.
     */
    private void indexAllParallel() {
        final var futureIndexSubjects = CompletableFuture.runAsync(
                () -> triples.forEachRow(this::addSIndex));

        final var futureIndexPredicates = CompletableFuture.runAsync(
                () -> triples.forEachRow(this::addPIndex));

        triples.forEachRow(this::addOIndex);

        CompletableFuture.allOf(futureIndexSubjects, futureIndexPredicates).join();
    }

    private void addSIndex(final BlockSet.BlockRow row) {
        final var indices = sNodeToIndices.computeIfAbsent(row.getTriple().getSubject(), IndexList::new);
        var positon = indices.add(row.index());
        row.setSIndex(positon);
    }

    private void addPIndex(final BlockSet.BlockRow row) {
        final var indices = pNodeToIndices.computeIfAbsent(row.getTriple().getPredicate(), IndexList::new);
        var positon = indices.add(row.index());
        row.setPIndex(positon);
    }

    private void addOIndex(final BlockSet.BlockRow row) {
        final var indices = oNodeToIndices.computeIfAbsent(row.getTriple().getObject(), IndexList::new);
        var positon = indices.add(row.index());
        row.setOIndex(positon);
    }

    private void removeIndexS(final BlockSet.BlockRow row) {
        final var indices = sNodeToIndices.get(row.getTriple().getSubject());
        var oldPosition = row.getSIndex();
        final var switched = indices.removeAt(oldPosition);
        if (indices.isEmpty()) {
            sNodeToIndices.removeUnchecked(row.getTriple().getSubject());
        } else if (-1 < switched) {
            this.triples.setSIndex(switched, oldPosition);
        }
    }

    private void removeIndexP(final BlockSet.BlockRow row) {
        final var indices = pNodeToIndices.get(row.getTriple().getPredicate());
        var oldPosition = row.getPIndex();
        final var switched = indices.removeAt(oldPosition);
        if (indices.isEmpty()) {
            pNodeToIndices.removeUnchecked(row.getTriple().getPredicate());
        } else if (-1 < switched) {
            this.triples.setPIndex(switched, oldPosition);
        }
    }

    private void removeIndexO(final BlockSet.BlockRow row) {
        final var indices = oNodeToIndices.get(row.getTriple().getObject());
        var oldPosition = row.getOIndex();
        final var switched = indices.removeAt(oldPosition);
        if (indices.isEmpty()) {
            oNodeToIndices.removeUnchecked(row.getTriple().getObject());
        } else if (-1 < switched) {
            this.triples.setOIndex(switched, oldPosition);
        }
    }

    @Override
    public void addToIndex(final BlockSet.BlockRow row) {
        addSIndex(row);
        addPIndex(row);
        addOIndex(row);
    }

    @Override
    public void removeFromIndex(final BlockSet.BlockRow row) {
        removeIndexS(row);
        removeIndexP(row);
        removeIndexO(row);
    }

    @Override
    public void clearIndex() {
        sNodeToIndices.clear();
        pNodeToIndices.clear();
        oNodeToIndices.clear();
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

                return IndexList.intersects(sIndices, triples::getSIndex, pIndices, triples::getPIndex);
            }

            case ANY_PRE_OBJ: {
                final var pIndices = pNodeToIndices.get(tripleMatch.getPredicate());
                if (null == pIndices)
                    return false;

                final var oIndices = oNodeToIndices.get(tripleMatch.getObject());
                if (null == oIndices)
                    return false;

                return IndexList.intersects(pIndices, triples::getPIndex, oIndices, triples::getOIndex);
            }

            case SUB_ANY_OBJ: {
                final var sIndices = sNodeToIndices.get(tripleMatch.getSubject());
                if (null == sIndices)
                    return false;

                final var oIndices = oNodeToIndices.get(tripleMatch.getObject());
                if (null == oIndices)
                    return false;

                return IndexList.intersects( sIndices, triples::getSIndex, oIndices, triples::getOIndex);
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
                        new IndexListSpliterator(triples, indexList, createConcurrentModificationChecker()),
                        false);
            }
            case ANY_PRE_ANY: {
                final IndexList indexList = pNodeToIndices.get(tripleMatch.getPredicate());
                if (indexList == null) {
                    return Stream.empty();
                }
                return StreamSupport.stream(
                        new IndexListSpliterator(triples, indexList, createConcurrentModificationChecker()),
                        false);
            }
            case ANY_ANY_OBJ: {
                final IndexList indexList = oNodeToIndices.get(tripleMatch.getObject());
                if(indexList == null) {
                    return Stream.empty();
                }
                return StreamSupport.stream(
                        new IndexListSpliterator(triples,  indexList, createConcurrentModificationChecker()),
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
                                sIndices, triples::getSIndex,
                                pIndices, triples::getPIndex,
                        createConcurrentModificationChecker()),
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
                                pIndices, triples::getPIndex,
                                oIndices, triples::getOIndex,
                        createConcurrentModificationChecker()),
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
                                sIndices, triples::getSIndex,
                                oIndices, triples::getOIndex,
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
        switch (pattern) {

            case SUB_ANY_ANY: {
                final IndexList indexList = sNodeToIndices.get(tripleMatch.getSubject());
                if (indexList == null) {
                    return NullIterator.instance();
                }
                return new IndexListIterator(triples, indexList, createConcurrentModificationChecker());
            }
            case ANY_PRE_ANY: {
                final IndexList indexList = pNodeToIndices.get(tripleMatch.getPredicate());
                if (indexList == null) {
                    return NullIterator.instance();
                }
                return new IndexListIterator(triples, indexList, createConcurrentModificationChecker());
            }
            case ANY_ANY_OBJ: {
                final IndexList indexList = oNodeToIndices.get(tripleMatch.getObject());
                if (indexList == null) {
                    return NullIterator.instance();
                }
                return new IndexListIterator(triples, indexList, createConcurrentModificationChecker());
            }
            case SUB_PRE_ANY: {
                final var sIndices = sNodeToIndices.get(tripleMatch.getSubject());
                if (null == sIndices)
                    return NullIterator.instance();

                final var pIndices = pNodeToIndices.get(tripleMatch.getPredicate());
                if (null == pIndices)
                    return NullIterator.instance();

                return new IndexListsIterator(triples,
                        sIndices, triples::getSIndex,
                        pIndices, triples::getPIndex,
                        createConcurrentModificationChecker());
            }

            case ANY_PRE_OBJ: {
                final var pIndices = pNodeToIndices.get(tripleMatch.getPredicate());
                if (null == pIndices)
                    return NullIterator.instance();

                final var oIndices = oNodeToIndices.get(tripleMatch.getObject());
                if (null == oIndices)
                    return NullIterator.instance();

                return new IndexListsIterator(triples,
                        pIndices, triples::getPIndex,
                        oIndices, triples::getOIndex,
                        createConcurrentModificationChecker());
            }

            case SUB_ANY_OBJ: {
                final var sIndices = sNodeToIndices.get(tripleMatch.getSubject());
                if (null == sIndices)
                    return NullIterator.instance();

                final var oIndices = oNodeToIndices.get(tripleMatch.getObject());
                if (null == oIndices)
                    return NullIterator.instance();

                return new IndexListsIterator(triples,
                        sIndices, triples::getSIndex,
                        oIndices, triples::getOIndex,
                        createConcurrentModificationChecker());
            }

            default:
                throw new IllegalStateException(String.format(UNSUPPORTED_PATTERN_CLASSIFIER,
                        PatternClassifier.classify(tripleMatch)));
        }
    }
}

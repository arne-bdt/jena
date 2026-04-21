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
package org.apache.jena.mem2.store.fast;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.collection.FastHashMap;
import org.apache.jena.mem2.collection.JenaMapSetCommon;
import org.apache.jena.mem2.iterator.IteratorOfJenaSets;
import org.apache.jena.mem2.pattern.PatternClassifier;
import org.apache.jena.mem2.store.TripleStore;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.SingletonIterator;

import java.util.ConcurrentModificationException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A triple store that uses hash tables to map from nodes to triple bunches.
 * <p>
 * Inner structure:
 * - three {@link FastHashMap}, one for each node (subject, predicate, object) in the triple
 * - each map maps from a node to a {@link FastTripleBunch}
 * - for up to 16 triples with the same subject, the bunch is an {@link FastArrayBunch}, otherwise it is
 * a {@link FastHashedTripleBunch}
 * - for up to 32 triples with the same predicate and object, the bunch is an {@link FastArrayBunch}, otherwise it is
 * a {@link FastHashedTripleBunch}
 * <p>
 * Other optimizations:
 * - each triple is added to three {@link FastTripleBunch}es. To avoid the overhead of calculating the hash code three
 * times, the hash code is calculated once and passed to the {@link FastTripleBunch}es.
 * - the different sizes for the {@link FastArrayBunch}es are chosen:
 * - to avoid the memory-overhead of {@link FastHashedTripleBunch} for a small number of triples
 * - the subject bunch is smaller than the predicate and object bunches, because the subject is typically used for
 * #contains operations, which are faster for {@link FastHashedTripleBunch}es.
 * - the predicate and object bunches are the same size, because they are typically used for #find operations, which
 * typically do not answer #contains operations. Making them much larger might slow down #remove operations on the
 * graph.
 * - "ANY_PRE_OBJ" matches primarily use the object bunch unless the size of the bunch is larger than 400 triples. In
 * that case, there is a secondary lookup in the predicate bunch. Then the smaller bunch is used for the lookup.
 * Especially for RDF graphs there are some very common object nodes like "true"/"false" or "0"/"1". In that case,
 * a secondary lookup in the predicate bunch might be faster than a primary lookup in the object bunch.
 * - FastTripleStore#contains uses {@link FastTripleBunch#anyMatchRandomOrder} for
 * "ANY_PRE_OBJ" lookups. This is only faster than {@link JenaMapSetCommon#anyMatch}
 * if there are many matches and the set is ordered in an unfavorable way. "ANY_PRE_OBJ" matches usually fall into
 * this category. This optimization was only needed because the {@link FastTripleBunch}es does not use the random
 * order of the triples in #anyMatch but the ordered dense array of triples, which is faster if there are only a few
 * matches.
 * - for the FastArrayBunches, the equals method of the triple is not called. Instead, only the two nodes that are
 *   not part of the key of the containing map are compared.
 */
public class FastTripleStore implements TripleStore {

    protected static final int THRESHOLD_FOR_SECONDARY_LOOKUP = 16;
    protected static final int MAX_ARRAY_BUNCH_SIZE_SUBJECT = 16;
    final FastHashedBunchMap subjects;
    final IndexListMap predicates;
    final IndexListMap objects;
    private int size = 0;

    public FastTripleStore() {
        subjects = new FastHashedBunchMap();
        predicates = new IndexListMap();
        objects = new IndexListMap();
    }

    private FastTripleStore(final FastTripleStore tripleStoreToCopy) {
        subjects = tripleStoreToCopy.subjects.copy();
        predicates = tripleStoreToCopy.predicates.copy();
        objects = tripleStoreToCopy.objects.copy();
        size = tripleStoreToCopy.size;
    }

    @Override
    public void add(Triple triple) {
        FastTripleBunch sBunch;
        int tripleIndex;
        int subjectIndex = subjects.indexOf(triple.getSubject());
        if (subjectIndex < 0) {
            sBunch = new ArrayBunchWithSameSubject();
            sBunch.addUnchecked(triple);
            subjectIndex = subjects.putAndGetIndex(triple.getSubject(), sBunch);
            tripleIndex = 0;
        } else {
            sBunch = subjects.getValueAt(subjectIndex);
            if (sBunch.isArray() && sBunch.size() == MAX_ARRAY_BUNCH_SIZE_SUBJECT) {
                sBunch = new FastHashedTripleBunch(sBunch);
                subjectIndex = subjects.putAndGetIndex(triple.getSubject(), sBunch);
            }
            tripleIndex = sBunch.addAndGetIndex(triple, triple.hashCode());
        }
        if (-1 < tripleIndex) {
            size++;
            var pList = predicates.get(triple.getPredicate());
            if (pList == null) {
                pList = new DoubleIndexList();
                predicates.put(triple.getPredicate(), pList);
            }
            var ptIndex = pList.add(subjectIndex, tripleIndex);

            var oList = objects.get(triple.getObject());
            if (oList == null) {
                oList = new DoubleIndexList();
                objects.put(triple.getObject(), oList);
            }
            var otIndex = oList.add(subjectIndex, tripleIndex);

            sBunch.setIndices(tripleIndex, ptIndex, otIndex);
        }
    }

    @Override
    public void remove(Triple triple) {
        final var sBunch = subjects.get(triple.getSubject());
        if(sBunch == null)
            return;
        final var tIndex = sBunch.indexOf(triple);
        if(tIndex < 0)
            return;

        {
            final var pBunch = predicates.get(triple.getPredicate());
            final var pIndex = sBunch.getPIndex(tIndex);
            final var posUpdate = pBunch.removeAt(pIndex);
            if (posUpdate.length == 2) {
                subjects.getValueAt(posUpdate[0]).setPIndex(posUpdate[1], pIndex);
            }
            if (pBunch.isEmpty()) {
                predicates.removeUnchecked(triple.getPredicate());
            }
        }
        {
            final var oBunch = objects.get(triple.getObject());
            final var oIndex = sBunch.getOIndex(tIndex);
            final var posUpdate = oBunch.removeAt(oIndex);
            if (posUpdate.length == 2) {
                subjects.getValueAt(posUpdate[0]).setOIndex(posUpdate[1], oIndex);
            }
            if (oBunch.isEmpty()) {
                objects.removeUnchecked(triple.getObject());
            }
        }

        var movedTripleIndex = sBunch.removeAt(tIndex);
        if(-1 < movedTripleIndex) {
            final var movedIndices = sBunch.getIndices(movedTripleIndex);
            final var movedTriple = sBunch.getKeyAt(tIndex);
            predicates.get(movedTriple.getPredicate()).getElementIndices()[movedIndices[0]] = tIndex;
            objects.get(movedTriple.getObject()).getElementIndices()[movedIndices[1]] = tIndex;

            sBunch.setIndices(tIndex, movedIndices);
        }
        if (sBunch.isEmpty()) {
            subjects.removeUnchecked(triple.getSubject());
        }
        size--;
    }

    @Override
    public void clear() {
        subjects.clear();
        predicates.clear();
        objects.clear();
        size = 0;
    }

    @Override
    public int countTriples() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean contains(Triple tripleMatch) {
        switch (PatternClassifier.classify(tripleMatch)) {

            case SUB_PRE_OBJ: {
                final var triples = subjects.get(tripleMatch.getSubject());
                if (triples == null) {
                    return false;
                }
                return triples.containsKey(tripleMatch);
            }

            case SUB_PRE_ANY: {
                var sIndex = subjects.indexOf(tripleMatch.getSubject());
                if(sIndex < 0) {
                    return false;
                }
                final var triplesBySubject = subjects.getValueAt(sIndex);
                if(triplesBySubject.size() < THRESHOLD_FOR_SECONDARY_LOOKUP) {
                    return triplesBySubject.anyMatch(t -> tripleMatch.getPredicate().equals(t.getPredicate()));
                }
                final var triplesByPredicate = predicates.get(tripleMatch.getPredicate());
                if (triplesByPredicate == null) {
                    return false;
                }
                return intersects(triplesBySubject, sIndex, triplesByPredicate, 0);
            }

            case SUB_ANY_OBJ: {
                var sIndex = subjects.indexOf(tripleMatch.getSubject());
                if(sIndex < 0) {
                    return false;
                }
                final var triplesBySubject = subjects.getValueAt(sIndex);
                if(triplesBySubject.size() < THRESHOLD_FOR_SECONDARY_LOOKUP) {
                    return triplesBySubject.anyMatch(t -> tripleMatch.getObject().equals(t.getObject()));
                }
                final var triplesByObject = objects.get(tripleMatch.getObject());
                if (triplesByObject == null) {
                    return false;
                }
                return intersects(triplesBySubject, sIndex, triplesByObject, 1);
            }

            case SUB_ANY_ANY:
                return subjects.containsKey(tripleMatch.getSubject());

            case ANY_PRE_OBJ: {
                final var triplesByPredicate = predicates.get(tripleMatch.getPredicate());
                if (triplesByPredicate == null) {
                    return false;
                }
                final var triplesByObject = objects.get(tripleMatch.getObject());
                if (triplesByObject == null) {
                    return false;
                }
                return intersects(triplesByPredicate, 0, triplesByObject, 1);
            }

            case ANY_PRE_ANY:
                return predicates.containsKey(tripleMatch.getPredicate());

            case ANY_ANY_OBJ:
                return objects.containsKey(tripleMatch.getObject());

            case ANY_ANY_ANY:
                return !isEmpty();

            default:
                throw new IllegalStateException(String.format("Unexpected value: %s", PatternClassifier.classify(tripleMatch)));
        }
    }

    private boolean intersects(FastTripleBunch triples, int sIndex, DoubleIndexList list, int listIndex) {
        final var triplesSize = list.size();
        final var listSize = list.size();
        if(triplesSize < listSize) {
            var i = triplesSize;
            while(-1 < --i) {
                final var listIndexCandidate = triples.getIndex(i, listIndex);
                if(listIndexCandidate < listSize) {
                    if(list.getSubjectIndexAt(listIndexCandidate) == sIndex
                        && list.getElementIndexAt(listIndexCandidate) == i) {
                        return true;
                    }
                }
            }
            return false;
        } else {
            var i = listSize;
            while (-1 < --i) {
                if(list.getSubjectIndexAt(i) == sIndex) {
                    final var tIndexCandidate = list.getElementIndexAt(i);
                    if(tIndexCandidate < triplesSize
                        && triples.getIndex(tIndexCandidate, listIndex) == i) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private boolean intersects(DoubleIndexList a, int listIndexA, DoubleIndexList b, int listIndexB) {
        return a.size() < b.size()
                ? intersectsSmallerWithLarger(a, b, listIndexB)
                : intersectsSmallerWithLarger(b, a, listIndexA);
    }

    private boolean intersectsSmallerWithLarger(DoubleIndexList smallerList,
                                                DoubleIndexList largerList, int  listIndexLarger) {
        final int largerListSize = largerList.size();
        var i = smallerList.size();
        while (-1 < --i) {
            final var sIndex = smallerList.getSubjectIndexAt(i);
            final var tripleIndex = smallerList.getElementIndexAt(i);
            final var largerListIndexCandidate = subjects.getValueAt(sIndex)
                    .getIndex(tripleIndex, listIndexLarger);
            if (largerListIndexCandidate < largerListSize) {
                if(largerList.getSubjectIndexAt(largerListIndexCandidate) == sIndex
                        && largerList.getElementIndexAt(largerListIndexCandidate) == tripleIndex) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Stream<Triple> stream() {
        return StreamSupport.stream(subjects.valueSpliterator(), false)
                .flatMap(bunch -> StreamSupport.stream(bunch.keySpliterator(), false));
    }

    @Override
    public Stream<Triple> stream(Triple tripleMatch) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(find(tripleMatch),
                Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.IMMUTABLE), false);
//        switch (PatternClassifier.classify(tripleMatch)) {
//
//            case SUB_PRE_OBJ: {
//                final var triples = subjects.get(tripleMatch.getSubject());
//                if (triples == null) {
//                    return Stream.empty();
//                }
//                return triples.containsKey(tripleMatch) ? Stream.of(tripleMatch) : Stream.empty();
//            }
//
//            case SUB_PRE_ANY: {
//                final var triplesBySubject = subjects.get(tripleMatch.getSubject());
//                if (triplesBySubject == null) {
//                    return Stream.empty();
//                }
//                return triplesBySubject.keyStream().filter(t -> tripleMatch.getPredicate().equals(t.getPredicate()));
//            }
//
//            case SUB_ANY_OBJ: {
//                final var triplesBySubject = subjects.get(tripleMatch.getSubject());
//                if (triplesBySubject == null) {
//                    return Stream.empty();
//                }
//                return triplesBySubject.keyStream().filter(t -> tripleMatch.getObject().equals(t.getObject()));
//            }
//
//            case SUB_ANY_ANY: {
//                final var triples = subjects.get(tripleMatch.getSubject());
//                return triples == null ? Stream.empty() : triples.keyStream();
//            }
//
//            case ANY_PRE_OBJ: {
//                final var triplesByObject = objects.get(tripleMatch.getObject());
//                if (triplesByObject == null) {
//                    return Stream.empty();
//                }
//                if (triplesByObject.size() > THRESHOLD_FOR_SECONDARY_LOOKUP) {
//                    final var triplesByPredicate = predicates.get(tripleMatch.getPredicate());
//                    if (triplesByPredicate == null) {
//                        return Stream.empty();
//                    }
//                    if (triplesByPredicate.size() < triplesByObject.size()) {
//                        return triplesByPredicate.keyStream().filter(t -> tripleMatch.getObject().equals(t.getObject()));
//                    }
//                }
//                return triplesByObject.keyStream().filter(t -> tripleMatch.getPredicate().equals(t.getPredicate()));
//            }
//
//            case ANY_PRE_ANY: {
//                final var triples = predicates.get(tripleMatch.getPredicate());
//                return triples == null ? Stream.empty() : triples.keyStream();
//            }
//
//            case ANY_ANY_OBJ: {
//                final var triples = objects.get(tripleMatch.getObject());
//                return triples == null ? Stream.empty() : triples.keyStream();
//            }
//
//            case ANY_ANY_ANY:
//                return stream();
//
//            default:
//                throw new IllegalStateException("Unexpected value: " + PatternClassifier.classify(tripleMatch));
//        }
    }

    private Runnable createConcurrentModificationChecker() {
        final var initialSize = this.size;
        return () -> {
            if (this.size != initialSize) throw new ConcurrentModificationException();
        };
    }

    @Override
    public ExtendedIterator<Triple> find(Triple tripleMatch) {
        switch (PatternClassifier.classify(tripleMatch)) {

            case SUB_PRE_OBJ: {
                final var triples = subjects.get(tripleMatch.getSubject());
                if (triples == null) {
                    return NiceIterator.emptyIterator();
                }
                return triples.containsKey(tripleMatch) ? new SingletonIterator<>(tripleMatch) : NiceIterator.emptyIterator();
            }

            case SUB_PRE_ANY: {
                final var triplesBySubject = subjects.get(tripleMatch.getSubject());
                if (triplesBySubject == null) {
                    return NiceIterator.emptyIterator();
                }
                return triplesBySubject.keyIterator().filterKeep(t -> tripleMatch.getPredicate().equals(t.getPredicate()));
            }

            case SUB_ANY_OBJ: {
                final var triplesBySubject = subjects.get(tripleMatch.getSubject());
                if (triplesBySubject == null) {
                    return NiceIterator.emptyIterator();
                }
                return triplesBySubject.keyIterator().filterKeep(t -> tripleMatch.getObject().equals(t.getObject()));
            }

            case SUB_ANY_ANY: {
                final var triples = subjects.get(tripleMatch.getSubject());
                return triples == null ? NiceIterator.emptyIterator() : triples.keyIterator();
            }

            case ANY_PRE_OBJ: {
                final var triplesByObject = objects.get(tripleMatch.getObject());
                if (triplesByObject == null) {
                    return NiceIterator.emptyIterator();
                }
                if (triplesByObject.size() > THRESHOLD_FOR_SECONDARY_LOOKUP) {
                    final var triplesByPredicate = predicates.get(tripleMatch.getPredicate());
                    if (triplesByPredicate == null) {
                        return NiceIterator.emptyIterator();
                    }
                    return new TwoDoubleIndexListsIterator(subjects,
                            triplesByPredicate, 0,
                            triplesByObject, 1,
                            createConcurrentModificationChecker());
                }
                return new DoubleIndexListsIterator(subjects, triplesByObject, createConcurrentModificationChecker())
                        .filterKeep(t -> tripleMatch.getPredicate().equals(t.getPredicate()));
            }

            case ANY_PRE_ANY: {
                final var triples = predicates.get(tripleMatch.getPredicate());
                return triples == null
                        ? NiceIterator.emptyIterator()
                        : new DoubleIndexListsIterator(subjects, triples, createConcurrentModificationChecker());
            }

            case ANY_ANY_OBJ: {
                final var triples = objects.get(tripleMatch.getObject());
                return triples == null
                        ? NiceIterator.emptyIterator()
                        : new DoubleIndexListsIterator(subjects, triples, createConcurrentModificationChecker());
            }

            case ANY_ANY_ANY:
                return new IteratorOfJenaSets<>(subjects.valueIterator());

            default:
                throw new IllegalStateException("Unexpected value: " + PatternClassifier.classify(tripleMatch));
        }
    }

    @Override
    public FastTripleStore copy() {
        return new FastTripleStore(this);
    }

    protected static class ArrayBunchWithSameSubject extends FastArrayBunch {

        public ArrayBunchWithSameSubject() {
            super();
        }

        private ArrayBunchWithSameSubject(ArrayBunchWithSameSubject bunchToCopy) {
            super(bunchToCopy);
        }

        @Override
        public ArrayBunchWithSameSubject copy() {
            return new ArrayBunchWithSameSubject(this);
        }

        @Override
        public boolean areEqual(final Triple a, final Triple b) {
            return a.getPredicate().equals(b.getPredicate())
                    && a.getObject().equals(b.getObject());
        }
    }

    protected static class ArrayBunchWithSamePredicate extends FastArrayBunch {

        public ArrayBunchWithSamePredicate() {
            super();
        }

        private ArrayBunchWithSamePredicate(ArrayBunchWithSamePredicate bunchToCopy) {
            super(bunchToCopy);
        }

        @Override
        public ArrayBunchWithSamePredicate copy() {
            return new ArrayBunchWithSamePredicate(this);
        }

        @Override
        public boolean areEqual(final Triple a, final Triple b) {
            return a.getSubject().equals(b.getSubject())
                    && a.getObject().equals(b.getObject());
        }
    }

    protected static class ArrayBunchWithSameObject extends FastArrayBunch {

        public ArrayBunchWithSameObject() {
            super();
        }

        private ArrayBunchWithSameObject(ArrayBunchWithSameObject bunchToCopy) {
            super(bunchToCopy);
        }

        @Override
        public ArrayBunchWithSameObject copy() {
            return new ArrayBunchWithSameObject(this);
        }

        @Override
        public boolean areEqual(final Triple a, final Triple b) {
            return a.getSubject().equals(b.getSubject())
                    && a.getPredicate().equals(b.getPredicate());
        }
    }
}

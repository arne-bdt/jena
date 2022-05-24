/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.mem2;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphWithPerform;
import org.apache.jena.mem.GraphMemBase;
import org.apache.jena.mem2.generic.*;
import org.apache.jena.mem2.helper.TripleEqualsOrMatches;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.FilterIterator;
import org.apache.jena.util.iterator.Map1Iterator;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * GraphMemUsingHashMap is supposed to completely replace the original GraphMem implementation.
 *
 * This implementation basically follows the same pattern as GraphMem:
 * - all triples are stored in three hash maps:
 *   - one with subjects as key, one with predicates as key and one with objects as key
 * Main differences between GraphMemUsingHashMap and GraphMem:
 * - GraphMem uses its own hash map and triple bag implementations while GraphMemUsingHashMap uses the standard
 *   HashMap<K,V> and ArrayList<T>.
 * - GraphMemUsingHashMap optimizes find operations by
 *   - implementing every possible permutation to avoid unnecessary repeated condition checks (Node.isConcrete)
 *   - careful order of conditions to fail as fast as possible
 * - GraphMemUsingHashMap has the Graph#stream operations implemented as real java streams considering the same
 *   optimizations as the find operations and not wrapping iterators to streams.
 * - GraphMemUsingHashMap optimizes memory usage by using Node.getIndexingValue().hashCode() as hash keys instead
 *   of the Node.getIndexingValue() object itself. This is totally fine, because values are lists.
 *
 * Benchmarks show that:
 * - adding triples is much faster than on GraphMem
 * - for large graphs this implementation need less memory than GraphMem
 * - find and contains operations are a bit faster than GraphMem
 * - stream operations are faster than GraphMem and can be accelerated even more by appending .parallel()
 *
 * The ExtendedIterator<> returned by Graph#find calls supports .remove and .removeNext to make it fully compatible with the
 * usages of GraphMem in the whole jena repository.
 *
 * Adding triples while iterating on a result is not supported, but it was probably not intentional that GraphMem
 * supported this in some cases. The implementation of ModelExpansion.addDomainTypes relayed on this behaviour, but it
 * has been fixed.
 */
public class GraphMem2 extends GraphMemBase implements GraphWithPerform {

    private static final int INITIAL_SIZE_FOR_ARRAY_LISTS = 2;
    private static final int THRESHOLD_UNTIL_FIND_IS_MORE_EXPENSIVE_THAN_ITERATE = 80;

    private final IntegerKeyedLowMemoryHashSet<TripleSetWithKey> triplesBySubject = new IntegerKeyedLowMemoryHashSet<>(TripleSetWithKey::getKeyOfSet);
    private final IntegerKeyedLowMemoryHashSet<TripleSetWithKey> triplesByPredicate = new IntegerKeyedLowMemoryHashSet<>(TripleSetWithKey::getKeyOfSet);
    private final IntegerKeyedLowMemoryHashSet<TripleSetWithKey> triplesByObject = new IntegerKeyedLowMemoryHashSet<>(TripleSetWithKey::getKeyOfSet);

    private static int THRESHOLD_FOR_LOW_MEMORY_HASH_SET = 60;//60-350;

//    private static Comparator<Triple> TRIPLE_INDEXING_VALUE_HASH_CODE_COMPARATOR_FOR_TRIPLES_BY_SUBJECT =
//            Comparator.comparingInt((Triple t) -> t.getObject().getIndexingValue().hashCode())
//                    .thenComparing(t -> t.getPredicate().getIndexingValue().hashCode());
//
//    private static Comparator<Triple> TRIPLE_INDEXING_VALUE_HASH_CODE_COMPARATOR_FOR_TRIPLES_BY_PREDICATE =
//            Comparator.comparingInt((Triple t) -> t.getSubject().getIndexingValue().hashCode())
//                    .thenComparing(t -> t.getObject().getIndexingValue().hashCode());
//
//    private static Comparator<Triple> TRIPLE_INDEXING_VALUE_HASH_CODE_COMPARATOR_FOR_TRIPLES_BY_OBJECT =
//            Comparator.comparingInt((Triple t) -> t.getSubject().getIndexingValue().hashCode())
//                    .thenComparing(t -> t.getPredicate().getIndexingValue().hashCode());

    private interface TripleSetWithKey extends Set<Triple> {

        int getKeyOfSet();

        void addUnsafe(Triple t);

        void removeUnsafe(Triple t);
    }

    private static abstract class AbstractSortedTriplesSet extends ListSetBase<Triple> implements TripleSetWithKey {

        private final int keyOfSet;

        public AbstractSortedTriplesSet(final int initialCapacity, final int keyOfSet) {
            super(initialCapacity);
            this.keyOfSet = keyOfSet;
        }

        @Override
        public int getKeyOfSet() {
            return this.keyOfSet;
        }
    }

    private static abstract class AbstractLowMemoryTripleHashSet extends LowMemoryHashSet<Triple> implements TripleSetWithKey {
        private final int keyOfSet;

        public AbstractLowMemoryTripleHashSet(TripleSetWithKey setWithKey) {
            super(setWithKey);
            this.keyOfSet = setWithKey.getKeyOfSet();
        }

        @Override
        public int getKeyOfSet() {
            return this.keyOfSet;
        }
    }

    private static Function<Integer, TripleSetWithKey> createSortedListSetForTriplesBySubject
            = keyOfSet -> new AbstractSortedTriplesSet(INITIAL_SIZE_FOR_ARRAY_LISTS, keyOfSet)  {
//        @Override
//        protected Comparator<Triple> getComparator() {
//            return TRIPLE_INDEXING_VALUE_HASH_CODE_COMPARATOR_FOR_TRIPLES_BY_SUBJECT;
//        }
//
//        @Override
//        protected int getSizeToStartSorting() { return 15; }

        @Override
        protected Predicate<Triple> getContainsPredicate(Triple value) {
            if(TripleEqualsOrMatches.isEqualsForObjectOk(value.getObject())) {
                return t -> value.getObject().equals(t.getObject())
                        && value.getPredicate().equals(t.getPredicate())
                        && value.getSubject().equals(t.getSubject());
            }
            return t -> value.getObject().sameValueAs(t.getObject())
                    && value.getPredicate().equals(t.getPredicate())
                    && value.getSubject().equals(t.getSubject());
        }
    };

    private static Function<Integer, TripleSetWithKey> createSortedListSetForTriplesByPredicate
            = keyOfSet -> new AbstractSortedTriplesSet(INITIAL_SIZE_FOR_ARRAY_LISTS, keyOfSet) {
//        @Override
//        protected Comparator<Triple> getComparator() {
//            return TRIPLE_INDEXING_VALUE_HASH_CODE_COMPARATOR_FOR_TRIPLES_BY_PREDICATE;
//        }
//
//        @Override
//        protected int getSizeToStartSorting() {
//            return 15;
//        }

        @Override
        protected Predicate<Triple> getContainsPredicate(Triple value) {
            if(TripleEqualsOrMatches.isEqualsForObjectOk(value.getObject())) {
                return t -> value.getSubject().equals(t.getSubject())
                        && value.getObject().equals(t.getObject())
                        && value.getPredicate().equals(t.getPredicate());
            }
            return t -> value.getSubject().equals(t.getSubject())
                    && value.getObject().sameValueAs(t.getObject())
                    && value.getPredicate().equals(t.getPredicate());
        }
    };

    private static Function<Integer, TripleSetWithKey> createSortedListSetForTriplesByObject
            = keyOfSet -> new AbstractSortedTriplesSet(INITIAL_SIZE_FOR_ARRAY_LISTS, keyOfSet) {

//        @Override
//        protected Comparator<Triple> getComparator() {
//            return TRIPLE_INDEXING_VALUE_HASH_CODE_COMPARATOR_FOR_TRIPLES_BY_OBJECT;
//        }
//
//        @Override
//        protected int getSizeToStartSorting() {
//            return 15;
//        }

        @Override
        protected Predicate<Triple> getContainsPredicate(Triple value) {
            if(TripleEqualsOrMatches.isEqualsForObjectOk(value.getObject())) {
                return t -> value.getSubject().equals(t.getSubject())
                        && value.getPredicate().equals(t.getPredicate())
                        && value.getObject().equals(t.getObject());
            }
            return t -> value.getSubject().equals(t.getSubject())
                    && value.getPredicate().equals(t.getPredicate())
                    && value.getObject().sameValueAs(t.getObject());
        }
    };

    private static Function<TripleSetWithKey, TripleSetWithKey> createLowMemoryHashSetForTriplesBySubject
            = (tripleSet) -> new AbstractLowMemoryTripleHashSet(tripleSet) {

        @Override
        protected int getHashCode(Triple value) {
            return (value.getObject().getIndexingValue().hashCode() >> 1)
                    ^ value.getPredicate().getIndexingValue().hashCode();
        }

        @Override
        protected Predicate<Triple> getContainsPredicate(Triple value) {
            if(TripleEqualsOrMatches.isEqualsForObjectOk(value.getObject())) {
                return t -> value.getObject().equals(t.getObject())
                        && value.getPredicate().equals(t.getPredicate())
                        && value.getSubject().equals(t.getSubject());
            }
            return t -> value.getObject().sameValueAs(t.getObject())
                    && value.getPredicate().equals(t.getPredicate())
                    && value.getSubject().equals(t.getSubject());
        }
    };

    private static Function<TripleSetWithKey, TripleSetWithKey> createLowMemoryHashSetForTriplesByPredicate
            = (tripleSet) -> new AbstractLowMemoryTripleHashSet(tripleSet) {

        @Override
        protected int getHashCode(Triple value) {
            return (value.getSubject().hashCode() >> 1)
                    ^ value.getObject().hashCode();
        }

        @Override
        protected Predicate<Triple> getContainsPredicate(Triple value) {
            if(TripleEqualsOrMatches.isEqualsForObjectOk(value.getObject())) {
                return t -> value.getSubject().equals(t.getSubject())
                        && value.getObject().equals(t.getObject())
                        && value.getPredicate().equals(t.getPredicate());
            }
            return t -> value.getSubject().equals(t.getSubject())
                    && value.getObject().sameValueAs(t.getObject())
                    && value.getPredicate().equals(t.getPredicate());
        }
    };

    private static Function<TripleSetWithKey, TripleSetWithKey> createLowMemoryHashSetForTriplesByObject
            = (tripleSet) -> new AbstractLowMemoryTripleHashSet(tripleSet) {

        @Override
        protected int getHashCode(Triple value) {
            return (value.getSubject().hashCode() >> 1)
                    ^ value.getPredicate().hashCode();
        }

        @Override
        protected Predicate<Triple> getContainsPredicate(Triple value) {
            if(TripleEqualsOrMatches.isEqualsForObjectOk(value.getObject())) {
                return t -> value.getSubject().equals(t.getSubject())
                        && value.getPredicate().equals(t.getPredicate())
                        && value.getObject().equals(t.getObject());
            }
            return t -> value.getSubject().equals(t.getSubject())
                    && value.getPredicate().equals(t.getPredicate())
                    && value.getObject().sameValueAs(t.getObject());
        }
    };

    public GraphMem2() {
        super();
    }

    /**
     * Subclasses over-ride this method to release any resources they no
     * longer need once fully closed.
     */
    @Override
    protected void destroy() {
        this.triplesBySubject.clear();
        this.triplesByPredicate.clear();
        this.triplesByObject.clear();
    }

    /**
     * Add a triple to the triple store. The default implementation throws an
     * AddDeniedException; subclasses must override if they want to be able to
     * add triples.
     *
     * @param t triple to add
     */
    @SuppressWarnings("java:S1199")
    @Override
    public void performAdd(final Triple t) {
        subject:
        {
            var sKey = t.getSubject().getIndexingValue().hashCode();
            var withSameSubjectKey = this.triplesBySubject.compute(
                    sKey,
                    ts -> {
                        if(ts == null) {
                            return createSortedListSetForTriplesBySubject.apply(sKey);
                        } else if(ts.size() == THRESHOLD_FOR_LOW_MEMORY_HASH_SET) {
                            return createLowMemoryHashSetForTriplesBySubject.apply(ts);
                        }
                        return ts;
                    });
            if(!withSameSubjectKey.add(t)) {
                return;
            }
        }
        predicate:
        {
            var pKey = t.getPredicate().getIndexingValue().hashCode();
            var withSamePredicateKey = this.triplesByPredicate.compute(
                pKey,
                    ts -> {
                        if(ts == null) {
                            return createSortedListSetForTriplesByPredicate.apply(pKey);
                        } else if(ts.size() == THRESHOLD_FOR_LOW_MEMORY_HASH_SET) {
                            return createLowMemoryHashSetForTriplesByPredicate.apply(ts);
                        }
                        return ts;
                    });
            withSamePredicateKey.addUnsafe(t);
        }
        object:
        {
            var oKey = t.getObject().getIndexingValue().hashCode();
            var withSameObjectKey = this.triplesByObject.compute(
                    oKey,
                    ts -> {
                        if(ts == null) {
                            return createSortedListSetForTriplesByObject.apply(oKey);
                        } else if(ts.size() == THRESHOLD_FOR_LOW_MEMORY_HASH_SET) {
                            return createLowMemoryHashSetForTriplesByObject.apply(ts);
                        }
                        return ts;
                    });
            withSameObjectKey.addUnsafe(t);
        }
    }

    /**
     * Remove a triple from the triple store. The default implementation throws
     * a DeleteDeniedException; subclasses must override if they want to be able
     * to remove triples.
     *
     * @param t triple to delete
     */
    @SuppressWarnings("java:S1199")
    @Override
    public void performDelete(Triple t) {
        subject:
        {
            final boolean[] removed = {false};
            var sKey = t.getSubject().getIndexingValue().hashCode();
            this.triplesBySubject.compute(
                    sKey,
                    ts -> {
                        if(ts == null) {
                            return null;
                        } else if(ts.remove(t)) {
                            removed[0] = true;
                            if(ts.isEmpty()) {
                                return null; /*thereby remove key*/
                            }
                        }
                        return ts;
                    });
            if(!removed[0]) {
                return;
            }
        }
        predicate:
        {
            var pKey = t.getPredicate().getIndexingValue().hashCode();
            this.triplesByPredicate.compute(
                    pKey,
                    ts -> {
                        ts.removeUnsafe(t);
                        return ts.isEmpty() ? null : ts;
                    });
        }
        object:
        {
            var oKey = t.getObject().getIndexingValue().hashCode();
            this.triplesByObject.compute(
                    oKey,
                    ts -> {
                        ts.removeUnsafe(t);
                        return ts.isEmpty() ? null : ts;
                    });
        }
    }

    /**
     * Remove all the statements from this graph.
     */
    @Override
    public void clear() {
        super.clear(); /* deletes all triples --> could be done better but later*/
        this.triplesBySubject.clear();
        this.triplesByPredicate.clear();
        this.triplesByObject.clear();
    }

    public Pair<Set<Triple>, Predicate<Triple>> getOptimalSetAndPredicate(final Node sm, final Node pm, final Node om) {
        if (sm.isConcrete()) { // SPO:S??
            var bySubjectIndex = this.triplesBySubject
                    .getIfPresent(sm.getIndexingValue().hashCode());
            if(bySubjectIndex == null) {
                return null;
            }
            if(om.isConcrete()) { //SPO:S?0
                if(bySubjectIndex.size() < THRESHOLD_UNTIL_FIND_IS_MORE_EXPENSIVE_THAN_ITERATE) {
                    if(pm.isConcrete()) { // SPO:SPO
                        if (TripleEqualsOrMatches.isEqualsForObjectOk(om)) {
                            return Pair.of(bySubjectIndex,
                                    t -> om.equals(t.getObject())
                                            && pm.equals(t.getPredicate())
                                            && sm.equals(t.getSubject()));
                        } else {
                            return Pair.of(bySubjectIndex,
                                    t -> om.sameValueAs(t.getObject())
                                            && pm.equals(t.getPredicate())
                                            && sm.equals(t.getSubject()));
                        }
                    } else { // SPO:S*O
                        if (TripleEqualsOrMatches.isEqualsForObjectOk(om)) {
                            return Pair.of(bySubjectIndex,
                                    t -> om.equals(t.getObject())
                                            && sm.equals(t.getSubject()));
                        } else {
                            return Pair.of(bySubjectIndex,
                                    t -> om.sameValueAs(t.getObject())
                                            && sm.equals(t.getSubject()));
                        }
                    }
                } else {
                    var byObjectIndex = this.triplesByObject
                            .getIfPresent(om.getIndexingValue().hashCode());
                    if (byObjectIndex == null) {
                        return null;
                    }
                    if(bySubjectIndex.size() <= byObjectIndex.size()) {
                        if (pm.isConcrete()) { // SPO:SPO
                            if (TripleEqualsOrMatches.isEqualsForObjectOk(om)) {
                                return Pair.of(bySubjectIndex,
                                        t -> om.equals(t.getObject())
                                                && pm.equals(t.getPredicate())
                                                && sm.equals(t.getSubject()));
                            } else {
                                return Pair.of(bySubjectIndex,
                                        t -> om.sameValueAs(t.getObject())
                                                && pm.equals(t.getPredicate())
                                                && sm.equals(t.getSubject()));
                            }
                        } else { // SPO:S*O
                            if (TripleEqualsOrMatches.isEqualsForObjectOk(om)) {
                                return Pair.of(bySubjectIndex,
                                        t -> om.equals(t.getObject())
                                                && sm.equals(t.getSubject()));
                            } else {
                                return Pair.of(bySubjectIndex,
                                        t -> om.sameValueAs(t.getObject())
                                                && sm.equals(t.getSubject()));
                            }
                        }
                    } else {
                        if (pm.isConcrete()) { // SPO:SPO
                            if (TripleEqualsOrMatches.isEqualsForObjectOk(om)) {
                                return Pair.of(byObjectIndex,
                                        t -> sm.equals(t.getSubject())
                                                && pm.equals(t.getPredicate())
                                                && om.equals(t.getObject()));
                            } else {
                                return Pair.of(byObjectIndex,
                                        t ->  sm.equals(t.getSubject())
                                                && pm.equals(t.getPredicate())
                                                && om.sameValueAs(t.getObject()));
                            }
                        } else { // SPO:S*O
                            if (TripleEqualsOrMatches.isEqualsForObjectOk(om)) {
                                return Pair.of(byObjectIndex,
                                        t -> sm.equals(t.getSubject())
                                                && om.equals(t.getObject()));
                            } else {
                                return Pair.of(byObjectIndex,
                                        t -> sm.equals(t.getSubject())
                                                && om.sameValueAs(t.getObject()));
                            }
                        }
                    }
                }
            } else if(pm.isConcrete()) { //SPO: SP*
                return Pair.of(bySubjectIndex,
                        t -> pm.equals(t.getPredicate())
                                && sm.equals(t.getSubject()));
            } else { // SPO:S**
                return Pair.of(bySubjectIndex,
                        t -> sm.equals(t.getSubject()));
            }
        } else if(om.isConcrete()) { // SPO:*?O
            var byObjectIndex = this.triplesByObject
                    .getIfPresent(om.getIndexingValue().hashCode());
            if(byObjectIndex == null) {
                return null;
            }
            if(pm.isConcrete()) { // SPO:*PO
                if(byObjectIndex.size() < THRESHOLD_UNTIL_FIND_IS_MORE_EXPENSIVE_THAN_ITERATE) {
                    if (TripleEqualsOrMatches.isEqualsForObjectOk(om)) {
                        return Pair.of(byObjectIndex,
                                t -> pm.equals(t.getPredicate())
                                        && om.equals(t.getObject()));
                    } else {
                        return Pair.of(byObjectIndex,
                                t ->  pm.equals(t.getPredicate())
                                        && om.sameValueAs(t.getObject()));
                    }
                } else {
                    var byPredicateIndex = this.triplesByPredicate
                            .getIfPresent(pm.getIndexingValue().hashCode());
                    if(byPredicateIndex == null) {
                        return null;
                    }
                    if(byObjectIndex.size() <= byPredicateIndex.size()) {
                        if (TripleEqualsOrMatches.isEqualsForObjectOk(om)) {
                            return Pair.of(byObjectIndex,
                                    t -> pm.equals(t.getPredicate())
                                            && om.equals(t.getObject()));
                        } else {
                            return Pair.of(byObjectIndex,
                                    t ->  pm.equals(t.getPredicate())
                                            && om.sameValueAs(t.getObject()));
                        }
                    } else {
                        if (TripleEqualsOrMatches.isEqualsForObjectOk(om)) {
                            return Pair.of(byPredicateIndex,
                                    t -> om.equals(t.getObject())
                                            && pm.equals(t.getPredicate()));
                        } else {
                            return Pair.of(byPredicateIndex,
                                    t ->  om.sameValueAs(t.getObject())
                                            && pm.equals(t.getPredicate()));
                        }
                    }
                }
            } else {    // SPO:**O
                if (TripleEqualsOrMatches.isEqualsForObjectOk(om)) {
                    return Pair.of(byObjectIndex,
                            t -> om.equals(t.getObject()));
                } else {
                    return Pair.of(byObjectIndex,
                            t -> om.sameValueAs(t.getObject()));
                }
            }
        } else if(pm.isConcrete()) { //SPO:*P*
            var byPredicateIndex = this.triplesByPredicate
                    .getIfPresent(pm.getIndexingValue().hashCode());
            if(byPredicateIndex == null) {
                return null;
            }
            return Pair.of(byPredicateIndex,
                    t -> pm.equals(t.getPredicate()));
        } else { // SPO:***
            throw new IllegalArgumentException("All node are Node.ANY, which is not allowed here.");
        }
    }

    /**
     * Answer true if the graph contains any triple matching <code>t</code>.
     * The default implementation uses <code>find</code> and checks to see
     * if the iterator is non-empty.
     *
     * @param triple triple which may be contained
     */
    @SuppressWarnings("java:S3776")
    @Override
    protected boolean graphBaseContains(Triple triple) {
        final Node sm = triple.getSubject();
        final Node pm = triple.getPredicate();
        final Node om = triple.getObject();

        if(sm.isConcrete() || pm.isConcrete() || om.isConcrete()) {
            if(sm.isConcrete() && pm.isConcrete() && om.isConcrete()) {
                var subjects = triplesBySubject.getIfPresent(sm.getIndexingValue().hashCode());
                if(subjects == null) {
                    return false;
                }
                if(subjects.size() < THRESHOLD_UNTIL_FIND_IS_MORE_EXPENSIVE_THAN_ITERATE) {
                    return subjects.contains(triple);
                } else {
                    var objects = triplesByObject.getIfPresent(om.getIndexingValue().hashCode());
                    if(objects == null) {
                        return false;
                    }
                    return objects.size() < subjects.size() ? objects.contains(triple) : subjects.contains(triple);
                }
            } else {
                var setAndPredicatePair = getOptimalSetAndPredicate(sm, pm, om);
                if (setAndPredicatePair == null) {
                    return false;
                }
                for (Triple t : setAndPredicatePair.getKey()) {
                    if (setAndPredicatePair.getValue().test(t)) {
                        return true;
                    }
                }
                return false;
            }
        } else {
            return !this.triplesBySubject.isEmpty();
        }
    }

    /**
     * Determines the map with the fewest keys.
     * This should be helpful in any case where one needs all lists of triples.
     * Its use makes obsolete the possibly false assumption that there are always
     * fewer predicates than subjects or objects.
     * @return
     */
    private IntegerKeyedLowMemoryHashSet<TripleSetWithKey> getMapWithFewestKeys() {
        var subjectCount = this.triplesBySubject.size();
        var predicateCount = this.triplesByPredicate.size();
        var objectCount = this.triplesByObject.size();
        if(subjectCount < predicateCount) {
            if(subjectCount < objectCount) {
                return this.triplesBySubject;
            } else {
                return this.triplesByObject;
            }
        } else {
            if(predicateCount < objectCount) {
                return this.triplesByPredicate;
            } else {
                return this.triplesByObject;
            }
        }
    }

    /**
     * Answer the number of triples in this graph. Default implementation counts its
     * way through the results of a findAll. Subclasses must override if they want
     * size() to be efficient.
     */
    @Override
    protected int graphBaseSize() {
        /*use the map with the fewest keys*/
        return this.getMapWithFewestKeys().stream().mapToInt(Set::size).sum();
    }

    /**
     * Returns a {@link Stream} of all triples in the graph.
     * Note: Caller may add .parallel() to improve performance.
     *
     * @return a stream  of triples in this graph.
     */
    @Override
    public Stream<Triple> stream() {
        /*use the map with the fewest keys*/
        return this.getMapWithFewestKeys().stream().flatMap(Collection::stream);
    }

    /**
     * Returns a {@link Stream} of Triples matching a pattern.
     * Note: Caller may add .parallel() to improve performance.
     * @param s subject node
     * @param p predicate node
     * @param o object node
     * @return a stream  of triples in this graph matching the pattern.
     */
    @SuppressWarnings("java:S3776")
    @Override
    public Stream<Triple> stream(final Node s, final Node p, final Node o) {
        final Stream<Triple> result;
        final var sm = null == s ? Node.ANY : s;
        final var pm = null == p ? Node.ANY : p;
        final var om = null == o ? Node.ANY : o;

        if(sm.isConcrete() || pm.isConcrete() || om.isConcrete()) {
            var setAndPredicatePair = getOptimalSetAndPredicate(sm, pm, om);
            if(setAndPredicatePair == null) {
                return Stream.empty();
            }
            return setAndPredicatePair.getKey()
                    .stream()
                    .filter(setAndPredicatePair.getValue());
        } else {
            return this.stream();
        }
    }



    @SuppressWarnings("java:S3776")
    @Override
    public ExtendedIterator<Triple> graphBaseFind(Triple triplePattern) {
        final Node sm = triplePattern.getSubject();
        final Node pm = triplePattern.getPredicate();
        final Node om = triplePattern.getObject();
        final Iterator<Triple> iterator;

        if(sm.isConcrete() || pm.isConcrete() || om.isConcrete()) {
            var setAndPredicatePair = getOptimalSetAndPredicate(sm, pm, om);
            if(setAndPredicatePair == null) {
                return NiceIterator.emptyIterator();
            }
            iterator = new IteratorFiltering(setAndPredicatePair.getKey().iterator(), setAndPredicatePair.getValue());
        } else {
            /*use the map with the fewest keys*/
            iterator = new ListsOfTriplesIterator(this.getMapWithFewestKeys().iterator());
        }
        return new IteratorWrapperWithRemove(iterator, this);
    }

    private static class ListsOfTriplesIterator implements Iterator<Triple> {

        private final Iterator<TripleSetWithKey> baseIterator;
        private Iterator<Triple> subIterator;
        boolean hasNext = false;

        public ListsOfTriplesIterator(Iterator<TripleSetWithKey> baseIterator) {

            this.baseIterator = baseIterator;
            subIterator = baseIterator.hasNext() ? baseIterator.next().iterator() : Collections.emptyIterator();
        }

        /**
         * Returns {@code true} if the iteration has more elements.
         * (In other words, returns {@code true} if {@link #next} would
         * return an element rather than throwing an exception.)
         *
         * @return {@code true} if the iteration has more elements
         */
        @Override
        public boolean hasNext() {
            if(subIterator.hasNext()) {
                return hasNext = true;
            }
            while(baseIterator.hasNext()) {
                if((subIterator = baseIterator.next().iterator()).hasNext()) {
                    return hasNext = true;
                }
            }
            return hasNext = false;
        }

        /**
         * Returns the next element in the iteration.
         *
         * @return the next element in the iteration
         * @throws NoSuchElementException if the iteration has no more elements
         */
        @Override
        public Triple next() {
            if(hasNext || this.hasNext()) {
                hasNext = false;
                return subIterator.next();
            }
            throw new NoSuchElementException();
        }
    }

    /**
     *  Wrapper for Iterator<Triple> which supports .remove and .removeNext, which deletes triples from the graph.
     *  It is done by simply replacing the wrapped iterator with .toList().iterator().
     */
    private static class IteratorWrapperWithRemove implements ExtendedIterator<Triple> {

        private Iterator<Triple> iterator;
        private final GraphMem2 graphMem;
        private boolean isStillIteratorWithNoRemove = true;

        /**
         The remembered current triple. Subclass should *not* assign to this variable.
         */
        protected Triple current;

        public IteratorWrapperWithRemove(Iterator<Triple> iteratorWithNoRemove, GraphMem2 graphMem) {
            this.iterator = iteratorWithNoRemove;
            this.graphMem = graphMem;
        }

        /**
         * Close the iterator. Other operations on this iterator may now throw an exception.
         * A ClosableIterator may be closed as many times as desired - the subsequent
         * calls do nothing.
         */
        @Override
        public void close() {
            /*this class can only wrap Iterator<>, which has no close method*/
        }

        /**
         * Answer the next object, and remove it. Equivalent to next(); remove().
         */
        @Override
        public Triple removeNext() {
            throw new NotImplementedException();
//            Triple result = next();
//            remove();
//            return result;
        }

        /**
         * return a new iterator which delivers all the elements of this iterator and
         * then all the elements of the other iterator. Does not copy either iterator;
         * they are consumed as the result iterator is consumed.
         *
         * @param other iterator to append
         */
        @Override
        public <X extends Triple> ExtendedIterator<Triple> andThen(Iterator<X> other) {
            return NiceIterator.andThen( this, other );
        }

        /**
         * return a new iterator containing only the elements of _this_ which
         * pass the filter _f_. The order of the elements is preserved. Does not
         * copy _this_, which is consumed as the result is consumed.
         *
         * @param f filter predicate
         */
        @Override
        public ExtendedIterator<Triple> filterKeep(Predicate<Triple> f) {
            return new FilterIterator<>( f, this );
        }

        /**
         * return a new iterator containing only the elements of _this_ which
         * are rejected by the filter _f_. The order of the elements is preserved.
         * Does not copy _this_, which is consumed as the result is consumed.
         *
         * @param f filter predicate
         */
        @Override
        public ExtendedIterator<Triple> filterDrop(Predicate<Triple> f) {
            return new FilterIterator<>( f.negate(), this );
        }

        /**
         * return a new iterator where each element is the result of applying
         * _map1_ to the corresponding element of _this_. _this_ is not
         * copied; it is consumed as the result is consumed.
         *
         * @param map1 mapping function
         */
        @Override
        public <U> ExtendedIterator<U> mapWith(Function<Triple, U> map1) {
            return new Map1Iterator<>( map1, this );
        }

        /**
         * Answer a list of the [remaining] elements of this iterator, in order,
         * consuming this iterator.
         */
        @Override
        public List<Triple> toList() {
            return NiceIterator.asList(this);
        }

        /**
         * Answer a set of the [remaining] elements of this iterator,
         * consuming this iterator.
         */
        @Override
        public Set<Triple> toSet() {
            return NiceIterator.asSet(this);
        }

        /**
         * Returns {@code true} if the iteration has more elements.
         * (In other words, returns {@code true} if {@link #next} would
         * return an element rather than throwing an exception.)
         *
         * @return {@code true} if the iteration has more elements
         */
        @Override
        public boolean hasNext() {
            return this.iterator.hasNext();
        }

        /**
         * Returns the next element in the iteration.
         *
         * @return the next element in the iteration
         * @throws NoSuchElementException if the iteration has no more elements
         */
        @Override
        public Triple next() {
            return current = this.iterator.next();
        }

        /**
         * Removes from the underlying collection the last element returned
         * by this iterator (optional operation).  This method can be called
         * only once per call to {@link #next}.
         * <p>
         * The behavior of an iterator is unspecified if the underlying collection
         * is modified while the iteration is in progress in any way other than by
         * calling this method, unless an overriding class has specified a
         * concurrent modification policy.
         * <p>
         * The behavior of an iterator is unspecified if this method is called
         * after a call to the {@link #forEachRemaining forEachRemaining} method.
         *
         * @throws UnsupportedOperationException if the {@code remove}
         *                                       operation is not supported by this iterator
         * @throws IllegalStateException         if the {@code next} method has not
         *                                       yet been called, or the {@code remove} method has already
         *                                       been called after the last call to the {@code next}
         *                                       method
         * @implSpec The default implementation throws an instance of
         * {@link UnsupportedOperationException} and performs no other action.
         */
        @Override
        public void remove() {
            if(isStillIteratorWithNoRemove) {
                var currentBeforeToList = current;
                this.iterator = this.toList().iterator();
                this.isStillIteratorWithNoRemove = false;
                graphMem.delete(currentBeforeToList);
            } else {
                graphMem.delete(current);
            }
        }
    }

    /**
     * Basically the same as FilterIterator<> but with clear and simple implementation without inheriting possibly
     * strange behaviour from any of the base classes.
     * This Iterator also directly supports wrapWithRemoveSupport
     */
    private static class IteratorFiltering implements Iterator<Triple> {

        private final Predicate<Triple> filter;
        private final Iterator<Triple> iterator;
        private boolean hasCurrent = false;
        /**
         The remembered current triple.
         */
        private Triple current;

        /**
         * Initialise this wrapping with the given base iterator and remove-control.
         *
         * @param iterator      the base iterator
         * @param filter        the filter predicate for this iteration
         */
        protected IteratorFiltering(Iterator<Triple> iterator, Predicate<Triple> filter) {
            this.iterator = iterator;
            this.filter = filter;
        }

        /**
         * Returns {@code true} if the iteration has more elements.
         * (In other words, returns {@code true} if {@link #next} would
         * return an element rather than throwing an exception.)
         *
         * @return {@code true} if the iteration has more elements
         */
        @Override
        public boolean hasNext() {
            while(!this.hasCurrent && this.iterator.hasNext()) {
                if(filter.test(current = this.iterator.next())) {
                    hasCurrent = true;
                }
            }
            return this.hasCurrent;
        }

        /**
         Answer the next object, remembering it in <code>current</code>.
         @see Iterator#next()
         */
        @Override
        public Triple next()
        {
            if (hasCurrent || hasNext())
            {
                hasCurrent = false;
                return current;
            }
            throw new NoSuchElementException();
        }
    }
}
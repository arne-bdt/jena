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
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphWithPerform;
import org.apache.jena.mem.GraphMemBase;
import org.apache.jena.mem2.generic.IntegerKeyedLowMemoryHashSet;
import org.apache.jena.mem2.generic.ListSetBase;
import org.apache.jena.mem2.generic.LowMemoryHashSet;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.FilterIterator;
import org.apache.jena.util.iterator.Map1Iterator;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * GraphMem2 is supposed to completely replace the original GraphMem implementation.
 *
 * This implementation basically follows the same pattern as GraphMem:
 * - all triples are stored in three hash maps:
 *   - one with subjects as key, one with predicates as key and one with objects as key
 * Main differences between GraphMemUsingHashMap and GraphMem:
 * - GraphMem2 optimizes find operations by
 *   - implementing every possible permutation to avoid unnecessary repeated condition checks (Node.isConcrete)
 *   - careful order of conditions to fail as fast as possible
 * - GraphMem2 has the Graph#stream operations implemented as real java streams considering the same
 *   optimizations as the find operations and not wrapping iterators to streams.
 * - GraphMem2 optimizes memory usage by using Node.getIndexingValue().hashCode() as hash keys instead
 *   of the Node.getIndexingValue() object itself. This is totally fine, because values are lists.
 *
 * Benchmarks show that:
 * - adding triples is faster than on GraphMem
 * - for large graphs this implementation need less memory than GraphMem
 * - stream operations are faster than GraphMem and can be accelerated even more by appending .parallel()
 *
 * The ExtendedIterator<> returned by Graph#find calls supports .remove to make it fully compatible with the
 * usages of GraphMem in the whole jena repository.
 *
 * Adding triples while iterating on a result is not supported, but it was probably not intentional that GraphMem
 * supported this in some cases. The implementation of ModelExpansion.addDomainTypes relayed on this behaviour, but it
 * has been fixed.
 */
public class GraphMem2 extends GraphMemBase implements GraphWithPerform {

    private static final int INITIAL_SIZE_FOR_ARRAY_LISTS = 2;
    private static final int THRESHOLD_UNTIL_FIND_IS_MORE_EXPENSIVE_THAN_ITERATE = 80;

    private static class KeyedHashSet extends IntegerKeyedLowMemoryHashSet<TripleSetWithKey>{
        @Override
        protected int getKey(TripleSetWithKey value) {
            return value.getKeyOfSet();
        }
    }

    private final KeyedHashSet triplesBySubject = new KeyedHashSet();
    private final KeyedHashSet triplesByPredicate = new KeyedHashSet();
    private final KeyedHashSet triplesByObject = new KeyedHashSet();

    private static int THRESHOLD_FOR_LOW_MEMORY_HASH_SET = 60;//60-350;

    private interface TripleSetWithKey extends Set<Triple> {

        int getKeyOfSet();

        void addUnsafe(Triple t);

        default boolean add(Triple t, int hashCode) {
            return this.add(t);
        }

        default void addUnsafe(Triple t, int hashCode) {
            this.addUnsafe(t);
        }

        default boolean remove(Triple t, int hashCode) {
            return remove(t);
        }

        void removeUnsafe(Triple t);

        default void removeUnsafe(Triple t, int hashCode) {
            this.removeUnsafe(t);
        }
    }

    private static abstract class AbstractSortedTriplesSet extends ListSetBase<Triple> implements TripleSetWithKey {

        private final int keyOfSet;

        public AbstractSortedTriplesSet(final int keyOfSet) {
            super(INITIAL_SIZE_FOR_ARRAY_LISTS);
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

    private static class TripleListSetForSubjects extends AbstractSortedTriplesSet {

        public TripleListSetForSubjects(int keyOfSet) {
            super(keyOfSet);
        }

        @Override
        public boolean contains(Object o) {
            var triple = (Triple)o;
            for (Triple t : this) {
                if(triple.getObject().sameValueAs(t.getObject())
                        && triple.getPredicate().equals(t.getPredicate())
                        && triple.getSubject().equals(t.getSubject())) {
                    return true;
                }
            }
            return false;
        }
    }

    private static class TripleListSetForPredicates extends AbstractSortedTriplesSet {

        public TripleListSetForPredicates(int keyOfSet) {
            super(keyOfSet);
        }

        @Override
        public boolean contains(Object o) {
            var triple = (Triple)o;
            for (Triple t : this) {
                if(triple.getSubject().equals(t.getSubject())
                        && triple.getObject().sameValueAs(t.getObject())
                        && triple.getPredicate().equals(t.getPredicate())) {
                    return true;
                }
            }
            return false;
        }
    }


    private static class TripleListSetForObjects extends AbstractSortedTriplesSet {

        public TripleListSetForObjects(int keyOfSet) {
            super(keyOfSet);
        }

        @Override
        public boolean contains(Object o) {
            var triple = (Triple)o;
            for (Triple t : this) {
                if(triple.getSubject().equals(t.getSubject())
                        && triple.getPredicate().equals(t.getPredicate())
                        && triple.getObject().sameValueAs(t.getObject())) {
                    return true;
                }
            }
            return false;
        }
    }

    private static class TripleHashSetForSubjects extends AbstractLowMemoryTripleHashSet {

        public TripleHashSetForSubjects(TripleSetWithKey setWithKey) {
            super(setWithKey);
        }

        @Override
        protected int getHashCode(Triple value) {
            return combineNodeHashes(value.getObject().hashCode(), value.getPredicate().hashCode());
        }

        @Override
        protected Predicate<Triple> getContainsPredicate(Triple value) {
            return t -> value.getObject().sameValueAs(t.getObject())
                    && value.getPredicate().equals(t.getPredicate())
                    && value.getSubject().equals(t.getSubject());
        }
    }


    private static class TripleHashSetForPredicates extends AbstractLowMemoryTripleHashSet {

        public TripleHashSetForPredicates(TripleSetWithKey setWithKey) {
            super(setWithKey);
        }

        @Override
        protected int getHashCode(Triple value) {
            return combineNodeHashes(value.getObject().getIndexingValue().hashCode(), value.getSubject().hashCode());
        }

        @Override
        protected Predicate<Triple> getContainsPredicate(Triple value) {
            return t -> value.getSubject().equals(t.getSubject())
                    && value.getObject().sameValueAs(t.getObject())
                    && value.getPredicate().equals(t.getPredicate());
        }
    }


    private static class TripleHashSetForObjects extends AbstractLowMemoryTripleHashSet {

        public TripleHashSetForObjects(TripleSetWithKey setWithKey) {
            super(setWithKey);
        }

        @Override
        protected int getHashCode(Triple value) {
            return combineNodeHashes(value.getPredicate().hashCode(), value.getSubject().hashCode());
        }

        @Override
        protected Predicate<Triple> getContainsPredicate(Triple value) {
            return t -> value.getSubject().equals(t.getSubject())
                    && value.getPredicate().equals(t.getPredicate())
                    && value.getObject().sameValueAs(t.getObject());
        }
    }


    private static int combineNodeHashes(final int hashCodeOne, final int hashCodeTwo) {
        return (31 * hashCodeOne) + hashCodeTwo;
    }

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
        var hashKeyOfSubject = t.getSubject().hashCode();
        var hashKeyOfPredicate = t.getPredicate().hashCode();
        var hashKeyOfObject = t.getObject().hashCode();
        var hashKeyOfObjectIndexing = t.getObject().getIndexingValue().hashCode();
        subject:
        {
            var sKey = t.getSubject().getIndexingValue() == t.getSubject()
                    ? hashKeyOfSubject
                    : t.getSubject().getIndexingValue().hashCode();
            var withSameSubjectKey = this.triplesBySubject.compute(
                    sKey,
                    ts -> {
                        if(ts == null) {
                            return new TripleListSetForSubjects(sKey);
                        } else if(ts.size() == THRESHOLD_FOR_LOW_MEMORY_HASH_SET) {
                            return new TripleHashSetForSubjects(ts);
                        }
                        return ts;
                    });
            if(!withSameSubjectKey.add(t,  combineNodeHashes(hashKeyOfObject, hashKeyOfPredicate))) {
                return;
            }
        }
        predicate:
        {
            var pKey = t.getPredicate().getIndexingValue() == t.getPredicate()
                    ? hashKeyOfPredicate
                    : t.getPredicate().getIndexingValue().hashCode();
            var withSamePredicateKey = this.triplesByPredicate.compute(
                pKey,
                    ts -> {
                        if(ts == null) {
                            return new TripleListSetForPredicates(pKey);
                        } else if(ts.size() == THRESHOLD_FOR_LOW_MEMORY_HASH_SET) {
                            return new TripleHashSetForPredicates(ts);
                        }
                        return ts;
                    });
            withSamePredicateKey.addUnsafe(t, combineNodeHashes(hashKeyOfObjectIndexing, hashKeyOfSubject));
        }
        object:
        {
            var withSameObjectKey = this.triplesByObject.compute(
                    hashKeyOfObjectIndexing,
                    ts -> {
                        if(ts == null) {
                            return new TripleListSetForObjects(hashKeyOfObjectIndexing);
                        } else if(ts.size() == THRESHOLD_FOR_LOW_MEMORY_HASH_SET) {
                            return new TripleHashSetForObjects(ts);
                        }
                        return ts;
                    });
            withSameObjectKey.addUnsafe(t, combineNodeHashes(hashKeyOfPredicate, hashKeyOfSubject));
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
        var hashKeyOfSubject = t.getSubject().hashCode();
        var hashKeyOfPredicate = t.getPredicate().hashCode();
        var hashKeyOfObject = t.getObject().hashCode();
        var hashKeyOfObjectIndexing = t.getObject().getIndexingValue().hashCode();
        subject:
        {
            final boolean[] removed = {false};
            this.triplesBySubject.compute(
                    t.getSubject().getIndexingValue() == t.getSubject()
                            ? hashKeyOfSubject
                            : t.getSubject().getIndexingValue().hashCode(),
                    ts -> {
                        if(ts == null) {
                            return null;
                        } else if(ts.remove(t, combineNodeHashes(hashKeyOfObject, hashKeyOfPredicate))) {
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
            this.triplesByPredicate.compute(
                    t.getPredicate().getIndexingValue() == t.getPredicate()
                            ? hashKeyOfPredicate
                            : t.getPredicate().getIndexingValue().hashCode(),
                    ts -> {
                        ts.removeUnsafe(t, combineNodeHashes(hashKeyOfObjectIndexing, hashKeyOfSubject));
                        return ts.isEmpty() ? null : ts;
                    });
        }
        object:
        {
            this.triplesByObject.compute(
                    hashKeyOfObjectIndexing,
                    ts -> {
                        ts.removeUnsafe(t, combineNodeHashes(hashKeyOfPredicate, hashKeyOfSubject));
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
                        return Pair.of(bySubjectIndex,
                                t -> om.sameValueAs(t.getObject())
                                        && pm.equals(t.getPredicate())
                                        && sm.equals(t.getSubject()));
                    } else { // SPO:S*O
                        return Pair.of(bySubjectIndex,
                                t -> om.sameValueAs(t.getObject())
                                        && sm.equals(t.getSubject()));
                    }
                } else {
                    var byObjectIndex = this.triplesByObject
                            .getIfPresent(om.getIndexingValue().hashCode());
                    if (byObjectIndex == null) {
                        return null;
                    }
                    if(bySubjectIndex.size() <= byObjectIndex.size()) {
                        if (pm.isConcrete()) { // SPO:SPO
                            return Pair.of(bySubjectIndex,
                                    t -> om.sameValueAs(t.getObject())
                                            && pm.equals(t.getPredicate())
                                            && sm.equals(t.getSubject()));
                        } else { // SPO:S*O
                            return Pair.of(bySubjectIndex,
                                    t -> om.sameValueAs(t.getObject())
                                            && sm.equals(t.getSubject()));
                        }
                    } else {
                        if (pm.isConcrete()) { // SPO:SPO
                            return Pair.of(byObjectIndex,
                                    t ->  sm.equals(t.getSubject())
                                            && pm.equals(t.getPredicate())
                                            && om.sameValueAs(t.getObject()));
                        } else { // SPO:S*O
                            return Pair.of(byObjectIndex,
                                    t -> sm.equals(t.getSubject())
                                            && om.sameValueAs(t.getObject()));
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
                    return Pair.of(byObjectIndex,
                            t -> pm.equals(t.getPredicate())
                                    && om.sameValueAs(t.getObject()));
                } else {
                    var byPredicateIndex = this.triplesByPredicate
                            .getIfPresent(pm.getIndexingValue().hashCode());
                    if(byPredicateIndex == null) {
                        return null;
                    }
                    if(byObjectIndex.size() <= byPredicateIndex.size()) {
                        return Pair.of(byObjectIndex,
                                t -> pm.equals(t.getPredicate())
                                        && om.sameValueAs(t.getObject()));
                    } else {
                        return Pair.of(byPredicateIndex,
                                t -> om.sameValueAs(t.getObject())
                                        && pm.equals(t.getPredicate()));
                    }
                }
            } else {    // SPO:**O
                return Pair.of(byObjectIndex,
                        t -> om.sameValueAs(t.getObject()));
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

        if(sm.isConcrete() || pm.isConcrete() || om.isConcrete()) {
            var setAndPredicatePair = getOptimalSetAndPredicate(sm, pm, om);
            if(setAndPredicatePair == null) {
                return NiceIterator.emptyIterator();
            }
            return new IteratorFiltering(setAndPredicatePair.getKey().iterator(), setAndPredicatePair.getValue(), this);
        } else {
            /*use the map with the fewest keys*/
            return new ListsOfTriplesIterator(this.getMapWithFewestKeys().iterator(), this);
        }
    }

    private static class ListsOfTriplesIterator implements ExtendedIterator<Triple> {
        private static final Iterator<TripleSetWithKey> EMPTY_SET_ITERATOR = Collections.emptyIterator();
        private static final Iterator<Triple> EMPTY_TRIPLES_ITERATOR = Collections.emptyIterator();
        private final Graph graph;

        private Iterator<TripleSetWithKey> baseIterator;
        private Iterator<Triple> subIterator;
        private Triple current;

        public ListsOfTriplesIterator(Iterator<TripleSetWithKey> baseIterator, Graph graph) {
            this.graph = graph;
            this.baseIterator = baseIterator;
            subIterator = baseIterator.hasNext() ? baseIterator.next().iterator() : EMPTY_TRIPLES_ITERATOR;
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
                return true;
            }
            while(baseIterator.hasNext()) {
                if((subIterator = baseIterator.next().iterator()).hasNext()) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Returns the next element in the iteration.
         *
         * @return the next element in the iteration
         * @throws NoSuchElementException if the iteration has no more elements
         */
        @Override
        public Triple next() {
            return current = subIterator.next();
        }

        @Override
        public void remove() {
            if(current == null) {
                throw new IllegalStateException();
            }
            if(this.baseIterator == EMPTY_SET_ITERATOR) {
                graph.delete(current);
            } else {
                var currentBeforeToList = current;
                this.subIterator = this.toList().iterator();
                this.baseIterator = EMPTY_SET_ITERATOR;
                graph.delete(currentBeforeToList);
            }
        }

        @Override
        public void close() {

        }

        @Override
        public Triple removeNext() {
            throw new NotImplementedException();
        }

        @Override
        public <X extends Triple> ExtendedIterator<Triple> andThen(Iterator<X> other) {
            return NiceIterator.andThen( this, other );
        }

        @Override
        public ExtendedIterator<Triple> filterKeep(Predicate<Triple> f) {
            return new FilterIterator<>( f, this );
        }

        @Override
        public ExtendedIterator<Triple> filterDrop(Predicate<Triple> f) {
            return new FilterIterator<>( f.negate(), this );
        }

        @Override
        public <U> ExtendedIterator<U> mapWith(Function<Triple, U> map1) {
            return new Map1Iterator<>( map1, this );
        }

        @Override
        public List<Triple> toList() {
            return NiceIterator.asList(this);
        }

        @Override
        public Set<Triple> toSet() {
            return NiceIterator.asSet(this);
        }
    }

    /**
     * Basically the same as FilterIterator<> but with clear and simple implementation without inheriting possibly
     * strange behaviour from any of the base classes.
     * This Iterator also directly supports wrapWithRemoveSupport
     */
    private static class IteratorFiltering implements ExtendedIterator<Triple> {

        private final static Predicate<Triple> FILTER_ANY = t -> true;
        private final Graph graph;
        private Predicate<Triple> filter;
        private Iterator<Triple> iterator;
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
        protected IteratorFiltering(Iterator<Triple> iterator, Predicate<Triple> filter, Graph graph) {
            this.graph = graph;
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
            if(hasCurrent) {
                return true;
            }
            while(iterator.hasNext()) {
                if (filter.test(current = this.iterator.next())) {
                    return hasCurrent = true;
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

        @Override
        public void remove() {
            if(current != null) {
                throw new IllegalStateException();
            }
            if(this.filter == FILTER_ANY) {
                graph.delete(current);
            } else {
                var currentBeforeToList = current;
                this.iterator = this.toList().iterator();
                this.filter = FILTER_ANY;
                graph.delete(currentBeforeToList);
            }
        }

        @Override
        public void close() {

        }

        @Override
        public Triple removeNext() {
            throw new NotImplementedException();
        }

        @Override
        public <X extends Triple> ExtendedIterator<Triple> andThen(Iterator<X> other) {
            return NiceIterator.andThen( this, other );
        }

        @Override
        public ExtendedIterator<Triple> filterKeep(Predicate<Triple> f) {
            return new FilterIterator<>( f, this );
        }

        @Override
        public ExtendedIterator<Triple> filterDrop(Predicate<Triple> f) {
            return new FilterIterator<>( f.negate(), this );
        }

        @Override
        public <U> ExtendedIterator<U> mapWith(Function<Triple, U> map1) {
            return new Map1Iterator<>( map1, this );
        }

        @Override
        public List<Triple> toList() {
            return NiceIterator.asList(this);
        }

        @Override
        public Set<Triple> toSet() {
            return NiceIterator.asSet(this);
        }
    }
}
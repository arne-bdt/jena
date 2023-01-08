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

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphWithPerform;
import org.apache.jena.mem.GraphMemBase;
import org.apache.jena.mem2.generic.ListSetBase;
import org.apache.jena.mem2.specialized.FastHashMapBase;
import org.apache.jena.mem2.specialized.TripleSetWithIndexingValue;
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
public class GraphMem2UsingHashes2 extends GraphMemBase implements GraphWithPerform {

    private TriplesBySubjectAndPredicate sp = new TriplesBySubjectAndPredicate();
    private TriplesByPredicateAndObject po = new TriplesByPredicateAndObject();
    private TriplesByObjectAndSubject os = new TriplesByObjectAndSubject();

    public class TripleWithHashCodes {
        public final int[] hashes;
        public final Triple triple;

        public TripleWithHashCodes(Triple triple) {
            this.triple = triple;
            this.hashes = new int[4];
            this.hashes[1] = triple.getSubject().getIndexingValue().hashCode();
            this.hashes[2] = triple.getPredicate().getIndexingValue().hashCode();
            this.hashes[3] = triple.getObject().getIndexingValue().hashCode();
//            /**
//             * same as in {@link org.apache.jena.graph.Triple.hashCode(org.apache.jena.graph.Node, org.apache.jena.graph.Node, org.apache.jena.graph.Node)}
//             **/
//            this.hashes[0] = (hashes[1] >> 1) ^ hashes[2] ^ (hashes[3] << 1);
        }
    }

    public class TriplesBySubjectAndPredicate extends FastHashMapBase<TriplesByNode>  {
        @Override
        protected TriplesByNode[] createEntryArray(int length) {
            return new TriplesByNode[length];
        }

        public boolean addWithHashCodes(TripleWithHashCodes tripleWithHashCodes) {
            return this.getOrCreate(tripleWithHashCodes.hashes[1], () -> new TriplesByNode())
                    .getOrCreate(tripleWithHashCodes.hashes[2], () -> new TripleSet())
                    .add(tripleWithHashCodes.triple);
        }

        public boolean removeWithHashCodes(TripleWithHashCodes tripleWithHashCodes) {
            final boolean[] removed = {false};
            this.removeIf(tripleWithHashCodes.hashes[1],
                    (s) -> {
                        s.removeIf(tripleWithHashCodes.hashes[2],
                                (p) -> {
                                    if (removed[0] = p.remove(tripleWithHashCodes.triple)) {
                                        return p.isEmpty();
                                    }
                                    return false;
                                });
                        return s.isEmpty();
                    });
            return removed[0];
        }
    }

    public class TriplesByPredicateAndObject extends FastHashMapBase<TriplesByNode>  {
        @Override
        protected TriplesByNode[] createEntryArray(int length) {
            return new TriplesByNode[length];
        }

        public void addWithHashCodesUnsafe(TripleWithHashCodes tripleWithHashCodes) {
            this.getOrCreate(tripleWithHashCodes.hashes[2], () -> new TriplesByNode())
                    .getOrCreate(tripleWithHashCodes.hashes[3], () -> new TripleSet())
                    .addUnsafe(tripleWithHashCodes.triple);
        }

        public void removeWithHashCodesUnsafe(TripleWithHashCodes tripleWithHashCodes) {
            this.removeIf(tripleWithHashCodes.hashes[2],
                    (p) -> {
                        p.removeIf(tripleWithHashCodes.hashes[3],
                                (o) -> {
                                    o.removeUnsafe(tripleWithHashCodes.triple);
                                    return o.isEmpty();
                                });
                        return p.isEmpty();
                    });
        }
    }

    public class TriplesByObjectAndSubject extends FastHashMapBase<TriplesByNode>  {
        @Override
        protected TriplesByNode[] createEntryArray(int length) {
            return new TriplesByNode[length];
        }

        public void addWithHashCodesUnsafe(TripleWithHashCodes tripleWithHashCodes) {
            this.getOrCreate(tripleWithHashCodes.hashes[3], () -> new TriplesByNode())
                    .getOrCreate(tripleWithHashCodes.hashes[1], () -> new TripleSet())
                    .addUnsafe(tripleWithHashCodes.triple);
        }

        public void removeWithHashCodesUnsafe(TripleWithHashCodes tripleWithHashCodes) {
            this.removeIf(tripleWithHashCodes.hashes[3],
                    (o) -> {
                        o.removeIf(tripleWithHashCodes.hashes[1],
                                (s) -> {
                                    s.removeUnsafe(tripleWithHashCodes.triple);
                                    return s.isEmpty();
                                });
                        return o.isEmpty();
                    });
        }
    }

    public class TriplesByNode extends FastHashMapBase<ListSetBase<Triple>>  {
        @Override
        protected ListSetBase<Triple>[] createEntryArray(int length) {
            return new ListSetBase[length];
        }
    }

    private static class TripleSet extends ListSetBase<Triple>  {

        public TripleSet() {
            super(2);
        }
    }


    public void add(final Triple triple) {
        var t = new TripleWithHashCodes(triple);

        if(this.sp.addWithHashCodes(t)) {
            this.po.addWithHashCodesUnsafe(t);
            this.os.addWithHashCodesUnsafe(t);
        }
    }

    public void remove(final Triple triple) {
        var t = new TripleWithHashCodes(triple);

        if(this.sp.removeWithHashCodes(t)) {
            this.po.removeWithHashCodesUnsafe(t);
            this.os.removeWithHashCodesUnsafe(t);
        }
    }


    public GraphMem2UsingHashes2() {
        super();
    }

    /**
     * Subclasses over-ride this method to release any resources they no
     * longer need once fully closed.
     */
    @Override
    protected void destroy() {
        this.sp.clear();
        this.po.clear();
        this.os.clear();
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
        var thc = new TripleWithHashCodes(t);

        if(this.sp.addWithHashCodes(thc)) {
            this.po.addWithHashCodesUnsafe(thc);
            this.os.addWithHashCodesUnsafe(thc);
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
        var thc= new TripleWithHashCodes(t);

        if(this.sp.removeWithHashCodes(thc)) {
            this.po.removeWithHashCodesUnsafe(thc);
            this.os.removeWithHashCodesUnsafe(thc);
        }
    }

    /**
     * Remove all the statements from this graph.
     */
    @Override
    public void clear() {
        super.clear(); /* deletes all triples --> could be done better but later*/
        this.sp.clear();
        this.po.clear();
        this.os.clear();
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

        final var thc = new TripleWithHashCodes(triple);

        if (sm.isConcrete()) { // SPO:S??
            if(pm.isConcrete()) { // SPO:SP?
                var withSubject = sp.getIfPresent(sm.getIndexingValue().hashCode());
                if(withSubject == null) {
                    return false;
                }
                var withPredicate = withSubject.getIfPresent(pm.getIndexingValue().hashCode());
                if(withPredicate == null) {
                    return false;
                }
                if(om.isConcrete()) {
                    return withPredicate.contains(triple);
                } else {
                    return true;
                }
            } else if (om.isConcrete()) { // SPO:S*O

            } else { // SPO:S**
                return true;
            }
        } else if(pm.isConcrete()) { // SPO:*P?

        } else if(om.isConcrete()) { // SPO:**O

        } else { // SPO:***
            return this.sp.size() > 0;
        }

        throw new UnsupportedOperationException();
    }

//    /**
//     * Determines the map with the fewest keys.
//     * This should be helpful in any case where one needs all lists of triples.
//     * Its use makes obsolete the possibly false assumption that there are always
//     * fewer predicates than subjects or objects.
//     * @return
//     */
//    private HashSetOfTripleSets getMapWithFewestKeys() {
//        var subjectCount = this.triplesBySubject.size();
//        var predicateCount = this.triplesByPredicate.size();
//        var objectCount = this.triplesByObject.size();
//        if(subjectCount < predicateCount) {
//            if(subjectCount < objectCount) {
//                return this.triplesBySubject;
//            } else {
//                return this.triplesByObject;
//            }
//        } else {
//            if(predicateCount < objectCount) {
//                return this.triplesByPredicate;
//            } else {
//                return this.triplesByObject;
//            }
//        }
//    }

    /**
     * Answer the number of triples in this graph. Default implementation counts its
     * way through the results of a findAll. Subclasses must override if they want
     * size() to be efficient.
     */
    @Override
    protected int graphBaseSize() {
        /*use the map with the fewest keys*/
        //return this.getMapWithFewestKeys().stream().mapToInt(Set::size).sum();
        throw new UnsupportedOperationException();
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
        //return this.getMapWithFewestKeys().stream().flatMap(Collection::stream);
        throw new UnsupportedOperationException();
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

//        if (sm.isConcrete()) { // SPO:S??
//            var bySubjectIndex = this.triplesBySubject
//                    .getIfPresent(sm.getIndexingValue());
//            if(bySubjectIndex == null) {
//                return Stream.empty();
//            }
//            if(om.isConcrete()) { //SPO:S?0
//                if(bySubjectIndex.size() < THRESHOLD_UNTIL_FIND_IS_MORE_EXPENSIVE_THAN_ITERATE) {
//                    if(pm.isConcrete()) { // SPO:SPO
//                        return bySubjectIndex.stream().filter(
//                                t -> pm.equals(t.getPredicate())
//                                        && om.sameValueAs(t.getObject()));
//                    } else { // SPO:S*O
//                        return bySubjectIndex.stream().filter(
//                                t -> om.sameValueAs(t.getObject()));
//                    }
//                } else {
//                    var byObjectIndex = this.triplesByObject
//                            .getIfPresent(om.getIndexingValue());
//                    if (byObjectIndex == null) {
//                        return Stream.empty();
//                    }
//                    if(bySubjectIndex.size() <= byObjectIndex.size()) {
//                        if (pm.isConcrete()) { // SPO:SPO
//                            return bySubjectIndex.stream().filter(
//                                    t -> pm.equals(t.getPredicate())
//                                            && om.sameValueAs(t.getObject()));
//                        } else { // SPO:S*O
//                            return bySubjectIndex.stream().filter(
//                                    t -> om.sameValueAs(t.getObject()));
//                        }
//                    } else {
//                        if (pm.isConcrete()) { // SPO:SPO
//                            return byObjectIndex.stream().filter(
//                                    t ->  sm.equals(t.getSubject())
//                                            && pm.equals(t.getPredicate()));
//                        } else { // SPO:S*O
//                            return byObjectIndex.stream()
//                                    .filter(t -> sm.equals(t.getSubject()));
//                        }
//                    }
//                }
//            } else if(pm.isConcrete()) { //SPO: SP*
//                return bySubjectIndex.stream()
//                        .filter(t -> pm.equals(t.getPredicate()));
//            } else { // SPO:S**
//                return bySubjectIndex.stream();
//            }
//        } else if(om.isConcrete()) { // SPO:*?O
//            var byObjectIndex = this.triplesByObject
//                    .getIfPresent(om.getIndexingValue());
//            if(byObjectIndex == null) {
//                return Stream.empty();
//            }
//            if(pm.isConcrete()) { // SPO:*PO
//                if(byObjectIndex.size() < THRESHOLD_UNTIL_FIND_IS_MORE_EXPENSIVE_THAN_ITERATE) {
//                    return byObjectIndex.stream()
//                            .filter(t -> pm.equals(t.getPredicate()));
//                } else {
//                    var byPredicateIndex = this.triplesByPredicate
//                            .getIfPresent(pm.getIndexingValue());
//                    if(byPredicateIndex == null) {
//                        return Stream.empty();
//                    }
//                    if(byObjectIndex.size() <= byPredicateIndex.size()) {
//                        return byObjectIndex.stream()
//                                .filter(t -> pm.equals(t.getPredicate()));
//                    } else {
//                        return byPredicateIndex.stream()
//                                .filter(t -> om.sameValueAs(t.getObject()));
//                    }
//                }
//            } else {    // SPO:**O
//                return byObjectIndex.stream();
//            }
//        } else if(pm.isConcrete()) { //SPO:*P*
//            var byPredicateIndex = this.triplesByPredicate
//                    .getIfPresent(pm.getIndexingValue());
//            if(byPredicateIndex == null) {
//                return Stream.empty();
//            }
//            return byPredicateIndex.stream();
//        } else { // SPO:***
//            return this.stream();
//        }
        throw new UnsupportedOperationException();
    }



    @SuppressWarnings("java:S3776")
    @Override
    public ExtendedIterator<Triple> graphBaseFind(Triple triplePattern) {
        final Node sm = triplePattern.getSubject();
        final Node pm = triplePattern.getPredicate();
        final Node om = triplePattern.getObject();

//        if (sm.isConcrete()) { // SPO:S??
//            var bySubjectIndex = this.triplesBySubject
//                    .getIfPresent(sm.getIndexingValue());
//            if(bySubjectIndex == null) {
//                return NiceIterator.emptyIterator();
//            }
//            if(om.isConcrete()) { //SPO:S?0
//                if(bySubjectIndex.size() < THRESHOLD_UNTIL_FIND_IS_MORE_EXPENSIVE_THAN_ITERATE) {
//                    if(pm.isConcrete()) { // SPO:SPO
//                        return new IteratorFiltering(bySubjectIndex.iterator(),
//                                t -> pm.equals(t.getPredicate())
//                                        && om.sameValueAs(t.getObject()),
//                                this);
//                    } else { // SPO:S*O
//                        return new IteratorFiltering(bySubjectIndex.iterator(),
//                                t -> om.sameValueAs(t.getObject()),
//                                this);
//                    }
//                } else {
//                    var byObjectIndex = this.triplesByObject
//                            .getIfPresent(om.getIndexingValue());
//                    if (byObjectIndex == null) {
//                        return NiceIterator.emptyIterator();
//                    }
//                    if(bySubjectIndex.size() <= byObjectIndex.size()) {
//                        if (pm.isConcrete()) { // SPO:SPO
//                            return new IteratorFiltering(bySubjectIndex.iterator(),
//                                    t -> pm.equals(t.getPredicate())
//                                            && om.sameValueAs(t.getObject()),
//                                    this);
//                        } else { // SPO:S*O
//                            return new IteratorFiltering(bySubjectIndex.iterator(),
//                                    t -> om.sameValueAs(t.getObject()),
//                                    this);
//                        }
//                    } else {
//                        if (pm.isConcrete()) { // SPO:SPO
//                            return new IteratorFiltering(byObjectIndex.iterator(),
//                                    t ->  sm.equals(t.getSubject())
//                                            && pm.equals(t.getPredicate()),
//                                    this);
//                        } else { // SPO:S*O
//                            return new IteratorFiltering(byObjectIndex.iterator(),
//                                    t -> sm.equals(t.getSubject()),
//                                    this);
//                        }
//                    }
//                }
//            } else if(pm.isConcrete()) { //SPO: SP*
//                return new IteratorFiltering(bySubjectIndex.iterator(),
//                        t -> pm.equals(t.getPredicate()),
//                        this);
//            } else { // SPO:S**
//                return new IteratorWrapperWithRemove(bySubjectIndex.iterator(), this);
//            }
//        } else if(om.isConcrete()) { // SPO:*?O
//            var byObjectIndex = this.triplesByObject
//                    .getIfPresent(om.getIndexingValue());
//            if(byObjectIndex == null) {
//                return NiceIterator.emptyIterator();
//            }
//            if(pm.isConcrete()) { // SPO:*PO
//                if(byObjectIndex.size() < THRESHOLD_UNTIL_FIND_IS_MORE_EXPENSIVE_THAN_ITERATE) {
//                    return new IteratorFiltering(byObjectIndex.iterator(),
//                            t -> pm.equals(t.getPredicate()),
//                            this);
//                } else {
//                    var byPredicateIndex = this.triplesByPredicate
//                            .getIfPresent(pm.getIndexingValue());
//                    if(byPredicateIndex == null) {
//                        return null;
//                    }
//                    if(byObjectIndex.size() <= byPredicateIndex.size()) {
//                        return new IteratorFiltering(byObjectIndex.iterator(),
//                                t -> pm.equals(t.getPredicate()),
//                                this);
//                    } else {
//                        return new IteratorFiltering(byPredicateIndex.iterator(),
//                                t -> om.sameValueAs(t.getObject()),
//                                this);
//                    }
//                }
//            } else {    // SPO:**O
//                return new IteratorWrapperWithRemove(byObjectIndex.iterator(), this);
//            }
//        } else if(pm.isConcrete()) { //SPO:*P*
//            var byPredicateIndex = this.triplesByPredicate
//                    .getIfPresent(pm.getIndexingValue());
//            if(byPredicateIndex == null) {
//                return NiceIterator.emptyIterator();
//            }
//            return new IteratorWrapperWithRemove(byPredicateIndex.iterator(), this);
//        } else { // SPO:***
//            /*use the map with the fewest keys*/
//            return new ListsOfTriplesIterator(this.getMapWithFewestKeys().iterator(), this);
//        }

        throw new UnsupportedOperationException();
    }

    private static class ListsOfTriplesIterator implements ExtendedIterator<Triple> {
        private static final Iterator<TripleSetWithIndexingValue> EMPTY_SET_ITERATOR = Collections.emptyIterator();
        private static final Iterator<Triple> EMPTY_TRIPLES_ITERATOR = Collections.emptyIterator();
        private final Graph graph;

        private Iterator<TripleSetWithIndexingValue> baseIterator;
        private Iterator<Triple> subIterator;
        private Triple current;

        public ListsOfTriplesIterator(Iterator<TripleSetWithIndexingValue> baseIterator, Graph graph) {
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
            var result = next();
            remove();
            return result;
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
            if(current == null) {
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
            var result = next();
            remove();
            return result;
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
     *  Wrapper for Iterator<Triple> which supports .remove and .removeNext, which deletes triples from the graph.
     *  It is done by simply replacing the wrapped iterator with .toList().iterator().
     */
    private static class IteratorWrapperWithRemove implements ExtendedIterator<Triple> {

        private Iterator<Triple> iterator;
        private final Graph graphMem;
        private boolean isStillIteratorWithNoRemove = true;

        /**
         The remembered current triple. Subclass should *not* assign to this variable.
         */
        protected Triple current;

        public IteratorWrapperWithRemove(Iterator<Triple> iteratorWithNoRemove, Graph graphMem) {
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
            var result = next();
            remove();
            return result;
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
}
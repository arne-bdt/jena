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

package org.apache.jena.mem.sorted.experiment.caching_hashes;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphWithPerform;
import org.apache.jena.mem.GraphMemBase;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.FilterIterator;
import org.apache.jena.util.iterator.Map1Iterator;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
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
public class GraphMemUsingHashMapSortedExperimentChachingHashes extends GraphMemBase implements GraphWithPerform {

    private final SubjectMap bySubject= new SubjectMap();
    private final PredicateMap byPredicate = new PredicateMap();
    private final ObjectMap byObject = new ObjectMap();

    public GraphMemUsingHashMapSortedExperimentChachingHashes() {
        super();
    }

    /**
     * Subclasses over-ride this method to release any resources they no
     * longer need once fully closed.
     */
    @Override
    protected void destroy() {
        this.bySubject.clear();
        this.byPredicate.clear();
        this.byObject.clear();
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
        var thc = bySubject.addIfNotExists(t);
        if(null != thc){
            byPredicate.addDefinitetly(thc);
            byObject.addDefinitetly(thc);
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
    public void performDelete(final Triple t) {
        var thc = bySubject.removeIfExits(t);
        if(null != thc){
            byPredicate.removeExisting(thc);
            byObject.removeExisting(thc);
        }
    }

    /**
     * Remove all the statements from this graph.
     */
    @Override
    public void clear() {
        super.clear(); /* deletes all triples --> could be done better but later*/
        this.bySubject.clear();
        this.byPredicate.clear();
        this.byObject.clear();
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
    protected boolean graphBaseContains(final Triple triple) {
        final Node sm = triple.getSubject();
        final Node pm = triple.getPredicate();
        final Node om = triple.getObject();

        if (sm.isConcrete()) { // SPO:S??
            if(pm.isConcrete()) { // SPO:SP?
                if(om.isConcrete()) { // SPO:SPO
                    return this.bySubject.contains(triple);
                } else { // SPO:SP*
                    final var triples = this.bySubject.get(sm);
                    if(triples == null) {
                        return false;
                    }
                    for(Triple t : triples) {
                        if(pm.equals(t.getPredicate())
                                && sm.equals(t.getSubject())) {
                            return true;
                        }
                    }
                }
            } else { // SPO:S*?
                final var triples = this.bySubject.get(sm);
                if(triples == null) {
                    return false;
                }
                if(om.isConcrete()) { // SPO:S*O
                    for(Triple t : triples) {
                        if(om.sameValueAs(t.getObject())
                            && sm.equals(t.getSubject())) {
                            return true;
                        }
                    }
                } else { // SPO:S**
                    for(Triple t : triples) {
                        if(sm.equals(t.getSubject())) {
                            return true;
                        }
                    }
                }
            }
        }
        else if (om.isConcrete()) {
            final var triples = this.byObject.get(om);
            if(triples == null) {
                return false;
            }
            if(pm.isConcrete()) { // SPO:*PO
                for(var t : triples) {
                    if(pm.equals(t.triple.getPredicate())
                            && om.sameValueAs(t.triple.getObject())) {
                        return true;
                    }
                }
            } else { // SPO:**O
                for(var t : triples) {
                    if(om.sameValueAs(t.triple.getObject())) {
                        return true;
                    }
                }
            }
        }
        else if (pm.isConcrete()) { // SPO:*P*
            final var triples = this.byPredicate.get(pm);
            if(triples == null) {
                return false;
            }
            for(var t : triples) {
                if(pm.equals(t.triple.getPredicate())) {
                    return true;
                }
            }
        } else { // SPO:***
            return !this.bySubject.isEmpty();
        }
        return false;
    }

    /**
     * Determines the map with the fewest keys.
     * This should be helpful in any case where one needs all lists of triples.
     * Its use makes obsolete the possibly false assumption that there are always
     * fewer predicates than subjects or objects.
     * @return
     */
    private TripleCollection getMapWithFewestKeys() {
        var subjectCount = this.bySubject.numberOfKeys();
        var predicateCount = this.byPredicate.numberOfKeys();
        var objectCount = this.byObject.numberOfKeys();
        if(subjectCount < predicateCount) {
            if(subjectCount < objectCount) {
                return this.bySubject;
            } else {
                return this.byObject;
            }
        } else {
            if(predicateCount < objectCount) {
                return this.byPredicate;
            } else {
                return this.byObject;
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
        return this.getMapWithFewestKeys().size();
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
        return this.getMapWithFewestKeys().stream();
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
    public Stream<Triple> stream(Node s, Node p, Node o) {
        final Stream<Triple> result;
        if (s != null && s.isConcrete()) {
            final var triples = this.bySubject.get(s);
            if(triples == null) {
                return Stream.empty();
            }
            if(p != null && p.isConcrete()) { // SPO:SP?
                if(o != null && o.isConcrete()) { // SPO:SPO
                    result = triples
                            .stream()
                            .filter(t -> o.sameValueAs(t.getObject())
                                    && p.equals(t.getPredicate())
                                    && s.equals(t.getSubject()));
                } else { // SPO:SP*
                    result = triples
                            .stream()
                            .filter(t -> p.equals(t.getPredicate())
                                    && s.equals(t.getSubject()));
                }
            } else { // SPO:S*?
               if(o != null && o.isConcrete()) { // SPO:S*O
                    result = triples
                            .stream()
                            .filter(t -> o.sameValueAs(t.getObject())
                                    && s.equals(t.getSubject()));
                } else { // SPO:S**
                    result = triples
                            .stream()
                            .filter(t -> s.equals(t.getSubject()));
                }
            }
        }
        else if (o != null && o.isConcrete()) { // SPO:*?O
            final var triples = this.byObject.get(o);
            if(triples == null) {
                return Stream.empty();
            }
            if(p != null && p.isConcrete()) { // SPO:*PO
                result = triples
                        .stream()
                        .filter(t -> p.equals(t.triple.getPredicate())
                                && o.sameValueAs(t.triple.getObject()))
                        .map(t -> t.triple);
            } else { // SPO:**O
                result = triples
                        .stream()
                        .filter(t -> o.sameValueAs(t.triple.getObject()))
                        .map(t -> t.triple);
            }
        }
        else if (p != null && p.isConcrete()) { // SPO:*P*
            final var triples = this.byPredicate.get(p);
            if(triples == null) {
                return Stream.empty();
            }
            result = triples
                    .stream()
                    .filter(t -> p.equals(t.triple.getPredicate()))
                    .map(t -> t.triple);
        }
        else { // SPO:***
            result = this.stream();
        }
        return result;
    }



    @SuppressWarnings("java:S3776")
    @Override
    public ExtendedIterator<Triple> graphBaseFind(Triple triplePattern) {
        final Node sm = triplePattern.getSubject();
        final Node pm = triplePattern.getPredicate();
        final Node om = triplePattern.getObject();
        final Iterator<Triple> iterator;

        if (sm.isConcrete()) { // SPO:S??
            final var triples = this.bySubject.get(sm);
            if(triples == null) {
                return NiceIterator.emptyIterator();
            }
            if(pm.isConcrete()) { // SPO:SP?
                if(om.isConcrete()) { // SPO:SPO
                    iterator = new IteratorFiltering(triples.iterator(),
                            t -> om.sameValueAs(t.getObject())
                                    && pm.equals(t.getPredicate())
                                    && sm.equals(t.getSubject()));
                } else { // SPO:SP*
                    iterator = new IteratorFiltering(triples.iterator(),
                            t -> pm.equals(t.getPredicate())
                                    && sm.equals(t.getSubject()));
                }
            } else { // SPO:S*?
               if(om.isConcrete()) { // SPO:S*O
                   iterator = new IteratorFiltering(triples.iterator(),
                            t -> om.sameValueAs(t.getObject())
                                    && sm.equals(t.getSubject()));
                } else { // SPO:S**
                   iterator = new IteratorFiltering(triples.iterator(),
                            t -> sm.equals(t.getSubject()));
                }
            }
        }
        else if (om.isConcrete()) { // SPO:*?O
            var triples = this.byObject.get(om);
            if(triples == null) {
                return NiceIterator.emptyIterator();
            }
            if(pm.isConcrete()) { // SPO:*PO
                iterator = new IteratorFilteringThc(triples.iterator(),
                        t -> pm.equals(t.getPredicate())
                                && om.sameValueAs(t.getObject()));
            } else { // SPO:**O
                iterator = new IteratorFilteringThc(triples.iterator(),
                        t -> om.sameValueAs(t.getObject()));
            }
        }
        else if (pm.isConcrete()) { // SPO:*P*
            var triples = this.byPredicate.get(pm);
            if(triples == null) {
                return NiceIterator.emptyIterator();
            }
            iterator = new IteratorFilteringThc(triples.iterator(),
                    t -> pm.equals(t.getPredicate()));
        }
        else { // SPO:***
            /*use the map with the fewest keys*/
            iterator = this.getMapWithFewestKeys().iterator();
        }
        return new IteratorWrapperWithRemove(iterator, this);
    }



    /**
     *  Wrapper for Iterator<Triple> which supports .remove and .removeNext, which deletes triples from the graph.
     *  It is done by simply replacing the wrapped iterator with .toList().iterator().
     */
    private static class IteratorWrapperWithRemove implements ExtendedIterator<Triple> {

        private Iterator<Triple> iterator;
        private final GraphMemUsingHashMapSortedExperimentChachingHashes graphMem;
        private boolean isStillIteratorWithNoRemove = true;

        /**
         The remembered current triple. Subclass should *not* assign to this variable.
         */
        protected Triple current;

        public IteratorWrapperWithRemove(Iterator<Triple> iteratorWithNoRemove, GraphMemUsingHashMapSortedExperimentChachingHashes graphMem) {
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
            Triple result = next();
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
            current = this.iterator.next();
            return current;
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
                current = currentBeforeToList;
            }
            graphMem.delete(current);
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
                var candidate = this.iterator.next();
                if(filter.test(candidate))
                {
                    this.current = candidate;
                    this.hasCurrent = true;
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

    /**
     * Basically the same as FilterIterator<> but with clear and simple implementation without inheriting possibly
     * strange behaviour from any of the base classes.
     * This Iterator also directly supports wrapWithRemoveSupport
     */
    private static class IteratorFilteringThc implements Iterator<Triple> {

        private final Predicate<Triple> filter;
        private final Iterator<TripleWithHashCodes> iterator;
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
        protected IteratorFilteringThc(Iterator<TripleWithHashCodes> iterator, Predicate<Triple> filter) {
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
                var candidate = this.iterator.next();
                if(filter.test(candidate.triple))
                {
                    this.current = candidate.triple;
                    this.hasCurrent = true;
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
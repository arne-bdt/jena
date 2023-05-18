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

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphWithPerform;
import org.apache.jena.mem.GraphMemBase;
import org.apache.jena.mem2.generic.ListSetBase;
import org.apache.jena.mem2.iterator.IteratorFiltering;
import org.apache.jena.mem2.specialized.HashSetOfTripleSets;
import org.apache.jena.mem2.specialized.LowMemoryTripleHashSetWithIndexingValueBase;
import org.apache.jena.mem2.specialized.TripleSetWithIndexingNode;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.WrappedIterator;

import java.util.Collection;
import java.util.Set;
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
 * - GraphMem2 optimizes memory usage by using Node.hashCode() as hash keys instead
 *   of the Node object itself. This is totally fine, because values are lists.
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
public class GraphMem2LowMemory extends GraphMemBase implements GraphWithPerform {

    private static final int INITIAL_SIZE_FOR_ARRAY_LISTS = 2;
    private static final int THRESHOLD_UNTIL_FIND_IS_MORE_EXPENSIVE_THAN_ITERATE = 80;


    private final HashSetOfTripleSets triplesBySubject = new HashSetOfTripleSets(); //256
    private final HashSetOfTripleSets triplesByPredicate = new HashSetOfTripleSets(); //64
    private final HashSetOfTripleSets triplesByObject = new HashSetOfTripleSets(); //512

    private static int THRESHOLD_FOR_LOW_MEMORY_HASH_SET = 60;//60-350;

    private static abstract class AbstractTriplesListSet extends ListSetBase<Triple> implements TripleSetWithIndexingNode {

        @Override
        public final boolean areOperationsWithHashCodesSupported() {
            return false;
        }

        public AbstractTriplesListSet() {
            super(INITIAL_SIZE_FOR_ARRAY_LISTS);
        }
    }

    private static class TripleListSetForSubjects extends AbstractTriplesListSet {

        @Override
        public boolean contains(Object o) {
            var triple = (Triple)o;
            for (Triple t : this) {
                if(triple.getPredicate().equals(t.getPredicate())
                        && triple.getObject().equals(t.getObject())) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Node getIndexingNode() {
            return this.get(0).getSubject();
        }
    }

    private static class TripleListSetForPredicates extends AbstractTriplesListSet {

        @Override
        public boolean contains(Object o) {
            var triple = (Triple)o;
            for (Triple t : this) {
                if(triple.getSubject().equals(t.getSubject())
                        && triple.getObject().equals(t.getObject())) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Node getIndexingNode() {
            return this.get(0).getPredicate();
        }
    }


    private static class TripleListSetForObjects extends AbstractTriplesListSet {

        @Override
        public boolean contains(Object o) {
            var triple = (Triple)o;
            for (Triple t : this) {
                if(triple.getSubject().equals(t.getSubject())
                        && triple.getPredicate().equals(t.getPredicate())) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Node getIndexingNode() {
            return this.get(0).getObject();
        }
    }

    private static class TripleHashSetForSubjects extends LowMemoryTripleHashSetWithIndexingValueBase {

        public TripleHashSetForSubjects(TripleSetWithIndexingNode setWithKey) {
            super(setWithKey);
        }

        public static int combineNodeHashes(final int hashCodeOfPredicate, final int hashCodeOfObject) {
            return (31 * hashCodeOfObject) ^ hashCodeOfPredicate;
        }

        @Override
        protected int getHashCode(final Triple value) {
            return combineNodeHashes(value.getPredicate().hashCode(), value.getObject().hashCode());
        }

        @Override
        protected Predicate<Triple> getContainsPredicate(final Triple value) {
            return t ->  value.getPredicate().equals(t.getPredicate())
                    && value.getObject().equals(t.getObject());
        }

        @Override
        public Node getIndexingNode() {
            return this.findAny().getSubject();
        }
    }


    private static class TripleHashSetForPredicates extends LowMemoryTripleHashSetWithIndexingValueBase {

        public TripleHashSetForPredicates(TripleSetWithIndexingNode setWithKey) {
            super(setWithKey);
        }

        public static int combineNodeHashes(final int hashCodeOfSubject, final int hashCodeOfObject) {
            return (127 * hashCodeOfSubject) ^ hashCodeOfObject;
        }
        @Override
        protected int getHashCode(final Triple value) {
            return combineNodeHashes(value.getSubject().hashCode(), value.getObject().hashCode());
        }

        @Override
        protected Predicate<Triple> getContainsPredicate(final Triple value) {
            return t -> value.getSubject().equals(t.getSubject())
                    && value.getObject().equals(t.getObject());
        }
        @Override
        public Node getIndexingNode() {
            return this.findAny().getPredicate();
        }
    }


    private static class TripleHashSetForObjects extends LowMemoryTripleHashSetWithIndexingValueBase {

        public TripleHashSetForObjects(TripleSetWithIndexingNode setWithKey) {
            super(setWithKey);
        }

        public static int combineNodeHashes(final int hashCodeOfSubject, final int hashCodeOfPredicate) {
            return (31 * hashCodeOfSubject) ^ hashCodeOfPredicate;
        }
        @Override
        protected int getHashCode(Triple value) {
            return combineNodeHashes(value.getSubject().hashCode(), value.getPredicate().hashCode());
        }

        @Override
        protected Predicate<Triple> getContainsPredicate(final Triple value) {
            return t -> value.getSubject().equals(t.getSubject())
                    && value.getPredicate().equals(t.getPredicate());
        }
        @Override
        public Node getIndexingNode() {
            return this.findAny().getObject();
        }
    }

    public GraphMem2LowMemory() {
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
        var hashCodeOfSubject = t.getSubject().hashCode();
        var hashCodeOfPredicate = t.getPredicate().hashCode();
        var hashCodeOfObject = t.getObject().hashCode();
        subject:
        {
            final boolean[] added = {false};
            this.triplesBySubject.compute(
                    t.getSubject(),
                    hashCodeOfSubject,
                    ts -> {
                        if(ts == null) {
                            ts = new TripleListSetForSubjects();
                            ts.addUnchecked(t);
                            added[0] = true;
                        } else if(ts.areOperationsWithHashCodesSupported()) {
                            added[0] = ts.add(t, TripleHashSetForSubjects.combineNodeHashes(hashCodeOfPredicate, hashCodeOfObject));
                        } else if (ts.size() == THRESHOLD_FOR_LOW_MEMORY_HASH_SET){
                            ts = new TripleHashSetForSubjects(ts);
                            added[0] = ts.add(t, TripleHashSetForSubjects.combineNodeHashes(hashCodeOfPredicate, hashCodeOfObject));
                        } else {
                            added[0] = ts.add(t);
                        }
                        return ts;
                    });
            if(!added[0]) {
                return;
            }
        }
        predicate:
        {
            this.triplesByPredicate.compute(
                    t.getPredicate(),
                    hashCodeOfPredicate,
                    ts -> {
                        if(ts == null) {
                            ts = new TripleListSetForPredicates();
                            ts.addUnchecked(t);
                        } else if (ts.areOperationsWithHashCodesSupported()) {
                            ts.addUnchecked(t, TripleHashSetForPredicates.combineNodeHashes(hashCodeOfSubject, hashCodeOfObject));
                        } else if(ts.size() == THRESHOLD_FOR_LOW_MEMORY_HASH_SET) {
                            ts = new TripleHashSetForPredicates(ts);
                            ts.addUnchecked(t, TripleHashSetForPredicates.combineNodeHashes(hashCodeOfSubject, hashCodeOfObject));
                        } else {
                            ts.addUnchecked(t);
                        }
                        return ts;
                    });
        }
        object:
        {
            this.triplesByObject.compute(
                    t.getObject(),
                    hashCodeOfObject,
                    ts -> {
                        if(ts == null) {
                            ts = new TripleListSetForObjects();
                            ts.addUnchecked(t);
                        } else if (ts.areOperationsWithHashCodesSupported()) {
                            ts.addUnchecked(t, TripleHashSetForObjects.combineNodeHashes(hashCodeOfSubject, hashCodeOfPredicate));
                        } else if(ts.size() == THRESHOLD_FOR_LOW_MEMORY_HASH_SET) {
                            ts = new TripleHashSetForObjects(ts);
                            ts.addUnchecked(t, TripleHashSetForObjects.combineNodeHashes(hashCodeOfSubject, hashCodeOfPredicate));
                        } else {
                            ts.addUnchecked(t);
                        }
                        return ts;
                    });
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
        var hashCodeOfSubject = t.getSubject().hashCode();
        var hashCodeOfPredicate = t.getPredicate().hashCode();
        var hashCodeOfObject = t.getObject().hashCode();
        subject:
        {
            final boolean[] removed = {false};
            this.triplesBySubject.compute(
                    t.getSubject(),
                    hashCodeOfSubject,
                    ts -> {
                        if(ts == null) {
                            return null;
                        } else if(ts.areOperationsWithHashCodesSupported()
                                ? ts.remove(t, TripleHashSetForSubjects.combineNodeHashes(hashCodeOfPredicate, hashCodeOfObject))
                                : ts.remove(t)) {
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
                    t.getPredicate(),
                    hashCodeOfPredicate,
                    ts -> {
                        if(ts.areOperationsWithHashCodesSupported()) {
                            ts.removeUnchecked(t, TripleHashSetForPredicates.combineNodeHashes(hashCodeOfSubject, hashCodeOfObject));
                        } else {
                            ts.removeUnchecked(t);
                        }
                        return ts.isEmpty() ? null : ts;
                    });
        }
        object:
        {
            this.triplesByObject.compute(
                    t.getObject(),
                    hashCodeOfObject,
                    ts -> {
                        if(ts.areOperationsWithHashCodesSupported()) {
                            ts.removeUnchecked(t, TripleHashSetForObjects.combineNodeHashes(hashCodeOfSubject, hashCodeOfPredicate));
                        } else {
                            ts.removeUnchecked(t);
                        }
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

        if (sm.isConcrete()) { // SPO:S??
            var bySubjectIndex = this.triplesBySubject
                    .getIfPresent(sm);
            if(bySubjectIndex == null) {
                return false;
            }
            if(om.isConcrete()) { //SPO:S?0
                if(bySubjectIndex.size() < THRESHOLD_UNTIL_FIND_IS_MORE_EXPENSIVE_THAN_ITERATE) {
                    if(pm.isConcrete()) { // SPO:SPO
                        return bySubjectIndex.contains(triple);
                    } else { // SPO:S*O
                        return bySubjectIndex.stream().anyMatch(
                                t -> om.equals(t.getObject()));
                    }
                } else {
                    var byObjectIndex = this.triplesByObject
                            .getIfPresent(om);
                    if (byObjectIndex == null) {
                        return false;
                    }
                    if(bySubjectIndex.size() <= byObjectIndex.size()) {
                        if (pm.isConcrete()) { // SPO:SPO
                            return bySubjectIndex.contains(triple);
                        } else { // SPO:S*O
                            return bySubjectIndex.stream().anyMatch(
                                    t -> om.equals(t.getObject()));
                        }
                    } else {
                        if (pm.isConcrete()) { // SPO:SPO
                            return byObjectIndex.contains(triple);
                        } else { // SPO:S*O
                            return byObjectIndex.stream().anyMatch(
                                    t -> sm.equals(t.getSubject()));
                        }
                    }
                }
            } else if(pm.isConcrete()) { //SPO: SP*
                return bySubjectIndex.stream().anyMatch(
                        t -> pm.equals(t.getPredicate()));
            } else { // SPO:S**
                return true;
            }
        } else if(om.isConcrete()) { // SPO:*?O
            var byObjectIndex = this.triplesByObject
                    .getIfPresent(om);
            if(byObjectIndex == null) {
                return false;
            }
            if(pm.isConcrete()) { // SPO:*PO
                if(byObjectIndex.size() < THRESHOLD_UNTIL_FIND_IS_MORE_EXPENSIVE_THAN_ITERATE) {
                    return byObjectIndex.stream().anyMatch(
                            t -> pm.equals(t.getPredicate()));
                } else {
                    var byPredicateIndex = this.triplesByPredicate
                            .getIfPresent(pm);
                    if(byPredicateIndex == null) {
                        return false;
                    }
                    if(byObjectIndex.size() <= byPredicateIndex.size()) {
                        return byObjectIndex.stream().anyMatch(
                                t -> pm.equals(t.getPredicate()));
                    } else {
                        return byPredicateIndex.stream().anyMatch(
                                t -> om.equals(t.getObject()));
                    }
                }
            } else {    // SPO:**O
                return true;
            }
        } else if(pm.isConcrete()) { //SPO:*P*
            var byPredicateIndex = this.triplesByPredicate
                    .getIfPresent(pm);
            if(byPredicateIndex == null) {
                return false;
            }
            return true;
        } else { // SPO:***
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
    private HashSetOfTripleSets getMapWithFewestKeys() {
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

        if (sm.isConcrete()) { // SPO:S??
            var bySubjectIndex = this.triplesBySubject
                    .getIfPresent(sm);
            if(bySubjectIndex == null) {
                return Stream.empty();
            }
            if(om.isConcrete()) { //SPO:S?0
                if(bySubjectIndex.size() < THRESHOLD_UNTIL_FIND_IS_MORE_EXPENSIVE_THAN_ITERATE) {
                    if(pm.isConcrete()) { // SPO:SPO
                        return bySubjectIndex.stream().filter(
                                t -> pm.equals(t.getPredicate())
                                        && om.equals(t.getObject()));
                    } else { // SPO:S*O
                        return bySubjectIndex.stream().filter(
                                t -> om.equals(t.getObject()));
                    }
                } else {
                    var byObjectIndex = this.triplesByObject
                            .getIfPresent(om);
                    if (byObjectIndex == null) {
                        return Stream.empty();
                    }
                    if(bySubjectIndex.size() <= byObjectIndex.size()) {
                        if (pm.isConcrete()) { // SPO:SPO
                            return bySubjectIndex.stream().filter(
                                    t -> pm.equals(t.getPredicate())
                                            && om.equals(t.getObject()));
                        } else { // SPO:S*O
                            return bySubjectIndex.stream().filter(
                                    t -> om.equals(t.getObject()));
                        }
                    } else {
                        if (pm.isConcrete()) { // SPO:SPO
                            return byObjectIndex.stream().filter(
                                    t ->  sm.equals(t.getSubject())
                                            && pm.equals(t.getPredicate()));
                        } else { // SPO:S*O
                            return byObjectIndex.stream()
                                    .filter(t -> sm.equals(t.getSubject()));
                        }
                    }
                }
            } else if(pm.isConcrete()) { //SPO: SP*
                return bySubjectIndex.stream()
                        .filter(t -> pm.equals(t.getPredicate()));
            } else { // SPO:S**
                return bySubjectIndex.stream();
            }
        } else if(om.isConcrete()) { // SPO:*?O
            var byObjectIndex = this.triplesByObject
                    .getIfPresent(om);
            if(byObjectIndex == null) {
                return Stream.empty();
            }
            if(pm.isConcrete()) { // SPO:*PO
                if(byObjectIndex.size() < THRESHOLD_UNTIL_FIND_IS_MORE_EXPENSIVE_THAN_ITERATE) {
                    return byObjectIndex.stream()
                            .filter(t -> pm.equals(t.getPredicate()));
                } else {
                    var byPredicateIndex = this.triplesByPredicate
                            .getIfPresent(pm);
                    if(byPredicateIndex == null) {
                        return Stream.empty();
                    }
                    if(byObjectIndex.size() <= byPredicateIndex.size()) {
                        return byObjectIndex.stream()
                                .filter(t -> pm.equals(t.getPredicate()));
                    } else {
                        return byPredicateIndex.stream()
                                .filter(t -> om.equals(t.getObject()));
                    }
                }
            } else {    // SPO:**O
                return byObjectIndex.stream();
            }
        } else if(pm.isConcrete()) { //SPO:*P*
            var byPredicateIndex = this.triplesByPredicate
                    .getIfPresent(pm);
            if(byPredicateIndex == null) {
                return Stream.empty();
            }
            return byPredicateIndex.stream();
        } else { // SPO:***
            return this.stream();
        }
    }



    @SuppressWarnings("java:S3776")
    @Override
    public ExtendedIterator<Triple> graphBaseFind(Triple triplePattern) {
        final Node sm = triplePattern.getSubject();
        final Node pm = triplePattern.getPredicate();
        final Node om = triplePattern.getObject();

        if (sm.isConcrete()) { // SPO:S??
            var bySubjectIndex = this.triplesBySubject
                    .getIfPresent(sm);
            if(bySubjectIndex == null) {
                return NiceIterator.emptyIterator();
            }
            if(om.isConcrete()) { //SPO:S?0
                if(bySubjectIndex.size() < THRESHOLD_UNTIL_FIND_IS_MORE_EXPENSIVE_THAN_ITERATE) {
                    if(pm.isConcrete()) { // SPO:SPO
                        return new IteratorFiltering(bySubjectIndex.iterator(),
                                t -> pm.equals(t.getPredicate())
                                        && om.equals(t.getObject()));
                    } else { // SPO:S*O
                        return new IteratorFiltering(bySubjectIndex.iterator(),
                                t -> om.equals(t.getObject()));
                    }
                } else {
                    var byObjectIndex = this.triplesByObject
                            .getIfPresent(om);
                    if (byObjectIndex == null) {
                        return NiceIterator.emptyIterator();
                    }
                    if(bySubjectIndex.size() <= byObjectIndex.size()) {
                        if (pm.isConcrete()) { // SPO:SPO
                            return new IteratorFiltering(bySubjectIndex.iterator(),
                                    t -> pm.equals(t.getPredicate())
                                            && om.equals(t.getObject()));
                        } else { // SPO:S*O
                            return new IteratorFiltering(bySubjectIndex.iterator(),
                                    t -> om.equals(t.getObject()));
                        }
                    } else {
                        if (pm.isConcrete()) { // SPO:SPO
                            return new IteratorFiltering(byObjectIndex.iterator(),
                                    t ->  sm.equals(t.getSubject())
                                            && pm.equals(t.getPredicate()));
                        } else { // SPO:S*O
                            return new IteratorFiltering(byObjectIndex.iterator(),
                                    t -> sm.equals(t.getSubject()));
                        }
                    }
                }
            } else if(pm.isConcrete()) { //SPO: SP*
                return new IteratorFiltering(bySubjectIndex.iterator(),
                        t -> pm.equals(t.getPredicate()));
            } else { // SPO:S**
                return WrappedIterator.createNoRemove(bySubjectIndex.iterator());
            }
        } else if(om.isConcrete()) { // SPO:*?O
            var byObjectIndex = this.triplesByObject
                    .getIfPresent(om);
            if(byObjectIndex == null) {
                return NiceIterator.emptyIterator();
            }
            if(pm.isConcrete()) { // SPO:*PO
                if(byObjectIndex.size() < THRESHOLD_UNTIL_FIND_IS_MORE_EXPENSIVE_THAN_ITERATE) {
                    return new IteratorFiltering(byObjectIndex.iterator(),
                            t -> pm.equals(t.getPredicate()));
                } else {
                    var byPredicateIndex = this.triplesByPredicate
                            .getIfPresent(pm);
                    if(byPredicateIndex == null) {
                        return null;
                    }
                    if(byObjectIndex.size() <= byPredicateIndex.size()) {
                        return new IteratorFiltering(byObjectIndex.iterator(),
                                t -> pm.equals(t.getPredicate()));
                    } else {
                        return new IteratorFiltering(byPredicateIndex.iterator(),
                                t -> om.equals(t.getObject()));
                    }
                }
            } else {    // SPO:**O
                return WrappedIterator.createNoRemove(byObjectIndex.iterator());
            }
        } else if(pm.isConcrete()) { //SPO:*P*
            var byPredicateIndex = this.triplesByPredicate
                    .getIfPresent(pm);
            if(byPredicateIndex == null) {
                return NiceIterator.emptyIterator();
            }
            return WrappedIterator.createNoRemove(byPredicateIndex.iterator());
        } else { // SPO:***
            /*use the map with the fewest keys*/
            return new ListsOfTriplesIterator(this.getMapWithFewestKeys().iterator());
        }
    }

}
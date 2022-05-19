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
import org.apache.jena.mem2.generic.LowMemoryHashSet;
import org.apache.jena.mem2.generic.SortedListSetBase;
import org.apache.jena.mem2.helper.TripleEqualsOrMatches;
import org.apache.jena.mem2.specialized.TripleHashSet;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.FilterIterator;
import org.apache.jena.util.iterator.Map1Iterator;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
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

    private final LowMemoryHashSet<TripleSetWithKey> triplesBySubject = new LowMemoryHashSet<>();
    private final LowMemoryHashSet<TripleSetWithKey> triplesByPredicate = new LowMemoryHashSet<>();
    private final LowMemoryHashSet<TripleSetWithKey> triplesByObject = new LowMemoryHashSet<>();

    private static int THRESHOLD_FOR_LOW_MEMORY_HASH_SET = 70;//350;

//    private static Comparator<Triple> TRIPLE_INDEXING_VALUE_HASH_CODE_COMPARATOR_FOR_TRIPLES =
//            Comparator.comparingInt(t -> t.hashCode());

    private static Comparator<Triple> TRIPLE_INDEXING_VALUE_HASH_CODE_COMPARATOR_FOR_TRIPLES_BY_SUBJECT =
            Comparator.comparingInt((Triple t) -> t.getObject().getIndexingValue().hashCode())
                    .thenComparing(t -> t.getPredicate().getIndexingValue().hashCode());

    private static Comparator<Triple> TRIPLE_INDEXING_VALUE_HASH_CODE_COMPARATOR_FOR_TRIPLES_BY_PREDICATE =
            Comparator.comparingInt((Triple t) -> t.getSubject().getIndexingValue().hashCode())
                    .thenComparing(t -> t.getObject().getIndexingValue().hashCode());

    private static Comparator<Triple> TRIPLE_INDEXING_VALUE_HASH_CODE_COMPARATOR_FOR_TRIPLES_BY_OBJECT =
            Comparator.comparingInt((Triple t) -> t.getSubject().getIndexingValue().hashCode())
                    .thenComparing(t -> t.getPredicate().getIndexingValue().hashCode());

    private interface TripleSetWithKey extends Set<Triple> {

        int getKeyOfSet();

        boolean equals(Object o);

        int hashCode();

        default void addUnsafe(Triple t) {
            this.add(t);
        }

        default void removeUnsafe(Triple t) {
            this.removeUnsafe(t);
        }
    }

    private static class LookupObjectForTripleSetWithKey implements TripleSetWithKey {

        private final int keyOfSet;

        private LookupObjectForTripleSetWithKey(int keyOfSet) {
            this.keyOfSet = keyOfSet;
        }

        @Override
        public int getKeyOfSet() {
            return keyOfSet;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || (!(o instanceof TripleSetWithKey))) return false;

            TripleSetWithKey that = (TripleSetWithKey) o;

            return this.getKeyOfSet() == that.getKeyOfSet();
        }

        @Override
        public int hashCode() {
            return this.getKeyOfSet();
        }

        /**
         * Returns the number of elements in this set (its cardinality).  If this
         * set contains more than {@code Integer.MAX_VALUE} elements, returns
         * {@code Integer.MAX_VALUE}.
         *
         * @return the number of elements in this set (its cardinality)
         */
        @Override
        public int size() {
            return 0;
        }

        /**
         * Returns {@code true} if this set contains no elements.
         *
         * @return {@code true} if this set contains no elements
         */
        @Override
        public boolean isEmpty() {
            return false;
        }

        /**
         * Returns {@code true} if this set contains the specified element.
         * More formally, returns {@code true} if and only if this set
         * contains an element {@code e} such that
         * {@code Objects.equals(o, e)}.
         *
         * @param o element whose presence in this set is to be tested
         * @return {@code true} if this set contains the specified element
         * @throws ClassCastException   if the type of the specified element
         *                              is incompatible with this set
         *                              (<a href="Collection.html#optional-restrictions">optional</a>)
         * @throws NullPointerException if the specified element is null and this
         *                              set does not permit null elements
         *                              (<a href="Collection.html#optional-restrictions">optional</a>)
         */
        @Override
        public boolean contains(Object o) {
            return false;
        }

        /**
         * Returns an iterator over the elements in this set.  The elements are
         * returned in no particular order (unless this set is an instance of some
         * class that provides a guarantee).
         *
         * @return an iterator over the elements in this set
         */
        @Override
        public Iterator<Triple> iterator() {
            return null;
        }

        /**
         * Returns an array containing all of the elements in this set.
         * If this set makes any guarantees as to what order its elements
         * are returned by its iterator, this method must return the
         * elements in the same order.
         *
         * <p>The returned array will be "safe" in that no references to it
         * are maintained by this set.  (In other words, this method must
         * allocate a new array even if this set is backed by an array).
         * The caller is thus free to modify the returned array.
         *
         * <p>This method acts as bridge between array-based and collection-based
         * APIs.
         *
         * @return an array containing all the elements in this set
         */
        @Override
        public Object[] toArray() {
            return new Object[0];
        }

        /**
         * Returns an array containing all of the elements in this set; the
         * runtime type of the returned array is that of the specified array.
         * If the set fits in the specified array, it is returned therein.
         * Otherwise, a new array is allocated with the runtime type of the
         * specified array and the size of this set.
         *
         * <p>If this set fits in the specified array with room to spare
         * (i.e., the array has more elements than this set), the element in
         * the array immediately following the end of the set is set to
         * {@code null}.  (This is useful in determining the length of this
         * set <i>only</i> if the caller knows that this set does not contain
         * any null elements.)
         *
         * <p>If this set makes any guarantees as to what order its elements
         * are returned by its iterator, this method must return the elements
         * in the same order.
         *
         * <p>Like the {@link #toArray()} method, this method acts as bridge between
         * array-based and collection-based APIs.  Further, this method allows
         * precise control over the runtime type of the output array, and may,
         * under certain circumstances, be used to save allocation costs.
         *
         * <p>Suppose {@code x} is a set known to contain only strings.
         * The following code can be used to dump the set into a newly allocated
         * array of {@code String}:
         *
         * <pre>
         *     String[] y = x.toArray(new String[0]);</pre>
         * <p>
         * Note that {@code toArray(new Object[0])} is identical in function to
         * {@code toArray()}.
         *
         * @param a the array into which the elements of this set are to be
         *          stored, if it is big enough; otherwise, a new array of the same
         *          runtime type is allocated for this purpose.
         * @return an array containing all the elements in this set
         * @throws ArrayStoreException  if the runtime type of the specified array
         *                              is not a supertype of the runtime type of every element in this
         *                              set
         * @throws NullPointerException if the specified array is null
         */
        @Override
        public <T> T[] toArray(T[] a) {
            return null;
        }

        /**
         * Adds the specified element to this set if it is not already present
         * (optional operation).  More formally, adds the specified element
         * {@code e} to this set if the set contains no element {@code e2}
         * such that
         * {@code Objects.equals(e, e2)}.
         * If this set already contains the element, the call leaves the set
         * unchanged and returns {@code false}.  In combination with the
         * restriction on constructors, this ensures that sets never contain
         * duplicate elements.
         *
         * <p>The stipulation above does not imply that sets must accept all
         * elements; sets may refuse to add any particular element, including
         * {@code null}, and throw an exception, as described in the
         * specification for {@link Collection#add Collection.add}.
         * Individual set implementations should clearly document any
         * restrictions on the elements that they may contain.
         *
         * @param triple element to be added to this set
         * @return {@code true} if this set did not already contain the specified
         * element
         * @throws UnsupportedOperationException if the {@code add} operation
         *                                       is not supported by this set
         * @throws ClassCastException            if the class of the specified element
         *                                       prevents it from being added to this set
         * @throws NullPointerException          if the specified element is null and this
         *                                       set does not permit null elements
         * @throws IllegalArgumentException      if some property of the specified element
         *                                       prevents it from being added to this set
         */
        @Override
        public boolean add(Triple triple) {
            return false;
        }

        /**
         * Removes the specified element from this set if it is present
         * (optional operation).  More formally, removes an element {@code e}
         * such that
         * {@code Objects.equals(o, e)}, if
         * this set contains such an element.  Returns {@code true} if this set
         * contained the element (or equivalently, if this set changed as a
         * result of the call).  (This set will not contain the element once the
         * call returns.)
         *
         * @param o object to be removed from this set, if present
         * @return {@code true} if this set contained the specified element
         * @throws ClassCastException            if the type of the specified element
         *                                       is incompatible with this set
         *                                       (<a href="Collection.html#optional-restrictions">optional</a>)
         * @throws NullPointerException          if the specified element is null and this
         *                                       set does not permit null elements
         *                                       (<a href="Collection.html#optional-restrictions">optional</a>)
         * @throws UnsupportedOperationException if the {@code remove} operation
         *                                       is not supported by this set
         */
        @Override
        public boolean remove(Object o) {
            return false;
        }

        /**
         * Returns {@code true} if this set contains all of the elements of the
         * specified collection.  If the specified collection is also a set, this
         * method returns {@code true} if it is a <i>subset</i> of this set.
         *
         * @param c collection to be checked for containment in this set
         * @return {@code true} if this set contains all of the elements of the
         * specified collection
         * @throws ClassCastException   if the types of one or more elements
         *                              in the specified collection are incompatible with this
         *                              set
         *                              (<a href="Collection.html#optional-restrictions">optional</a>)
         * @throws NullPointerException if the specified collection contains one
         *                              or more null elements and this set does not permit null
         *                              elements
         *                              (<a href="Collection.html#optional-restrictions">optional</a>),
         *                              or if the specified collection is null
         * @see #contains(Object)
         */
        @Override
        public boolean containsAll(Collection<?> c) {
            return false;
        }

        /**
         * Adds all of the elements in the specified collection to this set if
         * they're not already present (optional operation).  If the specified
         * collection is also a set, the {@code addAll} operation effectively
         * modifies this set so that its value is the <i>union</i> of the two
         * sets.  The behavior of this operation is undefined if the specified
         * collection is modified while the operation is in progress.
         *
         * @param c collection containing elements to be added to this set
         * @return {@code true} if this set changed as a result of the call
         * @throws UnsupportedOperationException if the {@code addAll} operation
         *                                       is not supported by this set
         * @throws ClassCastException            if the class of an element of the
         *                                       specified collection prevents it from being added to this set
         * @throws NullPointerException          if the specified collection contains one
         *                                       or more null elements and this set does not permit null
         *                                       elements, or if the specified collection is null
         * @throws IllegalArgumentException      if some property of an element of the
         *                                       specified collection prevents it from being added to this set
         * @see #add(Object)
         */
        @Override
        public boolean addAll(Collection<? extends Triple> c) {
            return false;
        }

        /**
         * Retains only the elements in this set that are contained in the
         * specified collection (optional operation).  In other words, removes
         * from this set all of its elements that are not contained in the
         * specified collection.  If the specified collection is also a set, this
         * operation effectively modifies this set so that its value is the
         * <i>intersection</i> of the two sets.
         *
         * @param c collection containing elements to be retained in this set
         * @return {@code true} if this set changed as a result of the call
         * @throws UnsupportedOperationException if the {@code retainAll} operation
         *                                       is not supported by this set
         * @throws ClassCastException            if the class of an element of this set
         *                                       is incompatible with the specified collection
         *                                       (<a href="Collection.html#optional-restrictions">optional</a>)
         * @throws NullPointerException          if this set contains a null element and the
         *                                       specified collection does not permit null elements
         *                                       (<a href="Collection.html#optional-restrictions">optional</a>),
         *                                       or if the specified collection is null
         * @see #remove(Object)
         */
        @Override
        public boolean retainAll(Collection<?> c) {
            return false;
        }

        /**
         * Removes from this set all of its elements that are contained in the
         * specified collection (optional operation).  If the specified
         * collection is also a set, this operation effectively modifies this
         * set so that its value is the <i>asymmetric set difference</i> of
         * the two sets.
         *
         * @param c collection containing elements to be removed from this set
         * @return {@code true} if this set changed as a result of the call
         * @throws UnsupportedOperationException if the {@code removeAll} operation
         *                                       is not supported by this set
         * @throws ClassCastException            if the class of an element of this set
         *                                       is incompatible with the specified collection
         *                                       (<a href="Collection.html#optional-restrictions">optional</a>)
         * @throws NullPointerException          if this set contains a null element and the
         *                                       specified collection does not permit null elements
         *                                       (<a href="Collection.html#optional-restrictions">optional</a>),
         *                                       or if the specified collection is null
         * @see #remove(Object)
         * @see #contains(Object)
         */
        @Override
        public boolean removeAll(Collection<?> c) {
            return false;
        }

        /**
         * Removes all of the elements from this set (optional operation).
         * The set will be empty after this call returns.
         *
         * @throws UnsupportedOperationException if the {@code clear} method
         *                                       is not supported by this set
         */
        @Override
        public void clear() {

        }
    }

    private static abstract class AbstractSortedTriplesSet extends SortedListSetBase<Triple> implements TripleSetWithKey {

        private final int keyOfSet;

        public AbstractSortedTriplesSet(final int initialCapacity, final int keyOfSet) {
            super(initialCapacity);
            this.keyOfSet = keyOfSet;
        }

        @Override
        public int getKeyOfSet() {
            return this.keyOfSet;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || (!(o instanceof TripleSetWithKey))) return false;

            TripleSetWithKey that = (TripleSetWithKey) o;

            return this.getKeyOfSet() == that.getKeyOfSet();
        }

        @Override
        public int hashCode() {
            return this.getKeyOfSet();
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || (!(o instanceof TripleSetWithKey))) return false;

            TripleSetWithKey that = (TripleSetWithKey) o;

            return this.getKeyOfSet() == that.getKeyOfSet();
        }

        @Override
        public int hashCode() {
            return this.getKeyOfSet();
        }
    }

    private static Function<Integer, TripleSetWithKey> createSortedListSetForTriplesBySubject
            = keyOfSet -> new AbstractSortedTriplesSet(INITIAL_SIZE_FOR_ARRAY_LISTS, keyOfSet)  {
        @Override
        protected Comparator<Triple> getComparator() {
            return TRIPLE_INDEXING_VALUE_HASH_CODE_COMPARATOR_FOR_TRIPLES_BY_SUBJECT;
        }

        @Override
        protected int getSizeToStartSorting() { return 15; }
    };

    private static Function<Integer, TripleSetWithKey> createSortedListSetForTriplesByPredicate
            = keyOfSet -> new AbstractSortedTriplesSet(INITIAL_SIZE_FOR_ARRAY_LISTS, keyOfSet) {
        @Override
        protected Comparator<Triple> getComparator() {
            return TRIPLE_INDEXING_VALUE_HASH_CODE_COMPARATOR_FOR_TRIPLES_BY_PREDICATE;
        }

        @Override
        protected int getSizeToStartSorting() {
            return 15;
        }
    };

    private static Function<Integer, TripleSetWithKey> createSortedListSetForTriplesByObject
            = keyOfSet -> new AbstractSortedTriplesSet(INITIAL_SIZE_FOR_ARRAY_LISTS, keyOfSet) {

        @Override
        protected Comparator<Triple> getComparator() {
            return TRIPLE_INDEXING_VALUE_HASH_CODE_COMPARATOR_FOR_TRIPLES_BY_OBJECT;
        }

        @Override
        protected int getSizeToStartSorting() {
            return 15;
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
            return (value.getSubject().getIndexingValue().hashCode() >> 1)
                    ^ value.getObject().getIndexingValue().hashCode();
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
            return (value.getSubject().getIndexingValue().hashCode() >> 1)
                    ^ value.getPredicate().getIndexingValue().hashCode();
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

    private Set<Triple> getSmallestSet(final Set<Triple> a, final Set<Triple> b, final Set<Triple> c) {
        if(a.size() < b.size()) {
            return c.size() < a.size() ? c : a;
        }
        return b.size() < c.size() ? b : c;
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
                    new LookupObjectForTripleSetWithKey(sKey),
                    ts -> {
                        if(ts == null) {
                            ts = createSortedListSetForTriplesBySubject.apply(sKey);
                        } else {
                            if(ts.size() == THRESHOLD_FOR_LOW_MEMORY_HASH_SET) {
                                ts = createLowMemoryHashSetForTriplesBySubject.apply(ts);
                            }
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
                new LookupObjectForTripleSetWithKey(pKey),
                    ts -> {
                        if(ts == null) {
                            ts = createSortedListSetForTriplesByPredicate.apply(pKey);
                        } else {
                            if(ts.size() == THRESHOLD_FOR_LOW_MEMORY_HASH_SET) {
                                ts = createLowMemoryHashSetForTriplesByPredicate.apply(ts);
                            }
                        }
                        return ts;
                    });
            withSamePredicateKey.addUnsafe(t);
        }
        object:
        {
            var oKey = t.getObject().getIndexingValue().hashCode();
            var withSameObjectKey = this.triplesByObject.compute(
                    new LookupObjectForTripleSetWithKey(oKey),
                    ts -> {
                        if(ts == null) {
                            ts = createSortedListSetForTriplesByObject.apply(oKey);
                        } else {
                            if(ts.size() == THRESHOLD_FOR_LOW_MEMORY_HASH_SET) {
                                ts = createLowMemoryHashSetForTriplesByObject.apply(ts);
                            }
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
                    new LookupObjectForTripleSetWithKey(sKey),
                    ts -> {
                        if(ts == null) {
                            return null;
                        } else {
                            if(ts.remove(t)) {
                                removed[0] = true;
                                if(ts.isEmpty()) {
                                    return null; /*thereby remove key*/
                                }
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
                    new LookupObjectForTripleSetWithKey(pKey),
                    ts -> {
                        ts.removeUnsafe(t);
                        return ts.isEmpty() ? null : ts;
                    });
        }
        object:
        {
            var oKey = t.getObject().getIndexingValue().hashCode();
            this.triplesByObject.compute(
                    new LookupObjectForTripleSetWithKey(oKey),
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
                    .getIfPresent(new LookupObjectForTripleSetWithKey(sm.getIndexingValue().hashCode()));
            if(bySubjectIndex == null) {
                return null;
            }
            if(pm.isConcrete()) { // SPO:SP?
                var byPredicateIndex = this.triplesByPredicate
                        .getIfPresent(new LookupObjectForTripleSetWithKey(pm.getIndexingValue().hashCode()));
                if(byPredicateIndex == null) {
                    return null;
                }
                if(om.isConcrete()) { // SPO:SPO
                    var byObjectIndex = this.triplesByObject
                            .getIfPresent(new LookupObjectForTripleSetWithKey(om.getIndexingValue().hashCode()));
                    if(byObjectIndex == null) {
                        return null;
                    }
                    var smallestSet = getSmallestSet(
                            bySubjectIndex, byPredicateIndex, byObjectIndex);
                    if(TripleEqualsOrMatches.isEqualsForObjectOk(om)) {
                        if(smallestSet == bySubjectIndex) {
                            return Pair.of(smallestSet,
                                    t -> om.equals(t.getObject())
                                            && pm.equals(t.getPredicate())
                                            && sm.equals(t.getSubject()));
                        }
                        if(smallestSet == byPredicateIndex) {
                            return Pair.of(smallestSet,
                                    t -> sm.equals(t.getSubject())
                                            && om.equals(t.getObject())
                                            && pm.equals(t.getPredicate()));
                        }
                        return Pair.of(smallestSet,
                                t -> sm.equals(t.getSubject())
                                        && pm.equals(t.getPredicate())
                                        && om.equals(t.getObject()));
                    } else {
                        if(smallestSet == bySubjectIndex) {
                            return Pair.of(smallestSet,
                                    t -> om.sameValueAs(t.getObject())
                                            && pm.equals(t.getPredicate())
                                            && sm.equals(t.getSubject()));
                        }
                        if(smallestSet == byPredicateIndex) {
                            return Pair.of(smallestSet,
                                    t -> sm.equals(t.getSubject())
                                            && om.sameValueAs(t.getObject())
                                            && pm.equals(t.getPredicate()));
                        }
                        return Pair.of(smallestSet,
                                t -> sm.equals(t.getSubject())
                                        && pm.equals(t.getPredicate())
                                        && om.sameValueAs(t.getObject()));
                    }
                } else { // SPO:SP*
                    if(bySubjectIndex.size() <= byPredicateIndex.size()) {
                        return Pair.of(bySubjectIndex,
                                t -> pm.equals(t.getPredicate())
                                        && sm.equals(t.getSubject()));
                    }
                    return Pair.of(byPredicateIndex,
                            t -> sm.equals(t.getSubject())
                                    && pm.equals(t.getPredicate()));
                }
            } else { // SPO:S*?
                if(om.isConcrete()) { // SPO:S*O
                    var byObjectIndex = this.triplesByObject
                            .getIfPresent(new LookupObjectForTripleSetWithKey(om.getIndexingValue().hashCode()));
                    if(byObjectIndex == null) {
                        return null;
                    }
                    if(TripleEqualsOrMatches.isEqualsForObjectOk(om)) {
                        if(bySubjectIndex.size() <= byObjectIndex.size()) {
                            return Pair.of(bySubjectIndex,
                                    t -> om.equals(t.getObject())
                                            && sm.equals(t.getSubject()));
                        }
                        return Pair.of(byObjectIndex,
                                t -> sm.equals(t.getSubject())
                                        && om.equals(t.getObject()));
                    } else {
                        if(bySubjectIndex.size() <= byObjectIndex.size()) {
                            return Pair.of(bySubjectIndex,
                                    t -> om.sameValueAs(t.getObject())
                                            && sm.equals(t.getSubject()));
                        }
                        return Pair.of(byObjectIndex,
                                t -> sm.equals(t.getSubject())
                                        && om.sameValueAs(t.getObject()));
                    }
                } else { // SPO:S**
                    return Pair.of(bySubjectIndex,
                            t -> sm.equals(t.getSubject()));
                }
            }
        }
        else if (om.isConcrete()) { // SPO:*?O
            var byObjectIndex = this.triplesByObject
                    .getIfPresent(new LookupObjectForTripleSetWithKey(om.getIndexingValue().hashCode()));
            if(byObjectIndex == null) {
                return null;
            }
            if(TripleEqualsOrMatches.isEqualsForObjectOk(om)) {
                if (pm.isConcrete()) { // SPO:*PO
                    var byPredicateIndex = this.triplesByPredicate
                            .getIfPresent(new LookupObjectForTripleSetWithKey(pm.getIndexingValue().hashCode()));
                    if(byPredicateIndex == null) {
                        return null;
                    }
                    if(byObjectIndex.size() <= byPredicateIndex.size()) {
                        return Pair.of(byObjectIndex,
                                t -> pm.equals(t.getPredicate())
                                        && om.equals(t.getObject()));
                    }
                    return Pair.of(byPredicateIndex,
                            t -> om.equals(t.getObject())
                                    && pm.equals(t.getPredicate()));
                } else { // SPO:**O
                    return Pair.of(byObjectIndex,
                            t -> om.equals(t.getObject()));
                }
            } else {
                if (pm.isConcrete()) { // SPO:*PO
                    var byPredicateIndex = this.triplesByPredicate
                            .getIfPresent(new LookupObjectForTripleSetWithKey(pm.getIndexingValue().hashCode()));
                    if(byPredicateIndex == null) {
                        return null;
                    }
                    if(byObjectIndex.size() <= byPredicateIndex.size()) {
                        return Pair.of(byObjectIndex,
                                t -> pm.equals(t.getPredicate())
                                        && om.sameValueAs(t.getObject()));
                    }
                    return Pair.of(byPredicateIndex,
                            t -> om.sameValueAs(t.getObject())
                                    && pm.equals(t.getPredicate()));
                } else { // SPO:**O
                    return Pair.of(byObjectIndex,
                            t -> om.sameValueAs(t.getObject()));
                }
            }
        }
        else if (pm.isConcrete()) { // SPO:*P*
            var byPredicateIndex = this.triplesByPredicate
                    .getIfPresent(new LookupObjectForTripleSetWithKey(pm.getIndexingValue().hashCode()));
            if(byPredicateIndex == null) {
                return null;
            }
            return Pair.of(byPredicateIndex,
                    t -> pm.equals(t.getPredicate()));
        }
        else { // SPO:***
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
                var subjects = triplesBySubject.getIfPresent(new LookupObjectForTripleSetWithKey(sm.getIndexingValue().hashCode()));
                if(subjects == null) {
                    return false;
                }
                return subjects.contains(triple);
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
    private LowMemoryHashSet<TripleSetWithKey> getMapWithFewestKeys() {
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
        private boolean hasSubIterator = false;

        public ListsOfTriplesIterator(Iterator<TripleSetWithKey> baseIterator) {

            this.baseIterator = baseIterator;
            if(baseIterator.hasNext()) {
                subIterator = baseIterator.next().iterator();
                hasSubIterator = true;
            }
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
            if(hasSubIterator) {
                if(subIterator.hasNext()) {
                    return true;
                }
                while(baseIterator.hasNext()) {
                    subIterator = baseIterator.next().iterator();
                    if(subIterator.hasNext()) {
                        return true;
                    }
                }
                hasSubIterator = false;
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
            if(!hasSubIterator || !this.hasNext()) {
                throw new NoSuchElementException();
            }
            return subIterator.next();
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
                this.hasCurrent = filter.test(candidate);
                if(this.hasCurrent) {
                    this.current = candidate;
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
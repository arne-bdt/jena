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

package org.apache.jena.mem2.specialized;

import org.apache.jena.graph.Triple;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Queue which grows, if needed but never shrinks.
 * This queue does not guarantee any order.
 * It´s purpose is to support fast remove operations.
 */
public abstract class FastTripleHashSetWithIndexingValue2 implements TripleSetWithIndexingValue2 {
    @Override
    public boolean areOperationsWithHashCodesSupported() {
        return true;
    }

    /*Idea from hashmap: improve hash code by (h = key.hashCode()) ^ (h >>> 16)*/
    private int calcStartIndexByHashCode(final int hashCode) {
        return (hashCode ^ (hashCode >>> 16)) & (entries.length-1);
    }

    protected Predicate<Triple> getContainsPredicate(final Triple value) {
        return other -> value.equals(other);
    }

    protected abstract int combineNodeHashes(final int hashCodeOfNode1, final int hashCodeOfNode2);

    protected abstract int getHashCodeOfNode1(final Triple triple);
    protected abstract int getHashCodeOfNode2(final Triple triple);

    private static int MINIMUM_SIZE = 16;
    private static float loadFactor = 0.5f;
    protected int size = 0;
    protected Triple[] entries;
    protected int[] hashCodesOfNode1;
    protected int[] hashCodesOfNode2;

    private final Object indexingValue;

    @Override
    public Object getIndexingValue() {
        return this.indexingValue;
    }

    public FastTripleHashSetWithIndexingValue2(TripleSetWithIndexingValue2 set) {
        this.indexingValue = set.getIndexingValue();
        this.entries = new Triple[Integer.highestOneBit(((int)(set.size()/loadFactor)+1)) << 1];
        this.hashCodesOfNode1 = new int[entries.length];
        this.hashCodesOfNode2 = new int[entries.length];
        int index, hashCodeOfNode1, hashCodeOfNode2;
        for (Triple t : set) {
            index = findEmptySlotWithoutEqualityCheck(combineNodeHashes(hashCodeOfNode1 = getHashCodeOfNode1(t), hashCodeOfNode2 = getHashCodeOfNode2(t)));
            entries[index] = t;
            hashCodesOfNode1[index] = hashCodeOfNode1;
            hashCodesOfNode2[index] = hashCodeOfNode2;
            size++;
        }
    }

    private int calcNewSize() {
        if(size >= entries.length*loadFactor && entries.length <= 1 << 30) { /*grow*/
            return entries.length << 1;
        }
        return -1;
    }

    private void growToMinCapacity(final int minCapacity) {
        grow(Integer.highestOneBit(((int)(minCapacity/loadFactor)+1)) << 1);
    }

    private boolean growIfNeeded() {
        final var newSize = calcNewSize();
        if(newSize < 0) {
            return false;
        }
        grow(newSize);
        return true;
    }

    private void grow() {
        final var newSize = calcNewSize();
        if(newSize < 0) {
            return;
        }
        grow(newSize);
    }

    private void grow(final int newSize) {
        final var oldEntries = this.entries;
        final var hashCodesOfNode1 = this.hashCodesOfNode1;
        final var hashCodesOfNode2 = this.hashCodesOfNode2;
        this.entries = new Triple[newSize];
        this.hashCodesOfNode1 = new int[newSize];
        this.hashCodesOfNode2 = new int[newSize];
        for(int i=0; i<oldEntries.length; i++) {
            if(null != oldEntries[i]) {
                var newSlot = findEmptySlotWithoutEqualityCheck(combineNodeHashes(hashCodesOfNode1[i], hashCodesOfNode2[i]));
                this.entries[newSlot] = oldEntries[i];
                this.hashCodesOfNode1[newSlot] = hashCodesOfNode1[i];
                this.hashCodesOfNode2[newSlot] = hashCodesOfNode2[i];
            }
        }
    }

    /**
     * Returns the number of elements in this collection.  If this collection
     * contains more than {@code Integer.MAX_VALUE} elements, returns
     * {@code Integer.MAX_VALUE}.
     *
     * @return the number of elements in this collection
     */
    @Override
    public int size() {
        return size;
    }

    /**
     * Returns {@code true} if this collection contains no elements.
     *
     * @return {@code true} if this collection contains no elements
     */
    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean contains(Object o) {
        final var t = (Triple)o;
        final int hashCodeOfNode1, hashCodeOfNode2;
        var index = calcStartIndexByHashCode(
                combineNodeHashes(hashCodeOfNode1 = getHashCodeOfNode1(t), hashCodeOfNode2 = getHashCodeOfNode2(t)));
        if(null == entries[index]) {
            return false;
        }
        var predicate = getContainsPredicate(t);
        if(hashCodeOfNode1 == hashCodesOfNode1[index]
                && hashCodeOfNode2 == hashCodesOfNode2[index]
                && predicate.test(entries[index])) {
            return true;
        } else if(--index < 0){
            index += entries.length;
        }
        while(true) {
            if(null == entries[index]) {
                return false;
            } else if(hashCodeOfNode1 == hashCodesOfNode1[index]
                    && hashCodeOfNode2 == hashCodesOfNode2[index]
                    && predicate.test(entries[index])) {
                return true;
            } else if(--index < 0){
                index += entries.length;
            }
        }
    }


    @Override
    public Iterator<Triple> iterator() {
        return new ArrayWithNullsIterator(entries, size);
    }

    @Override
    public Iterator<Triple> iterator(int hashCodeOfNode1, int hashCodeOfNode2) {
        return new ArrayWithNullsIteratorTwoNodes(entries, hashCodesOfNode1, hashCodesOfNode2, hashCodeOfNode1, hashCodeOfNode2);
    }

    @Override
    public Iterator<Triple> iteratorByNode1(int hashCodeOfNode1) {
        return new ArrayWithNullsIteratorOneNode(entries, hashCodesOfNode1, hashCodeOfNode1);
    }

    @Override
    public Iterator<Triple> iteratorByNode2(int hashCodeOfNode2) {
        return new ArrayWithNullsIteratorOneNode(entries, hashCodesOfNode2, hashCodeOfNode2);
    }

    @Override
    public Object[] toArray() {
        return this.stream().toArray();
    }


    @Override
    public <T1> T1[] toArray(T1[] a) {
        var asArray = this.stream().toArray();
        if (a.length < size) {
            return (T1[]) asArray;
        }
        System.arraycopy(asArray, 0, a, 0, asArray.length);
        if (a.length > size)
            a[size] = null;
        return a;
    }

    @Override
    public boolean add(Triple value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(Triple value, int hashCodeOfNode1, int hashCodeOfNode2) {
        grow();
        var index = findIndex(value, hashCodeOfNode1, hashCodeOfNode2);
        if(index < 0) {
            entries[index = ~index] = value;
            hashCodesOfNode1[index] = hashCodeOfNode1;
            hashCodesOfNode2[index] = hashCodeOfNode2;
            size++;
            return true;
        }
        return false;
    }

    public void addUnsafe(Triple value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addUnsafe(Triple value, int hashCodeOfNode1, int hashCodeOfNode2) {
        grow();
        var index = findEmptySlotWithoutEqualityCheck(combineNodeHashes(hashCodeOfNode1, hashCodeOfNode2));
        entries[index] = value;
        hashCodesOfNode1[index] = hashCodeOfNode1;
        hashCodesOfNode2[index] = hashCodeOfNode2;
        size++;
    }

    private int findIndex(final Triple e, final int hashCodeOfNode1, final int hashCodeOfNode2) {
        var index = calcStartIndexByHashCode(combineNodeHashes(hashCodeOfNode1, hashCodeOfNode2));
        while(true) {
            if(null == entries[index]) {
                return ~index;
            } else if(hashCodeOfNode1 == hashCodesOfNode1[index]
                    && hashCodeOfNode2 == hashCodesOfNode2[index]
                    && e.equals(entries[index])) {
                return index;
            } else if(--index < 0){
                index += entries.length;
            }
        }
    }

    private int findEmptySlotWithoutEqualityCheck(final int hashCode) {
        var index = calcStartIndexByHashCode(hashCode);
        while(true) {
            if(null == entries[index]) {
                return index;
            } else if(--index < 0){
                index += entries.length;
            }
        }
    }

    /**
     * Removes a single instance of the specified element from this
     * collection, if it is present (optional operation).  More formally,
     * removes an element {@code e} such that
     * {@code Objects.equals(o, e)}, if
     * this collection contains one or more such elements.  Returns
     * {@code true} if this collection contained the specified element (or
     * equivalently, if this collection changed as a result of the call).
     *
     * @param o element to be removed from this collection, if present
     * @return {@code true} if an element was removed as a result of this call
     * @throws ClassCastException            if the type of the specified element
     *                                       is incompatible with this collection
     *                                       (<a href="{@docRoot}/java.base/java/util/Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException          if the specified element is null and this
     *                                       collection does not permit null elements
     *                                       (<a href="{@docRoot}/java.base/java/util/Collection.html#optional-restrictions">optional</a>)
     * @throws UnsupportedOperationException if the {@code remove} operation
     *                                       is not supported by this collection
     */
    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Triple t, int hashCodeOfNode1, int hashCodeOfNode2) {
        var index = findIndex(t, hashCodeOfNode1, hashCodeOfNode2);
        if (index < 0) {
            return false;
        }
        entries[index] = null;
        hashCodesOfNode1[index] = 0;
        hashCodesOfNode2[index] = 0;
        size--;
        rearrangeNeighbours(index);
        return true;
    }

    public void removeUnsafe(Triple e) {
        throw new UnsupportedOperationException();
    }

    public void removeUnsafe(Triple e, int hashCodeOfNode1, int hashCodeOfNode2) {
        var index = findIndex(e, hashCodeOfNode1, hashCodeOfNode2);
        entries[index] = null;
        hashCodesOfNode1[index] = hashCodesOfNode2[index] = 0;
        size--;
        rearrangeNeighbours(index);
    }

    private void rearrangeNeighbours(int index) {
        /*rearrange neighbours*/
        var neighbours = getNeighbours(index);
        if(neighbours == null) {
            return;
        }
        Arrays.sort(neighbours, ObjectsWithStartIndexIndexAndDistance.distanceComparator);
        boolean elementsHaveBeenSwitched;
        do {
            elementsHaveBeenSwitched = false;
            for (ObjectsWithStartIndexIndexAndDistance neighbour : neighbours) {
                if(neighbour.distance == 0) {
                    break;
                }
                if (neighbour.isTargetIndexNearerToStartIndex(index)){
                    var oldIndexOfNeighbour = neighbour.currentIndex;
                    entries[index] = entries[oldIndexOfNeighbour];
                    hashCodesOfNode1[index] = hashCodesOfNode1[oldIndexOfNeighbour];
                    hashCodesOfNode2[index] = hashCodesOfNode2[oldIndexOfNeighbour];
                    entries[oldIndexOfNeighbour] = null;
                    hashCodesOfNode1[oldIndexOfNeighbour] = hashCodesOfNode2[oldIndexOfNeighbour] = 0;
                    neighbour.setCurrentIndex(index);
                    index = oldIndexOfNeighbour;
                    Arrays.sort(neighbours, ObjectsWithStartIndexIndexAndDistance.distanceComparator);
                    elementsHaveBeenSwitched = true;
                    break;
                }
            }
        } while(elementsHaveBeenSwitched);
    }

    private ObjectsWithStartIndexIndexAndDistance[] getNeighbours(final int index) {
        var neighbours = new ArrayList<ObjectsWithStartIndexIndexAndDistance>();
        ObjectsWithStartIndexIndexAndDistance neighbour;
        /*find left*/
        var i = index;
        while(true) {
            if(--i < 0){
                i += entries.length;
            }
            if(null == entries[i]) {
                break;
            } else {
                neighbour = new ObjectsWithStartIndexIndexAndDistance(
                        entries.length, calcStartIndexByHashCode(combineNodeHashes(hashCodesOfNode1[i], hashCodesOfNode2[i])), i);
                if(neighbour.distance > 0) {
                    neighbours.add(neighbour);
                }
            }
        }
        i = index;
        /*find right*/
        while(true) {
            if(++i == entries.length){
                i = 0;
            }
            if(null == entries[i]) {
                break;
            } else {
                neighbour = new ObjectsWithStartIndexIndexAndDistance(
                        entries.length, calcStartIndexByHashCode(combineNodeHashes(hashCodesOfNode1[i], hashCodesOfNode2[i])), i);
                if(neighbour.distance > 0) {
                    neighbours.add(neighbour);
                }
            }
        }
        return neighbours.isEmpty()
                ? null : neighbours.toArray(new ObjectsWithStartIndexIndexAndDistance[neighbours.size()]);
    }

    private static class ObjectsWithStartIndexIndexAndDistance {
        public final static Comparator<ObjectsWithStartIndexIndexAndDistance>  distanceComparator
                = Comparator.comparingInt((ObjectsWithStartIndexIndexAndDistance n) -> n.distance).reversed();
        final int startIndex;
        final int length;
        int currentIndex;
        int distance;

        public ObjectsWithStartIndexIndexAndDistance(final int length, final int startIndex, final int currentIndex) {
            this.length = length;
            this.startIndex = startIndex;
            this.setCurrentIndex(currentIndex);
        }

        void setCurrentIndex(final int currentIndex) {
            this.currentIndex = currentIndex;
            this.distance = calcDistance(currentIndex);
        }

        private int calcDistance(final int index) {
            return index <= startIndex
                    ? startIndex - index
                    : startIndex + length - index;
        }

        boolean isTargetIndexNearerToStartIndex(final int targetIndex) {
            return calcDistance(targetIndex) < distance;
        }
    }

    /**
     * Returns {@code true} if this collection contains all of the elements
     * in the specified collection.
     *
     * @param c collection to be checked for containment in this collection
     * @return {@code true} if this collection contains all of the elements
     * in the specified collection
     * @throws ClassCastException   if the types of one or more elements
     *                              in the specified collection are incompatible with this
     *                              collection
     *                              (<a href="{@docRoot}/java.base/java/util/Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException if the specified collection contains one
     *                              or more null elements and this collection does not permit null
     *                              elements
     *                              (<a href="{@docRoot}/java.base/java/util/Collection.html#optional-restrictions">optional</a>),
     *                              or if the specified collection is null.
     * @see #contains(Object)
     */
    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if(!this.contains(o)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Adds all of the elements in the specified collection to this collection
     * (optional operation).  The behavior of this operation is undefined if
     * the specified collection is modified while the operation is in progress.
     * (This implies that the behavior of this call is undefined if the
     * specified collection is this collection, and this collection is
     * nonempty.)
     *
     * @param c collection containing elements to be added to this collection
     * @return {@code true} if this collection changed as a result of the call
     * @throws UnsupportedOperationException if the {@code addAll} operation
     *                                       is not supported by this collection
     * @throws ClassCastException            if the class of an element of the specified
     *                                       collection prevents it from being added to this collection
     * @throws NullPointerException          if the specified collection contains a
     *                                       null element and this collection does not permit null elements,
     *                                       or if the specified collection is null
     * @throws IllegalArgumentException      if some property of an element of the
     *                                       specified collection prevents it from being added to this
     *                                       collection
     * @throws IllegalStateException         if not all the elements can be added at
     *                                       this time due to insertion restrictions
     * @see #add(Object)
     */
    @Override
    public boolean addAll(Collection<? extends Triple> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * Removes all of this collection's elements that are also contained in the
     * specified collection (optional operation).  After this call returns,
     * this collection will contain no elements in common with the specified
     * collection.
     *
     * @param c collection containing elements to be removed from this collection
     * @return {@code true} if this collection changed as a result of the
     * call
     * @throws UnsupportedOperationException if the {@code removeAll} method
     *                                       is not supported by this collection
     * @throws ClassCastException            if the types of one or more elements
     *                                       in this collection are incompatible with the specified
     *                                       collection
     *                                       (<a href="{@docRoot}/java.base/java/util/Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException          if this collection contains one or more
     *                                       null elements and the specified collection does not support
     *                                       null elements
     *                                       (<a href="{@docRoot}/java.base/java/util/Collection.html#optional-restrictions">optional</a>),
     *                                       or if the specified collection is null
     * @see #remove(Object)
     * @see #contains(Object)
     */
    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * Retains only the elements in this collection that are contained in the
     * specified collection (optional operation).  In other words, removes from
     * this collection all of its elements that are not contained in the
     * specified collection.
     *
     * @param c collection containing elements to be retained in this collection
     * @return {@code true} if this collection changed as a result of the call
     * @throws UnsupportedOperationException if the {@code retainAll} operation
     *                                       is not supported by this collection
     * @throws ClassCastException            if the types of one or more elements
     *                                       in this collection are incompatible with the specified
     *                                       collection
     *                                       (<a href="{@docRoot}/java.base/java/util/Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException          if this collection contains one or more
     *                                       null elements and the specified collection does not permit null
     *                                       elements
     *                                       (<a href="{@docRoot}/java.base/java/util/Collection.html#optional-restrictions">optional</a>),
     *                                       or if the specified collection is null
     * @see #remove(Object)
     * @see #contains(Object)
     */
    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * Removes all of the elements from this collection (optional operation).
     * The collection will be empty after this method returns.
     *
     * @throws UnsupportedOperationException if the {@code clear} operation
     *                                       is not supported by this collection
     */
    @Override
    public void clear() {
        entries = new Triple[MINIMUM_SIZE];
        hashCodesOfNode1 = new int[MINIMUM_SIZE];
        size = 0;
    }

    /**
     * Returns a sequential {@code Stream} with this collection as its source.
     *
     * <p>This method should be overridden when the {@link #spliterator()}
     * method cannot return a spliterator that is {@code IMMUTABLE},
     * {@code CONCURRENT}, or <em>late-binding</em>. (See {@link #spliterator()}
     * for details.)
     *
     * @return a sequential {@code Stream} over the elements in this collection
     * @implSpec The default implementation creates a sequential {@code Stream} from the
     * collection's {@code Spliterator}.
     * @since 1.8
     */
    @Override
    public Stream<Triple> stream() {
        return StreamSupport.stream(new ArrayWithNullsSpliteratorSized(entries, size), false);
    }

    /**
     * Returns a possibly parallel {@code Stream} with this collection as its
     * source.  It is allowable for this method to return a sequential stream.
     *
     * <p>This method should be overridden when the {@link #spliterator()}
     * method cannot return a spliterator that is {@code IMMUTABLE},
     * {@code CONCURRENT}, or <em>late-binding</em>. (See {@link #spliterator()}
     * for details.)
     *
     * @return a possibly parallel {@code Stream} over the elements in this
     * collection
     * @implSpec The default implementation creates a parallel {@code Stream} from the
     * collection's {@code Spliterator}.
     * @since 1.8
     */
    @Override
    public Stream<Triple> parallelStream() {
        return StreamSupport.stream(new ArrayWithNullsSpliteratorSized(entries, size), true);
    }

    private static class ArrayWithNullsSpliteratorSized implements Spliterator<Triple> {

        private final Triple[] entries;
        private final int maxPos;
        private int pos = -1;
        private int maxRemaining;
        private boolean hasBeenSplit = false;

        public ArrayWithNullsSpliteratorSized(final Triple[] entries, final int size) {
            this.entries = entries;
            this.maxPos = entries.length - 1;
            this.maxRemaining = size;
        }

        /**
         * If a remaining element exists, performs the given action on it,
         * returning {@code true}; else returns {@code false}.  If this
         * Spliterator is {@link #ORDERED} the action is performed on the
         * next element in encounter order.  Exceptions thrown by the
         * action are relayed to the caller.
         *
         * @param action The action
         * @return {@code false} if no remaining elements existed
         * upon entry to this method, else {@code true}.
         * @throws NullPointerException if the specified action is null
         */
        @Override
        public boolean tryAdvance(Consumer<? super Triple> action) {
            if(0 < maxRemaining) {
                while (pos < maxPos) {
                    if (null != entries[++pos]) {
                        maxRemaining--;
                        action.accept(entries[pos]);
                        return true;
                    }
                }
            }
            return false;
        }

        /**
         * Performs the given action for each remaining element, sequentially in
         * the current thread, until all elements have been processed or the action
         * throws an exception.  If this Spliterator is {@link #ORDERED}, actions
         * are performed in encounter order.  Exceptions thrown by the action
         * are relayed to the caller.
         *
         * @param action The action
         * @throws NullPointerException if the specified action is null
         * @implSpec The default implementation repeatedly invokes {@link #tryAdvance} until
         * it returns {@code false}.  It should be overridden whenever possible.
         */
        @Override
        public void forEachRemaining(Consumer<? super Triple> action) {
            while(0 < maxRemaining && pos < maxPos) {
                if(null != entries[++pos]) {
                    maxRemaining--;
                    action.accept(entries[pos]);
                }
            }
        }

        /**
         * If this spliterator can be partitioned, returns a Spliterator
         * covering elements, that will, upon return from this method, not
         * be covered by this Spliterator.
         *
         * <p>If this Spliterator is {@link #ORDERED}, the returned Spliterator
         * must cover a strict prefix of the elements.
         *
         * <p>Unless this Spliterator covers an infinite number of elements,
         * repeated calls to {@code trySplit()} must eventually return {@code null}.
         * Upon non-null return:
         * <ul>
         * <li>the value reported for {@code estimateSize()} before splitting,
         * must, after splitting, be greater than or equal to {@code estimateSize()}
         * for this and the returned Spliterator; and</li>
         * <li>if this Spliterator is {@code SUBSIZED}, then {@code estimateSize()}
         * for this spliterator before splitting must be equal to the sum of
         * {@code estimateSize()} for this and the returned Spliterator after
         * splitting.</li>
         * </ul>
         *
         * <p>This method may return {@code null} for any reason,
         * including emptiness, inability to split after traversal has
         * commenced, data structure constraints, and efficiency
         * considerations.
         *
         * @return a {@code Spliterator} covering some portion of the
         * elements, or {@code null} if this spliterator cannot be split
         * @apiNote An ideal {@code trySplit} method efficiently (without
         * traversal) divides its elements exactly in half, allowing
         * balanced parallel computation.  Many departures from this ideal
         * remain highly effective; for example, only approximately
         * splitting an approximately balanced tree, or for a tree in
         * which leaf nodes may contain either one or two elements,
         * failing to further split these nodes.  However, large
         * deviations in balance and/or overly inefficient {@code
         * trySplit} mechanics typically result in poor parallel
         * performance.
         */
        @Override
        public Spliterator<Triple> trySplit() {
            if(entries.length - pos < 10) {
                return null;
            }
            var mid = (pos + 1 + entries.length) >>> 1;
            var remainingInSubSpliterator = this.maxRemaining;
            var fromIndexForSubSpliterator = pos + 1;
            this.pos = mid - 1;
            this.maxRemaining = Math.min(this.maxRemaining, entries.length- pos);
            remainingInSubSpliterator = Math.min(remainingInSubSpliterator, mid-fromIndexForSubSpliterator);
            this.hasBeenSplit = true;
            return new ArrayWithNullsSubSpliteratorUnSized(entries, fromIndexForSubSpliterator, mid, remainingInSubSpliterator);
        }

        /**
         * Returns an estimate of the number of elements that would be
         * encountered by a {@link #forEachRemaining} traversal, or returns {@link
         * Long#MAX_VALUE} if infinite, unknown, or too expensive to compute.
         *
         * <p>If this Spliterator is {@link #SIZED} and has not yet been partially
         * traversed or split, or this Spliterator is {@link #SUBSIZED} and has
         * not yet been partially traversed, this estimate must be an accurate
         * count of elements that would be encountered by a complete traversal.
         * Otherwise, this estimate may be arbitrarily inaccurate, but must decrease
         * as specified across invocations of {@link #trySplit}.
         *
         * @return the estimated size, or {@code Long.MAX_VALUE} if infinite,
         * unknown, or too expensive to compute.
         * @apiNote Even an inexact estimate is often useful and inexpensive to compute.
         * For example, a sub-spliterator of an approximately balanced binary tree
         * may return a value that estimates the number of elements to be half of
         * that of its parent; if the root Spliterator does not maintain an
         * accurate count, it could estimate size to be the power of two
         * corresponding to its maximum depth.
         */
        @Override
        public long estimateSize() {
            return maxRemaining;
        }

        /**
         * Returns a set of characteristics of this Spliterator and its
         * elements. The result is represented as ORed values from {@link
         * #ORDERED}, {@link #DISTINCT}, {@link #SORTED}, {@link #SIZED},
         * {@link #NONNULL}, {@link #IMMUTABLE}, {@link #CONCURRENT},
         * {@link #SUBSIZED}.  Repeated calls to {@code characteristics()} on
         * a given spliterator, prior to or in-between calls to {@code trySplit},
         * should always return the same result.
         *
         * <p>If a Spliterator reports an inconsistent set of
         * characteristics (either those returned from a single invocation
         * or across multiple invocations), no guarantees can be made
         * about any computation using this Spliterator.
         *
         * @return a representation of characteristics
         * @apiNote The characteristics of a given spliterator before splitting
         * may differ from the characteristics after splitting.  For specific
         * examples see the characteristic values {@link #SIZED}, {@link #SUBSIZED}
         * and {@link #CONCURRENT}.
         */
        @Override
        public int characteristics() {
            if(this.hasBeenSplit) {
                return DISTINCT |NONNULL | IMMUTABLE;
            } else {
                return DISTINCT | SIZED | NONNULL | IMMUTABLE;
            }
        }
    }

    private static class ArrayWithNullsSubSpliteratorUnSized implements Spliterator<Triple> {

        private final Triple[] entries;
        private int pos;
        private final int maxPos;
        private int maxRemaining;

        public ArrayWithNullsSubSpliteratorUnSized(final Triple[] entries, final int fromIndex, final int toIndex, final int maxSize) {
            this.entries = entries;
            this.maxRemaining = maxSize;
            this.pos = fromIndex - 1;
            this.maxPos = toIndex - 1;
            this.updateMaxRemaining();
        }

        private void updateMaxRemaining() {
            this.maxRemaining = Math.min(this.maxRemaining, entries.length-(maxPos - pos));
        }

        /**
         * If a remaining element exists, performs the given action on it,
         * returning {@code true}; else returns {@code false}.  If this
         * Spliterator is {@link #ORDERED} the action is performed on the
         * next element in encounter order.  Exceptions thrown by the
         * action are relayed to the caller.
         *
         * @param action The action
         * @return {@code false} if no remaining elements existed
         * upon entry to this method, else {@code true}.
         * @throws NullPointerException if the specified action is null
         */
        @Override
        public boolean tryAdvance(Consumer<? super Triple> action) {
            if(0 < maxRemaining) {
                while (pos < maxPos) {
                    if (null != entries[++pos]) {
                        maxRemaining--;
                        action.accept(entries[pos]);
                        return true;
                    }
                }
            }
            return false;
        }

        /**
         * Performs the given action for each remaining element, sequentially in
         * the current thread, until all elements have been processed or the action
         * throws an exception.  If this Spliterator is {@link #ORDERED}, actions
         * are performed in encounter order.  Exceptions thrown by the action
         * are relayed to the caller.
         *
         * @param action The action
         * @throws NullPointerException if the specified action is null
         * @implSpec The default implementation repeatedly invokes {@link #tryAdvance} until
         * it returns {@code false}.  It should be overridden whenever possible.
         */
        @Override
        public void forEachRemaining(Consumer<? super Triple> action) {
            while(0 < maxRemaining && pos < maxPos) {
                if(null != entries[++pos]) {
                    maxRemaining--;
                    action.accept(entries[pos]);
                }
            }
        }

        /**
         * If this spliterator can be partitioned, returns a Spliterator
         * covering elements, that will, upon return from this method, not
         * be covered by this Spliterator.
         *
         * <p>If this Spliterator is {@link #ORDERED}, the returned Spliterator
         * must cover a strict prefix of the elements.
         *
         * <p>Unless this Spliterator covers an infinite number of elements,
         * repeated calls to {@code trySplit()} must eventually return {@code null}.
         * Upon non-null return:
         * <ul>
         * <li>the value reported for {@code estimateSize()} before splitting,
         * must, after splitting, be greater than or equal to {@code estimateSize()}
         * for this and the returned Spliterator; and</li>
         * <li>if this Spliterator is {@code SUBSIZED}, then {@code estimateSize()}
         * for this spliterator before splitting must be equal to the sum of
         * {@code estimateSize()} for this and the returned Spliterator after
         * splitting.</li>
         * </ul>
         *
         * <p>This method may return {@code null} for any reason,
         * including emptiness, inability to split after traversal has
         * commenced, data structure constraints, and efficiency
         * considerations.
         *
         * @return a {@code Spliterator} covering some portion of the
         * elements, or {@code null} if this spliterator cannot be split
         * @apiNote An ideal {@code trySplit} method efficiently (without
         * traversal) divides its elements exactly in half, allowing
         * balanced parallel computation.  Many departures from this ideal
         * remain highly effective; for example, only approximately
         * splitting an approximately balanced tree, or for a tree in
         * which leaf nodes may contain either one or two elements,
         * failing to further split these nodes.  However, large
         * deviations in balance and/or overly inefficient {@code
         * trySplit} mechanics typically result in poor parallel
         * performance.
         */
        @Override
        public Spliterator<Triple> trySplit() {
            if(maxPos - pos < 10) {
                return null;
            }
            var mid = (pos + maxPos + 2) >>> 1;
            var remainingInSubSpliterator = this.maxRemaining;
            var fromIndexForSubSpliterator = pos +1;
            this.pos = mid - 1;
            this.updateMaxRemaining();
            remainingInSubSpliterator = Math.min(remainingInSubSpliterator, mid-fromIndexForSubSpliterator);
            return new ArrayWithNullsSubSpliteratorUnSized(entries, fromIndexForSubSpliterator, mid, remainingInSubSpliterator);
        }

        /**
         * Returns an estimate of the number of elements that would be
         * encountered by a {@link #forEachRemaining} traversal, or returns {@link
         * Long#MAX_VALUE} if infinite, unknown, or too expensive to compute.
         *
         * <p>If this Spliterator is {@link #SIZED} and has not yet been partially
         * traversed or split, or this Spliterator is {@link #SUBSIZED} and has
         * not yet been partially traversed, this estimate must be an accurate
         * count of elements that would be encountered by a complete traversal.
         * Otherwise, this estimate may be arbitrarily inaccurate, but must decrease
         * as specified across invocations of {@link #trySplit}.
         *
         * @return the estimated size, or {@code Long.MAX_VALUE} if infinite,
         * unknown, or too expensive to compute.
         * @apiNote Even an inexact estimate is often useful and inexpensive to compute.
         * For example, a sub-spliterator of an approximately balanced binary tree
         * may return a value that estimates the number of elements to be half of
         * that of its parent; if the root Spliterator does not maintain an
         * accurate count, it could estimate size to be the power of two
         * corresponding to its maximum depth.
         */
        @Override
        public long estimateSize() {
            return maxRemaining;
        }

        /**
         * Returns a set of characteristics of this Spliterator and its
         * elements. The result is represented as ORed values from {@link
         * #ORDERED}, {@link #DISTINCT}, {@link #SORTED}, {@link #SIZED},
         * {@link #NONNULL}, {@link #IMMUTABLE}, {@link #CONCURRENT},
         * {@link #SUBSIZED}.  Repeated calls to {@code characteristics()} on
         * a given spliterator, prior to or in-between calls to {@code trySplit},
         * should always return the same result.
         *
         * <p>If a Spliterator reports an inconsistent set of
         * characteristics (either those returned from a single invocation
         * or across multiple invocations), no guarantees can be made
         * about any computation using this Spliterator.
         *
         * @return a representation of characteristics
         * @apiNote The characteristics of a given spliterator before splitting
         * may differ from the characteristics after splitting.  For specific
         * examples see the characteristic values {@link #SIZED}, {@link #SUBSIZED}
         * and {@link #CONCURRENT}.
         */
        @Override
        public int characteristics() {
            return DISTINCT | NONNULL | IMMUTABLE;
        }
    }

    private static class ArrayWithNullsIterator implements Iterator<Triple> {

        private final Triple[] entries;
        private int remaining;
        private int pos = -1;

        private ArrayWithNullsIterator(final Triple[] entries, final int size) {
            this.entries = entries;
            this.remaining = size;
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
            return 0 < remaining;
        }

        /**
         * Returns the next element in the iteration.
         *
         * @return the next element in the iteration
         * @throws NoSuchElementException if the iteration has no more elements
         */
        @Override
        public Triple next() {
            if(0 < remaining--) {
                while(entries[++pos] == null);
                return entries[pos];
            }
            throw new NoSuchElementException();
        }
    }

    private static class ArrayWithNullsIteratorOneNode implements Iterator<Triple> {

        protected final Triple[] entries;
        protected final int[] hashCodesOfNode1;
        protected final int hashCodeOfNode1;
        protected boolean hasCurrent = false;

        protected int pos;

        private ArrayWithNullsIteratorOneNode(final Triple[] entries, final int[] hashCodesOfNode1, final int hashCodeOfNode1) {
            this.entries = entries;
            this.hashCodesOfNode1 = hashCodesOfNode1;
            this.hashCodeOfNode1 = hashCodeOfNode1;
            pos = entries.length;
        }

        @Override
        public boolean hasNext() {
            if(hasCurrent) {
                return true;
            }
            while(pos-- > 0) {
                if(hashCodeOfNode1 == hashCodesOfNode1[pos]
                        && entries[pos] != null) {
                    return hasCurrent = true;
                }
            }
            return false;
        }

        @Override
        public Triple next() {
            if (hasCurrent || hasNext()) {
                hasCurrent = false;
                return entries[pos];
            }
            throw new NoSuchElementException();
        }
    }

    private static class ArrayWithNullsIteratorTwoNodes extends ArrayWithNullsIteratorOneNode {

        protected final int[] hashCodesOfNode2;
        protected final int hashCodeOfNode2;

        private ArrayWithNullsIteratorTwoNodes(final Triple[] entries, int[] hashCodesOfNode1, int[] hashCodesOfNode2, final int hashCodeOfNode1, final int hashCodeOfNode2) {
            super(entries, hashCodesOfNode1, hashCodeOfNode1);
            this.hashCodesOfNode2 = hashCodesOfNode2;
            this.hashCodeOfNode2 = hashCodeOfNode2;
        }

        @Override
        public boolean hasNext() {
            if(hasCurrent) {
                return true;
            }
            while(pos-- > 0) {
                if(hashCodeOfNode1 == hashCodesOfNode1[pos]
                        && hashCodeOfNode2 == hashCodesOfNode2[pos]
                        && entries[pos] != null) {
                    return hasCurrent = true;
                }
            }
            return false;
        }
    }
}

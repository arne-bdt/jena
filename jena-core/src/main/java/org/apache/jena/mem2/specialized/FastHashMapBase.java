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

import org.apache.jena.mem2.iterator.ArrayWithNullsIterator;
import org.apache.jena.mem2.spliterator.ArrayWithNullsSpliteratorSized;
import org.apache.jena.mem2.spliterator.SparseArraySpliterator;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Queue which grows, if needed but never shrinks.
 * This queue does not guarantee any order.
 * It´s purpose is to support fast remove operations.
 */
public abstract class FastHashMapBase<E> implements Collection<E> {
    /*Idea from hashmap: improve hash code by (h = key.hashCode()) ^ (h >>> 16)*/
    private int calcStartIndexByHashCode(final int hashCode) {
        //return (hashCode ^ (hashCode >>> 16)) & (entries.length-1);
        return hashCode & (entries.length-1);
    }
    private static int MINIMUM_SIZE = 2;
    private static float loadFactor = 0.5f;
    protected int size = 0;
    protected E[] entries;
    protected int[] hashCodes;

    /**
     * This method must be overridden by subclasses to create an array of the correct type.
     * It is used to avoid the performance penalty of using reflection to create an array.
     * @param length
     * @return
     */
    protected abstract E[] createEntryArray(final int length);

    public FastHashMapBase() {
        this.entries = createEntryArray(MINIMUM_SIZE);
        this.hashCodes = new int[entries.length];
    }

    public FastHashMapBase(final int minCapacity) {
        this.entries = createEntryArray(Integer.highestOneBit(((int)(minCapacity/loadFactor)+1)) << 1);
        this.hashCodes = new int[entries.length];
    }

    private int calcNewSize() {
        if(size >= entries.length*loadFactor && entries.length <= 1 << 30) { /*grow*/
            return entries.length << 1;
        }
        return -1;
    }



    private void grow(final int minCapacity) {
        final var oldEntries = this.entries;
        final var oldHashCodes = this.hashCodes;
        this.entries = createEntryArray(Integer.highestOneBit(((int)(minCapacity/loadFactor)+1)) << 1);
        this.hashCodes = new int[entries.length];
        for(int i=0; i<oldEntries.length; i++) {
            if(null != oldEntries[i]) {
                var newSlot = findEmptySlotWithoutEqualityCheck(oldHashCodes[i]);
                this.entries[newSlot] = oldEntries[i];
                this.hashCodes[newSlot] = oldHashCodes[i];
            }
        }
    }

    private boolean grow() {
        final var newSize = calcNewSize();
        if(newSize < 0) {
            return false;
        }
        final var oldEntries = this.entries;
        final var oldHashCodes = this.hashCodes;
        this.entries = createEntryArray(newSize);
        this.hashCodes = new int[newSize];
        for(int i=0; i<oldEntries.length; i++) {
            if(null != oldEntries[i]) {
                var newSlot = findEmptySlotWithoutEqualityCheck(oldHashCodes[i]);
                this.entries[newSlot] = oldEntries[i];
                this.hashCodes[newSlot] = oldHashCodes[i];
            }
        }
        return true;
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
        throw new UnsupportedOperationException();
    }

//    @Override
//    public boolean contains(Object o) {
//        final var e = (E)o;
//        final int hashCode;
//        var index = calcStartIndexByHashCode(hashCode = e.hashCode());
//        if(null == entries[index]) {
//            return false;
//        }
//        if(hashCode == hashCodes[index] && e.equals(entries[index])) {
//            return true;
//        } else if(--index < 0){
//            index += entries.length;
//        }
//        while(true) {
//            if(null == entries[index]) {
//                return false;
//            } else if(hashCode == hashCodes[index] && e.equals(entries[index])) {
//                return true;
//            } else if(--index < 0){
//                index += entries.length;
//            }
//        }
//    }


    @Override
    public Iterator<E> iterator() {
        return new ArrayWithNullsIterator(entries);
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
    public boolean add(E e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    public E findAny() {
        var index = -1;
        while(entries[++index] == null);
        return entries[index];
    }


    public boolean add(int hashCode, E value) {
        grow();
        var index = findIndex(hashCode);
        if(index < 0) {
            entries[~index] = value;
            hashCodes[~index] = hashCode;
            size++;
            return true;
        }
        return false;
    }

    public void addUnsafe(int hashCode, E value) {
        grow();
        var index = findEmptySlotWithoutEqualityCheck(hashCode);
        entries[index] = value;
        hashCodes[index] = hashCode;
        size++;
    }

    public E getIfPresent(int hashCode) {
        var index = calcStartIndexByHashCode(hashCode);
        while(true) {
            if(null == entries[index]) {
                return null;
            } else if(hashCode == hashCodes[index]) {
                return entries[index];
            } else if(--index < 0){
                index += entries.length;
            }
        }
    }

    public E getOrCreate(int hashCode, Supplier<E> supplier) {
        var index = findIndex(hashCode);
        if(index < 0) {
            if(grow()) {
                index = findIndex(hashCode);
            }
            entries[index = ~index] = supplier.get();
            hashCodes[index] = hashCode;
            size++;
        }
        return entries[index];
    }

    public void compute(int hashCode, Function<E, E> remappingFunction) {
        var index = findIndex(hashCode);
        if(index < 0) { /*value does not exist yet*/
            var newValue = remappingFunction.apply(null);
            if(newValue == null) {
                return;
            }
//            if(hashCode != newValue.hashCode()
//                throw new IllegalArgumentException("remapped value is not equal to value");
//            }
            if(grow()) {
                index = findEmptySlotWithoutEqualityCheck(hashCode);
            } else {
                index = ~index;
            }
            entries[index] = newValue;
            hashCodes[index] = hashCode;
            size++;
        } else { /*existing value found*/
            var newValue = remappingFunction.apply(entries[index]);
            if(newValue == null) {
                entries[index] = null;
                size--;
                rearrangeNeighbours(index);
            } else {
                entries[index] = newValue;
            }
        }
    }


    public void removeIf(int hashCode, Predicate<E> predicate) {
        var index = findIndex(hashCode);
        if(index < 0) {
            return;
        } else {
            if(predicate.test(entries[index = ~index])) {
                entries[index] = null;
                size--;
            }
        }
    }

    private int findIndex(final int hashCode) {
        var index = calcStartIndexByHashCode(hashCode);
        while(true) {
            if(null == entries[index]) {
                return ~index;
            } else if(hashCode == hashCodes[index]) {
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

    public boolean remove(int hashCode) {
        var index = findIndex(hashCode);
        if (index < 0) {
            return false;
        }
        entries[index] = null;
        size--;
        rearrangeNeighbours(index);
        return true;
    }

    public void removeUnsafe(int hashCode) {
        var index = findIndex(hashCode);
        entries[index] = null;
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
                    hashCodes[index] = hashCodes[oldIndexOfNeighbour];
                    entries[oldIndexOfNeighbour] = null;
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
                        entries.length, calcStartIndexByHashCode(hashCodes[i]), i);
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
                        entries.length, calcStartIndexByHashCode(hashCodes[i]), i);
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
    public boolean addAll(Collection<? extends E> c) {
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
        entries = createEntryArray(MINIMUM_SIZE);
        hashCodes = new int[MINIMUM_SIZE];
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
    public Stream<E> stream() {
        return StreamSupport.stream(new SparseArraySpliterator(entries, size), false);
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
    public Stream<E> parallelStream() {
        return StreamSupport.stream(new ArrayWithNullsSpliteratorSized(entries, size), true);
    }

}

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

package org.apache.jena.mem2.collection;

import org.apache.jena.graph.Triple;
import org.apache.jena.memTermEquality.SparseArrayIterator;
import org.apache.jena.memTermEquality.SparseArraySpliterator;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Queue which grows, if needed but never shrinks.
 * This queue does not guarantee any order.
 * It´s purpose is to support fast remove operations.
 */
public class FastTripleHashSet implements Set<Triple> {

    /*Idea from hashmap: improve hash code by (h = key.hashCode()) ^ (h >>> 16)*/
    private int calcStartIndexByHashCode(final int hashCode) {
        //return (hashCode ^ (hashCode >>> 16)) & (entries.length-1);
        return hashCode & (entries.length-1);
    }

    private static int MINIMUM_SIZE = 16;
    private static float loadFactor = 0.5f;
    protected int size = 0;
    protected Triple[] entries;
    protected int[] hashCodes;

    public FastTripleHashSet(int initialSize) {
        this.entries = new Triple[Integer.highestOneBit(((int)(initialSize/loadFactor)+1)) << 1];
        this.hashCodes = new int[entries.length];
    }

    public FastTripleHashSet() {
        this.entries = new Triple[MINIMUM_SIZE];
        this.hashCodes = new int[MINIMUM_SIZE];

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
        this.entries = new Triple[Integer.highestOneBit(((int)(minCapacity/loadFactor)+1)) << 1];
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
        this.entries = new Triple[newSize];
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
        final int hashCode;
        var index = calcStartIndexByHashCode(hashCode = o.hashCode());
        while(true) {
            if(null == entries[index]) {
                return false;
            } else if(hashCode == hashCodes[index] && o.equals(entries[index])) {
                return true;
            } else if(--index < 0){
                index += entries.length;
            }
        }
    }


    @Override
    public Iterator<Triple> iterator() {
        final var initialSize = size;
        final Runnable checkForConcurrentModification = () ->
        {
            if (size != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArrayIterator<>(entries, checkForConcurrentModification);
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

    public Triple findAny() {
        var index = -1;
        while(entries[++index] == null);
        return entries[index];
    }

    @Override
    public boolean add(Triple value) {
        return add(value, value.hashCode());
    }

    public boolean add(Triple value, int hashCode) {
        grow();
        var index = findIndex(value, hashCode);
        if(index < 0) {
            entries[~index] = value;
            hashCodes[~index] = hashCode;
            size++;
            return true;
        }
        return false;
    }

    public void addUnchecked(Triple value) {
        addUnchecked(value, value.hashCode());
    }

    public void addUnchecked(Triple value, int hashCode) {
        grow();
        var index = findEmptySlotWithoutEqualityCheck(hashCode);
        entries[index] = value;
        hashCodes[index] = hashCode;
        size++;
    }

    public Triple addIfAbsent(Triple value) {
        grow();
        final int hashCode;
        final var index = findIndex(value, hashCode = value.hashCode());
        if(index < 0) {
            entries[~index] = value;
            hashCodes[~index] = hashCode;
            size++;
            return value;
        }
        return entries[index];
    }

    public Triple getIfPresent(Triple value) {
        final int hashCode;
        var index = calcStartIndexByHashCode(hashCode = value.hashCode());
        while(true) {
            if(null == entries[index]) {
                return null;
            } else if(hashCode == hashCodes[index] && value.equals(entries[index])) {
                return entries[index];
            } else if(--index < 0){
                index += entries.length;
            }
        }
    }

    public Triple compute(Triple value, Function<Triple, Triple> remappingFunction) {
        final int hashCode;
        var index = findIndex(value, hashCode = value.hashCode());
        if(index < 0) { /*value does not exist yet*/
            var newValue = remappingFunction.apply(null);
            if(newValue == null) {
                return null;
            }
            if(!value.equals(newValue)) {
                throw new IllegalArgumentException("remapped value is not equal to value");
            }
            if(grow()) {
                index = findEmptySlotWithoutEqualityCheck(hashCode);
            } else {
                index = ~index;
            }
            entries[index] = newValue;
            hashCodes[index] = hashCode;
            size++;
            return newValue;
        } else { /*existing value found*/
            var newValue = remappingFunction.apply(entries[index]);
            if(newValue == null) {
                removeFrom(index);
                return null;
            } else {
                entries[index] = newValue;
                return newValue;
            }
        }
    }

    private int findIndex(final Triple e, final int hashCode) {
        var index = calcStartIndexByHashCode(hashCode);
        while(true) {
            if(null == entries[index]) {
                return ~index;
            } else if(hashCode == hashCodes[index] && e.equals(entries[index])) {
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
        final var e = (Triple)o;
        return remove(e, e.hashCode());
    }

    public boolean remove(Triple e, int hashCode) {
        final var index = findIndex(e, hashCode);
        if (index < 0) {
            return false;
        }
        removeFrom(index);
        return true;
    }

    public void removeUnchecked(Triple e) {
        removeUnchecked(e, e.hashCode());
    }

    public void removeUnchecked(Triple e, int hashCode) {
        removeFrom(findIndex(e, hashCode));
    }

    protected void removeFrom(int here) {
        size--;
        while (true) {
            entries[here] = null;
            int scan = here;
            while (true) {
                if (--scan < 0) scan += entries.length;
                if (entries[scan] == null) return;
                int r = calcStartIndexByHashCode(hashCodes[scan]);
                if (scan <= r && r < here || r < here && here < scan || here < scan && scan <= r) { /* Nothing. We'd have preferred an `unless` statement. */} else {
                    entries[here] = entries[scan];
                    hashCodes[here] = hashCodes[scan];
                    here = scan;
                    break;
                }
            }
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
     */
    @Override
    public boolean addAll(Collection<? extends Triple> c) {
        grow(size + c.size());
        boolean modified = false;
        int index;
        int hashCode;
        for (Triple t : c) {
            if((index=findIndex(t, hashCode = t.hashCode())) < 0) {
                entries[~index] = t;
                hashCodes[~index] = hashCode;
                size++;
                modified = true;
            }
        }
        return modified;
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
    public Stream<Triple> stream() {
        final var initialSize = size;
        final Runnable checkForConcurrentModification = () ->
        {
            if (size != initialSize) throw new ConcurrentModificationException();
        };
        return StreamSupport.stream(new SparseArraySpliterator<>(entries, checkForConcurrentModification), false);
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
        final var initialSize = size;
        final Runnable checkForConcurrentModification = () ->
        {
            if (size != initialSize) throw new ConcurrentModificationException();
        };
        return StreamSupport.stream(new SparseArraySpliterator<>(entries, checkForConcurrentModification), true);
    }

}
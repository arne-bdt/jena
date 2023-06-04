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
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Queue which grows, if needed but never shrinks.
 * This queue does not guarantee any order.
 * It´s purpose is to support fast remove operations.
 */
public class FastTripleHashSet2 {

    /*Idea from hashmap: improve hash code by (h = key.hashCode()) ^ (h >>> 16)*/
    private int calcStartIndexByHashCode(final int hashCode) {
        //return (hashCode ^ (hashCode >>> 16)) & (entries.length-1);
        return hashCode & (entries.length-1);
    }

    private static int MINIMUM_HASHES_SIZE = 16;
    private static int MINIMUM_ELEMENTS_SIZE = 10;
    private static float loadFactor = 0.5f;
    protected int size = 0;
    protected Triple[] entries;
    protected int[] hashCodes;
    /*negative positions*/
    protected int[] positions;

    private final ArrayDeque<Integer> deletedIndices = new ArrayDeque<>();

    public FastTripleHashSet2(int initialSize) {
        this.positions = new int[Integer.highestOneBit(((int)(initialSize/loadFactor)+1)) << 1];
        this.entries = new Triple[initialSize];
        this.hashCodes = new int[entries.length];
    }

    public FastTripleHashSet2() {
        this.positions = new int[MINIMUM_HASHES_SIZE];
        this.entries = new Triple[MINIMUM_ELEMENTS_SIZE];
        this.hashCodes = new int[MINIMUM_ELEMENTS_SIZE];

    }

    private int calcNewPositionsSize() {
        if(size >= entries.length*loadFactor && entries.length <= 1 << 30) { /*grow*/
            final var newLength = entries.length << 1;
            return newLength < 0 ? Integer.MAX_VALUE : newLength;
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
        final var newSize = calcNewPositionsSize();
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
    public int size() {
        return size- deletedIndices.size();
    }

    private int getFreeElementIndex(){
        if(deletedIndices.isEmpty()) {
            var index = size++;
            if(index >= entries.length) {
                growEntriesAndHashCodes();
            }
            return size;
        } else {
            return deletedIndices.pop();
        }
    }

    private void growEntriesAndHashCodes() {
    }

    /**
     * Returns {@code true} if this collection contains no elements.
     *
     * @return {@code true} if this collection contains no elements
     */
    public boolean isEmpty() {
        return size == 0;
    }

    public boolean contains(Triple o) {
        final int hashCode;
        var index = calcStartIndexByHashCode(hashCode = o.hashCode());
        if(0 == positions[index]) {
            return false;
        }
        //var
        if(hashCode == hashCodes[index] && o.equals(entries[index])) {
            return true;
        } else if(--index < 0){
            index += entries.length;
        }
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


    public Iterator<Triple> iterator() {
        final var initialSize = size;
        final Runnable checkForConcurrentModification = () ->
        {
            if (size != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArrayIterator<>(entries, checkForConcurrentModification);
    }

    public boolean add(Triple value) {
        return add(value, value.hashCode());
    }

    public boolean add(Triple value, int hashCode) {
        grow();
        var pIndex = findPosition(value, hashCode);
        if(pIndex < 0) {
            var pos = ~pIndex;
            entries[~pIndex] = value;
            hashCodes[~pIndex] = hashCode;
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


    private int findPosition(final Triple e, final int hashCode) {
        var pIndex = calcStartIndexByHashCode(hashCode);
        while(true) {
            final var pos = ~positions[pIndex];
            if(0 == positions[pIndex]) {
                return ~pIndex;
            } else {
                if(hashCode == hashCodes[pos] && e.equals(entries[pos])) {
                    return pIndex;
                } else if(--pIndex < 0){
                    pIndex += positions.length;
                }
            }
        }
    }

    private int findEmptySlotWithoutEqualityCheck(final int hashCode) {
        var pIndex = calcStartIndexByHashCode(hashCode);
        while(true) {
            if(0 == positions[pIndex]) {
                return pIndex;
            } else if(--pIndex < 0){
                pIndex += positions.length;
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
    public boolean remove(Triple o) {
        return remove(o, o.hashCode());
    }

    public boolean remove(Triple e, int hashCode) {
        final var index = findPosition(e, hashCode);
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
        removeFrom(findPosition(e, hashCode));
    }

    protected Triple removeFrom(int here) {
        final int original = here;
        Triple wrappedAround = null;
        size--;
        while (true) {
            entries[here] = null;
            int scan = here;
            while (true) {
                if (--scan < 0) scan += entries.length;
                if (entries[scan] == null) return wrappedAround;
                int r = calcStartIndexByHashCode(hashCodes[scan]);
                if (scan <= r && r < here || r < here && here < scan || here < scan && scan <= r) { /* Nothing. We'd have preferred an `unless` statement. */} else {
                    if (here >= original && scan < original) {
                        wrappedAround = entries[scan];
                    }
                    entries[here] = entries[scan];
                    hashCodes[here] = hashCodes[scan];
                    here = scan;
                    break;
                }
            }
        }
    }

    /**
     * Removes all of the elements from this collection (optional operation).
     * The collection will be empty after this method returns.
     *
     * @throws UnsupportedOperationException if the {@code clear} operation
     *                                       is not supported by this collection
     */
    public void clear() {
        positions = new int[MINIMUM_HASHES_SIZE];
        entries = new Triple[MINIMUM_ELEMENTS_SIZE];
        hashCodes = new int[MINIMUM_ELEMENTS_SIZE];
        size = 0;
    }

    public Spliterator spliterator() {
        final var initialSize = size;
        final Runnable checkForConcurrentModification = () ->
        {
            if (size != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArraySpliterator<>(entries, checkForConcurrentModification);
    }

    public Stream<Triple> stream() {
        final var initialSize = size;
        final Runnable checkForConcurrentModification = () ->
        {
            if (size != initialSize) throw new ConcurrentModificationException();
        };
        return StreamSupport.stream(new SparseArraySpliterator<>(entries, checkForConcurrentModification), false);
    }

    public Stream<Triple> parallelStream() {
        final var initialSize = size;
        final Runnable checkForConcurrentModification = () ->
        {
            if (size != initialSize) throw new ConcurrentModificationException();
        };
        return StreamSupport.stream(new SparseArraySpliterator<>(entries, checkForConcurrentModification), true);
    }

}

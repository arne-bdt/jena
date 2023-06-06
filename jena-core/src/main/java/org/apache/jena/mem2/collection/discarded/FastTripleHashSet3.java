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

package org.apache.jena.mem2.collection.discarded;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.spliterator.SparseArraySubSpliterator;
import org.apache.jena.memTermEquality.SparseArrayIterator;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Queue which grows, if needed but never shrinks.
 * This queue does not guarantee any order.
 * It´s purpose is to support fast remove operations.
 */
public class FastTripleHashSet3 {

    private static final int MINIMUM_HASHES_SIZE = 16;
    private static final int MINIMUM_ELEMENTS_SIZE = 10;
    private static final float loadFactor = 0.5f;
    protected int entriesPos = 0;
    protected Triple[] entries;
    protected int[] hashCodes;
    /**
     * The negative indices to the entries and hashCode arrays.
     * The indices of the postions array are derived from the hashCodes.
     * Any postion 0 indicates an empty element.
     */
    protected int[] positions;

    public FastTripleHashSet3(int initialSize) {
        this.positions = new int[Integer.highestOneBit(((int) (initialSize / loadFactor) + 1)) << 1];
        this.entries = new Triple[initialSize];
        this.hashCodes = new int[initialSize];
    }

    public FastTripleHashSet3() {
        this.positions = new int[MINIMUM_HASHES_SIZE];
        this.entries = new Triple[MINIMUM_ELEMENTS_SIZE];
        this.hashCodes = new int[MINIMUM_ELEMENTS_SIZE];

    }

    /*Idea from hashmap: improve hash code by (h = key.hashCode()) ^ (h >>> 16)*/
    private int calcStartIndexByHashCode(final int hashCode) {
        //return (hashCode ^ (hashCode >>> 16)) & (entries.length-1);
        return hashCode & (positions.length - 1);
    }

    private int calcNewPositionsSize() {
        if (entriesPos >= positions.length * loadFactor && positions.length <= 1 << 30) { /*grow*/
            return positions.length << 1;
            //final var newLength = positions.length << 1;
            //return newLength < 0 ? Integer.MAX_VALUE : newLength;
        }
        return -1;
    }

    private void growPositionsArrayIfNeeded() {
        final var newSize = calcNewPositionsSize();
        if (newSize < 0) {
            return;
        }
        final var oldPositions = this.positions;
        this.positions = new int[newSize];
        for (int i = 0; i < oldPositions.length; i++) {
            if (0 != oldPositions[i]) {
                this.positions[findEmptySlotWithoutEqualityCheck(hashCodes[~oldPositions[i]])] = oldPositions[i];
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
    public int size() {
        return entriesPos;
    }

    private int getFreeElementIndex() {
        final var index = entriesPos++;
        if (index == entries.length) {
            growEntriesAndHashCodeArrays();
        }
        return index;
    }

    private void growEntriesAndHashCodeArrays() {
        var newSize = (entries.length >> 1) + entries.length;
        if (newSize < 0) {
            newSize = Integer.MAX_VALUE;
        }
        final var oldEntries = this.entries;
        this.entries = new Triple[newSize];
        System.arraycopy(oldEntries, 0, entries, 0, oldEntries.length);
        final var oldHashCodes = this.hashCodes;
        this.hashCodes = new int[newSize];
        System.arraycopy(oldHashCodes, 0, hashCodes, 0, oldHashCodes.length);
    }

    /**
     * Returns {@code true} if this collection contains no elements.
     *
     * @return {@code true} if this collection contains no elements
     */
    public boolean isEmpty() {
        return this.size() == 0;
    }

    public boolean contains(Triple o) {
        final int hashCode;
        var pIndex = calcStartIndexByHashCode(hashCode = o.hashCode());
        while (true) {
            if (0 == positions[pIndex]) {
                return false;
            } else {
                final var eIndex = ~positions[pIndex];
                if (hashCode == hashCodes[eIndex] && o.equals(entries[eIndex])) {
                    return true;
                } else if (--pIndex < 0) {
                    pIndex += positions.length;
                }
            }
        }
    }


    public Iterator<Triple> iterator() {
        final var initialSize = size();
        final Runnable checkForConcurrentModification = () ->
        {
            if (size() != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArrayIterator<>(entries, entriesPos, checkForConcurrentModification);
    }

    public boolean add(Triple value) {
        return add(value, value.hashCode());
    }

    public boolean add(Triple value, int hashCode) {
        growPositionsArrayIfNeeded();
        var pIndex = findPosition(value, hashCode);
        if (pIndex < 0) {
            final var eIndex = getFreeElementIndex();
            entries[eIndex] = value;
            hashCodes[eIndex] = hashCode;
            positions[~pIndex] = ~eIndex;
            return true;
        }
        return false;
    }

    public void addUnchecked(Triple value) {
        addUnchecked(value, value.hashCode());
    }

    public void addUnchecked(Triple value, int hashCode) {
        growPositionsArrayIfNeeded();
        final var eIndex = getFreeElementIndex();
        entries[eIndex] = value;
        hashCodes[eIndex] = hashCode;
        positions[findEmptySlotWithoutEqualityCheck(hashCode)] = ~eIndex;
    }


    private int findPosition(final Triple e, final int hashCode) {
        var pIndex = calcStartIndexByHashCode(hashCode);
        while (true) {
            if (0 == positions[pIndex]) {
                return ~pIndex;
            } else {
                final var pos = ~positions[pIndex];
                if (hashCode == hashCodes[pos] && e.equals(entries[pos])) {
                    return pIndex;
                } else if (--pIndex < 0) {
                    pIndex += positions.length;
                }
            }
        }
    }

    private int findEmptySlotWithoutEqualityCheck(final int hashCode) {
        var pIndex = calcStartIndexByHashCode(hashCode);
        while (true) {
            if (0 == positions[pIndex]) {
                return pIndex;
            } else if (--pIndex < 0) {
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

    protected void removeFrom(int here) {
        final var pIndex = ~positions[here];
        entriesPos--;
        if (entriesPos != pIndex) { /*swap entries*/
            final var there = findPosition(entries[entriesPos], hashCodes[entriesPos]);
            entries[pIndex] = entries[entriesPos];
            hashCodes[pIndex] = hashCodes[entriesPos];
            positions[there] = ~pIndex;
        }
        entries[entriesPos] = null;
        while (true) {
            positions[here] = 0;
            int scan = here;
            while (true) {
                if (--scan < 0) scan += positions.length;
                if (positions[scan] == 0) return;
                int r = calcStartIndexByHashCode(hashCodes[~positions[scan]]);
                if (scan <= r && r < here || r < here && here < scan || here < scan && scan <= r) { /* Nothing. We'd have preferred an `unless` statement. */} else {
                    positions[here] = positions[scan];
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
        entriesPos = 0;
    }

    public Spliterator spliterator() {
        final var initialSize = this.size();
        final Runnable checkForConcurrentModification = () ->
        {
            if (this.size() != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArraySubSpliterator<>(entries, 0, entriesPos, checkForConcurrentModification);
    }

    public Stream<Triple> stream() {
        final var initialSize = this.size();
        final Runnable checkForConcurrentModification = () ->
        {
            if (this.size() != initialSize) throw new ConcurrentModificationException();
        };
        return StreamSupport.stream(new SparseArraySubSpliterator<>(entries, 0, entriesPos, checkForConcurrentModification), false);
    }

    public Stream<Triple> parallelStream() {
        final var initialSize = this.size();
        final Runnable checkForConcurrentModification = () ->
        {
            if (this.size() != initialSize) throw new ConcurrentModificationException();
        };
        return StreamSupport.stream(new SparseArraySubSpliterator<>(entries, 0, entriesPos, checkForConcurrentModification), true);
    }

}

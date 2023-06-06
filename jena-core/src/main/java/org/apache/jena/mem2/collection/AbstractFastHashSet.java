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

import org.apache.jena.mem2.spliterator.SparseArraySubSpliterator;
import org.apache.jena.memTermEquality.SparseArrayIterator;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Set which grows, if needed but never shrinks.
 * This set does not guarantee any order.
 * This set does not allow null values.
 * This set is not thread safe.
 * It´s purpose is to support fast add, remove, contains and stream / iterate operations.
 * Only remove operations are not as fast as in {@link:java.util.HashSet}
 * Iterating over this set not get much faster again after removing elements.
 */
public abstract class AbstractFastHashSet<E> {

    private static final int MINIMUM_HASHES_SIZE = 16;
    private static final int MINIMUM_ELEMENTS_SIZE = 10;
    private static final float loadFactor = 0.5f;
    protected int entriesPos = 0;
    protected E[] entries;
    protected int[] hashCodesOrDeletedIndices;
    protected int lastDeletedIndex = -1;
    protected int removedElementsCount = 0;
    /**
     * The negative indices to the entries and hashCode arrays.
     * The indices of the postions array are derived from the hashCodes.
     * Any postion 0 indicates an empty element.
     */
    protected int[] positions;

    protected abstract E[] createEntriesArray(int size);

    public AbstractFastHashSet(int initialSize) {
        this.positions = new int[Integer.highestOneBit(((int) (initialSize / loadFactor) + 1)) << 1];
        this.entries = createEntriesArray(initialSize);
        this.hashCodesOrDeletedIndices = new int[initialSize];
    }

    public AbstractFastHashSet() {
        this.positions = new int[MINIMUM_HASHES_SIZE];
        this.entries = createEntriesArray(MINIMUM_ELEMENTS_SIZE);
        this.hashCodesOrDeletedIndices = new int[MINIMUM_ELEMENTS_SIZE];

    }

    private int calcStartIndexByHashCode(final int hashCode) {
        return hashCode & (positions.length - 1);
    }

    private int calcNewPositionsSize() {
        if (entriesPos >= positions.length * loadFactor && positions.length <= 1 << 30) { /*grow*/
            //return positions.length << 1;
            final var newLength = positions.length << 1;
            return newLength < 0 ? Integer.MAX_VALUE : newLength;
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
                this.positions[findEmptySlotWithoutEqualityCheck(hashCodesOrDeletedIndices[~oldPositions[i]])] = oldPositions[i];
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
        return entriesPos - removedElementsCount;
    }

    private int getFreeElementIndex() {
        if (lastDeletedIndex == -1) {
            final var index = entriesPos++;
            if (index == entries.length) {
                growEntriesAndHashCodeArrays();
            }
            return index;
        } else {
            final var index = lastDeletedIndex;
            lastDeletedIndex = hashCodesOrDeletedIndices[lastDeletedIndex];
            removedElementsCount--;
            return index;
        }
    }

    private void growEntriesAndHashCodeArrays() {
        var newSize = (entries.length >> 1) + entries.length;
        if (newSize < 0) {
            newSize = Integer.MAX_VALUE;
        }
        final var oldEntries = this.entries;
        this.entries = createEntriesArray(newSize);
        System.arraycopy(oldEntries, 0, entries, 0, oldEntries.length);
        final var oldHashCodes = this.hashCodesOrDeletedIndices;
        this.hashCodesOrDeletedIndices = new int[newSize];
        System.arraycopy(oldHashCodes, 0, hashCodesOrDeletedIndices, 0, oldHashCodes.length);
    }

    /**
     * Returns {@code true} if this collection contains no elements.
     *
     * @return {@code true} if this collection contains no elements
     */
    public boolean isEmpty() {
        return this.size() == 0;
    }

    public boolean contains(E o) {
        final int hashCode;
        var pIndex = calcStartIndexByHashCode(hashCode = o.hashCode());
        while (true) {
            if (0 == positions[pIndex]) {
                return false;
            } else {
                final var eIndex = ~positions[pIndex];
                if (hashCode == hashCodesOrDeletedIndices[eIndex] && o.equals(entries[eIndex])) {
                    return true;
                } else if (--pIndex < 0) {
                    pIndex += positions.length;
                }
            }
        }
    }


    public Iterator<E> iterator() {
        final var initialSize = size();
        final Runnable checkForConcurrentModification = () ->
        {
            if (size() != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArrayIterator<>(entries, entriesPos, checkForConcurrentModification);
    }

    public boolean add(E value) {
        return add(value, value.hashCode());
    }

    public boolean add(E value, int hashCode) {
        growPositionsArrayIfNeeded();
        var pIndex = findPosition(value, hashCode);
        if (pIndex < 0) {
            final var eIndex = getFreeElementIndex();
            entries[eIndex] = value;
            hashCodesOrDeletedIndices[eIndex] = hashCode;
            positions[~pIndex] = ~eIndex;
            return true;
        }
        return false;
    }

    public void addUnchecked(E value) {
        addUnchecked(value, value.hashCode());
    }

    public void addUnchecked(E value, int hashCode) {
        growPositionsArrayIfNeeded();
        final var eIndex = getFreeElementIndex();
        entries[eIndex] = value;
        hashCodesOrDeletedIndices[eIndex] = hashCode;
        positions[findEmptySlotWithoutEqualityCheck(hashCode)] = ~eIndex;
    }


    private int findPosition(final E e, final int hashCode) {
        var pIndex = calcStartIndexByHashCode(hashCode);
        while (true) {
            if (0 == positions[pIndex]) {
                return ~pIndex;
            } else {
                final var pos = ~positions[pIndex];
                if (hashCode == hashCodesOrDeletedIndices[pos] && e.equals(entries[pos])) {
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
    public boolean remove(E o) {
        return remove(o, o.hashCode());
    }

    public boolean remove(E e, int hashCode) {
        final var index = findPosition(e, hashCode);
        if (index < 0) {
            return false;
        }
        removeFrom(index);
        return true;
    }

    public void removeUnchecked(E e) {
        removeUnchecked(e, e.hashCode());
    }

    public void removeUnchecked(E e, int hashCode) {
        removeFrom(findPosition(e, hashCode));
    }

    protected void removeFrom(int here) {
        final var pIndex = ~positions[here];
        hashCodesOrDeletedIndices[pIndex] = lastDeletedIndex;
        lastDeletedIndex = pIndex;
        removedElementsCount++;
        entries[pIndex] = null;
        while (true) {
            positions[here] = 0;
            int scan = here;
            while (true) {
                if (--scan < 0) scan += positions.length;
                if (positions[scan] == 0) return;
                int r = calcStartIndexByHashCode(hashCodesOrDeletedIndices[~positions[scan]]);
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
        entries = createEntriesArray(MINIMUM_ELEMENTS_SIZE);
        hashCodesOrDeletedIndices = new int[MINIMUM_ELEMENTS_SIZE];
        entriesPos = 0;
        lastDeletedIndex = -1;
        removedElementsCount = 0;
    }

    public Spliterator<E> spliterator() {
        final var initialSize = this.size();
        final Runnable checkForConcurrentModification = () ->
        {
            if (this.size() != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArraySubSpliterator<>(entries, 0, entriesPos, checkForConcurrentModification);
    }

    public Stream<E> stream() {
        return StreamSupport.stream(this.spliterator(), false);
    }

    public Stream<E> parallelStream() {
        return StreamSupport.stream(this.spliterator(), true);
    }

}

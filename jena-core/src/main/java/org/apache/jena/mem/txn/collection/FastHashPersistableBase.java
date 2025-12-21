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
package org.apache.jena.mem.txn.collection;

import org.apache.jena.mem.collection.JenaMapSetCommon;
import org.apache.jena.mem.txn.iterator.SparseArrayIterator;
import org.apache.jena.mem.txn.spliterator.SparseArraySpliterator;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.ConcurrentModificationException;
import java.util.Spliterator;
import java.util.function.Predicate;


public abstract class FastHashPersistableBase<K> implements JenaMapSetCommon<K> {
    protected static final int MINIMUM_HASHES_SIZE = 16;
    protected static final int MINIMUM_ELEMENTS_SIZE = 8;
    protected int keysPos = 0;
    protected K[] keys;
    protected int[] hashCodesOrDeletedIndices;
    protected boolean[] deleted;
    protected int lastDeletedIndex = -1;
    protected int removedKeysCount = 0;

    protected int[] positions;

    protected FastHashPersistableBase(int initialSize) {
        if(initialSize < 2) {
            initialSize = 2;
        }
        var positionsSize = getNextHigherBase2(initialSize);
        this.positions = new int[positionsSize];
        this.keys = newKeysArray(initialSize);
        this.hashCodesOrDeletedIndices = new int[initialSize];
        this.deleted = new boolean[initialSize];
    }

    private static int getNextHigherBase2(int initialSize) {
        var size = Integer.highestOneBit(initialSize << 1);
        if (size < initialSize << 1) {
            size <<= 1;
        }
        return size;
    }

    protected FastHashPersistableBase() {
        this.positions = new int[MINIMUM_HASHES_SIZE];
        this.keys = newKeysArray(MINIMUM_ELEMENTS_SIZE);
        this.hashCodesOrDeletedIndices = new int[MINIMUM_ELEMENTS_SIZE];
        this.deleted = new boolean[MINIMUM_ELEMENTS_SIZE];
    }

    protected <T extends FastHashPersistableBase<K>> FastHashPersistableBase(final T base, boolean createImmutableChild)  {
        this.positions = new int[base.positions.length];
        this.deleted = new boolean[base.deleted.length];

        System.arraycopy(base.positions, 0, this.positions, 0, base.positions.length);
        System.arraycopy(base.deleted, 0, this.deleted, 0, base.deleted.length);

        if(createImmutableChild) {
            this.keys = base.keys;
            this.hashCodesOrDeletedIndices = base.hashCodesOrDeletedIndices;
        } else {
            this.keys = newKeysArray(base.keys.length);
            this.hashCodesOrDeletedIndices = new int[base.hashCodesOrDeletedIndices.length];

            System.arraycopy(base.keys, 0, this.keys, 0, base.keys.length);
            System.arraycopy(base.hashCodesOrDeletedIndices, 0, this.hashCodesOrDeletedIndices, 0, base.hashCodesOrDeletedIndices.length);
        }

        this.keysPos = base.keysPos;
        this.lastDeletedIndex = base.lastDeletedIndex;
        this.removedKeysCount = base.removedKeysCount;
    }

    /**
     * Gets a new array of keys with the given size.
     *
     * @param size the size of the array
     * @return the new array
     */
    protected static <E> E[] newKeysArray(int size) {
        @SuppressWarnings("unchecked")
        final E[] newArray = (E[]) new Object[size];
        return newArray;
    }

    /**
     * Calculates a position in the positions array by the hashCode.
     *
     * @param hashCode the hashCode
     * @return the start index in the positions array to search for the key
     */
    protected final int calcStartIndexByHashCode(final int hashCode) {
        return hashCode & (positions.length - 1);
    }

    /**
     * Calculates the new size of the positions array, if it needs to be grown.
     *
     * @return the new size or -1 if it does not need to be grown
     */
    private int calcNewPositionsSize() {
        var newSize = getNextHigherBase2(this.size());
        if (newSize < 0) {
            return Integer.MAX_VALUE;
        }
        if (newSize > positions.length) { /*grow*/
            return newSize;
        }
        return -1;
    }

    /**
     * Grow the positions array if needed.
     *
     * @return true if the positions array was grown
     */
    protected final boolean tryGrowPositionsArrayIfNeeded() {
        final var newSize = calcNewPositionsSize();
        if (newSize < 0) {
            return false;
        }
        final var oldPositions = this.positions;
        this.positions = new int[newSize];
        for (int oldPosition : oldPositions) {
            if (0 != oldPosition) {
                this.positions[findEmptySlotWithoutEqualityCheck(hashCodesOrDeletedIndices[~oldPosition])] = oldPosition;
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
        return keysPos - removedKeysCount;
    }

    /**
     * Finds the next free slot in the keys array.
     * If the keys array needs to be grown, it is grown.
     * If there are deleted keys, the index of the last deleted key is returned.
     *
     * @return the index of the next free slot
     */
    protected final int getFreeKeyIndex() {
        final int index;
        if (lastDeletedIndex == -1) {
            index = keysPos++;
            if (index == keys.length) {
                keysPos--;
                growKeysAndHashCodeArrays();
                return getFreeKeyIndex();
            }
        } else {
            index = lastDeletedIndex;
            lastDeletedIndex = hashCodesOrDeletedIndices[lastDeletedIndex];
            deleted[index] = false;
            removedKeysCount--;
        }
        return index;
    }

    /**
     * Grow the keys and hashCodes arrays.
     */
    protected void growKeysAndHashCodeArrays() {
        final var actualSize = this.size();
        var newSize = (actualSize >> 1) + actualSize;
        if (newSize < 0) {
            newSize = Integer.MAX_VALUE;
        }
        if (newSize < keys.length) {
            newSize = keys.length;
        }
        final var oldKeys = this.keys;
        final var oldHashCodes = this.hashCodesOrDeletedIndices;
        final var oldDeleted = this.deleted;

        this.keys = newKeysArray(newSize);
        this.hashCodesOrDeletedIndices = new int[newSize];
        this.deleted = new boolean[newSize];

        System.arraycopy(oldKeys, 0, keys, 0, oldKeys.length);
        System.arraycopy(oldHashCodes, 0, hashCodesOrDeletedIndices, 0, oldHashCodes.length);
        System.arraycopy(oldDeleted, 0, deleted, 0, oldDeleted.length);

        afterGrowKeysAndHashCodeArraysProcessDeletedIndices(oldDeleted);
    }

    protected void afterGrowKeysAndHashCodeArraysProcessDeletedIndices(final boolean[] oldDeleted) {
        lastDeletedIndex = -1;
        removedKeysCount = 0;
        var deletedIndex = oldDeleted.length;
        while (0 < deletedIndex--) {
            if (oldDeleted[deletedIndex]) {
                keys[deletedIndex] = null;
                hashCodesOrDeletedIndices[deletedIndex] = lastDeletedIndex;
                lastDeletedIndex = deletedIndex;
                removedKeysCount++;
                break;
            }
        }
    }

    /**
     * Gets the key at the given index.
     *
     * @param i the index
     * @return the key at the given index
     */
    public final K getKeyAt(int i) {
        return keys[i];
    }

    @Override
    public boolean tryRemove(K o) {
        return tryRemove(o, o.hashCode());
    }

    public boolean tryRemove(K e, int hashCode) {
        final var index = findPosition(e, hashCode);
        if (index < 0) {
            return false;
        }
        removeFrom(index);
        return true;
    }

    /**
     * Removes the element at the given position.
     *
     * @param e the element
     * @return the index of the removed element or -1 if the element was not found
     */
    public int removeAndGetIndex(final K e) {
        return removeAndGetIndex(e, e.hashCode());
    }

    /**
     * Removes the element at the given position.
     *
     * @param e        the element
     * @param hashCode the hash code of the element. This is a performance optimization.
     * @return the index of the removed element or -1 if the element was not found
     */
    public int removeAndGetIndex(final K e, final int hashCode) {
        final var pIndex = findPosition(e, hashCode);
        if (pIndex < 0) {
            return -1;
        }
        final var eIndex = ~positions[pIndex];
        removeFrom(pIndex);
        return eIndex;
    }

    @Override
    public void removeUnchecked(K e) {
        removeUnchecked(e, e.hashCode());
    }

    public void removeUnchecked(K e, int hashCode) {
        removeFrom(findPosition(e, hashCode));
    }

    /**
     * Removes the element at the given position.
     * <p>
     * This is an implementation of Knuth's Algorithm R from tAoCP vol3, p 527,
     * with exchanging of the roles of i and j so that they can be usefully renamed
     * to <i>here</i> and <i>scan</i>.
     * <p>
     * It relies on linear probing but doesn't require a distinguished REMOVED
     * value. Since we resize the table when it gets fullish, we don't worry [much]
     * about the overhead of the linear probing.
     * <p>
     *
     * @param here the index in the positions array
     */
    protected final void removeFrom(int here) {
        final var pIndex = ~positions[here];
        deleted[pIndex] = true;
        removedKeysCount++;
        while (true) {
            positions[here] = 0;
            int scan = here;
            while (true) {
                if (--scan < 0) scan += positions.length;
                if (positions[scan] == 0) return;
                int r = calcStartIndexByHashCode(hashCodesOrDeletedIndices[~positions[scan]]);
                if ((scan > r || r >= here) && (r >= here || here >= scan) && (here >= scan || scan > r)) {
                    positions[here] = positions[scan];
                    here = scan;
                    break;
                }
            }
        }
    }

    /**
     * Returns {@code true} if this collection contains no elements.
     *
     * @return {@code true} if this collection contains no elements
     */
    @Override
    public final boolean isEmpty() {
        return this.size() == 0;
    }

    @Override
    public final boolean containsKey(K o) {
        final int hashCode = o.hashCode();
        var pIndex = calcStartIndexByHashCode(hashCode);
        while (true) {
            if (0 == positions[pIndex]) {
                return false;
            } else {
                final var eIndex = ~positions[pIndex];
                if (hashCode == hashCodesOrDeletedIndices[eIndex] && o.equals(keys[eIndex])) {
                    return true;
                } else if (--pIndex < 0) {
                    pIndex += positions.length;
                }
            }
        }
    }

    /**
     * Attentions: Due to the ordering of the keys, this method may be slow
     * if matching elements are at the start of the list.
     * Try to use {@link #anyMatchRandomOrder(Predicate)} instead.
     */
    @Override
    public final boolean anyMatch(Predicate<K> predicate) {
        var pos = keysPos - 1;
        while (-1 < pos) {
            if (!deleted[pos] && predicate.test(keys[pos])) {
                return true;
            }
            pos--;
        }
        return false;
    }

    /**
     * This method can be faster than {@link #anyMatch(Predicate)} if one expects
     * to find many matches. But it is slower if one expects to find no matches or just a single one.
     *
     * @param predicate the predicate to apply to elements of this collection
     * @return {@code true} if any element of the collection matches the predicate
     */
    public final boolean anyMatchRandomOrder(Predicate<K> predicate) {
        var pIndex = positions.length - 1;
        while (-1 < pIndex) {
            if (0 != positions[pIndex] && predicate.test(keys[~positions[pIndex]])) {
                return true;
            }
            pIndex--;
        }
        return false;
    }

    @Override
    public final ExtendedIterator<K> keyIterator() {
        final var initialSize = size();
        final Runnable checkForConcurrentModification = () ->
        {
            if (size() != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArrayIterator<>(keys, deleted, keysPos, checkForConcurrentModification);
    }

    public final int findPosition(final K e, final int hashCode) {
        var pIndex = calcStartIndexByHashCode(hashCode);
        while (true) {
            if (0 == positions[pIndex]) {
                return ~pIndex;
            } else {
                final var pos = ~positions[pIndex];
                if (hashCode == hashCodesOrDeletedIndices[pos] && e.equals(keys[pos])) {
                    return pIndex;
                } else if (--pIndex < 0) {
                    pIndex += positions.length;
                }
            }
        }
    }

    protected final int findEmptySlotWithoutEqualityCheck(final int hashCode) {
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
     * Removes all the elements from this collection (optional operation).
     * The collection will be empty after this method returns.
     *
     * @throws UnsupportedOperationException if the {@code clear} operation
     *                                       is not supported by this collection
     */
    @Override
    public void clear() {
        positions = new int[MINIMUM_HASHES_SIZE];
        keys = newKeysArray(MINIMUM_ELEMENTS_SIZE);
        hashCodesOrDeletedIndices = new int[MINIMUM_ELEMENTS_SIZE];
        deleted = new boolean[MINIMUM_ELEMENTS_SIZE];
        keysPos = 0;
        removedKeysCount = 0;
    }

    @Override
    public final Spliterator<K> keySpliterator() {
        final var initialSize = this.size();
        final Runnable checkForConcurrentModification = () ->
        {
            if (this.size() != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArraySpliterator<>(keys, deleted, keysPos, checkForConcurrentModification);
    }
}

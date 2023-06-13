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

import org.apache.jena.mem2.iterator.SparseArrayIterator;
import org.apache.jena.mem2.spliterator.SparseArraySubMappingSpliterator;
import org.apache.jena.mem2.spliterator.SparseArraySubSpliterator;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.ConcurrentModificationException;
import java.util.Spliterator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Map which grows, if needed but never shrinks.
 * This map does not guarantee any order.
 * This map does not allow null values.
 * This map is not thread safe.
 * It´s purpose is to support fast add, remove, contains and stream / iterate operations.
 * Only remove operations are not as fast as in {@link:java.util.HashMap}
 * Iterating over this map does not get much faster again after removing elements.
 */
public abstract class FastPseudoMap<K, V> implements JenaMap<K, V> {

    protected static final int MINIMUM_HASHES_SIZE = 16;
    protected static final int MINIMUM_ELEMENTS_SIZE = 10;
    protected int valuePos = 0;
    protected V[] values;
    protected int[] hashCodesOrDeletedIndices;
    protected int lastDeletedIndex = -1;
    protected int removedKeysCount = 0;

    /**
     * The negative indices to the entries and hashCode arrays.
     * The indices of the positions array are derived from the hashCodes.
     * Any position 0 indicates an empty element.
     */
    protected int[] positions;

    protected FastPseudoMap(int initialSize) {
        var positionsSize = Integer.highestOneBit(initialSize << 1);
        if (positionsSize < initialSize << 1) {
            positionsSize <<= 1;
        }
        this.positions = new int[positionsSize];
        this.values = newValuesArray(initialSize);
        this.hashCodesOrDeletedIndices = new int[initialSize];
    }

    protected FastPseudoMap() {
        this.positions = new int[MINIMUM_HASHES_SIZE];
        this.values = newValuesArray(MINIMUM_ELEMENTS_SIZE);
        this.hashCodesOrDeletedIndices = new int[MINIMUM_ELEMENTS_SIZE];

    }

    protected abstract V[] newValuesArray(int size);

    protected final int calcStartIndexByHashCode(final int hashCode) {
        return hashCode & (positions.length - 1);
    }

    private int calcNewPositionsSize() {
        if (valuePos << 1 > positions.length) { /*grow*/
            final var newLength = positions.length << 1;
            return newLength < 0 ? Integer.MAX_VALUE : newLength;
        }
        return -1;
    }

    protected final void growPositionsArrayIfNeeded() {
        final var newSize = calcNewPositionsSize();
        if (newSize < 0) {
            return;
        }
        final var oldPositions = this.positions;
        this.positions = new int[newSize];
        for (int oldPosition : oldPositions) {
            if (0 != oldPosition) {
                this.positions[findEmptySlotWithoutEqualityCheck(hashCodesOrDeletedIndices[~oldPosition])] = oldPosition;
            }
        }
    }

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
        return valuePos - removedKeysCount;
    }

    protected final int getFreeKeyIndex() {
        final int index;
        if (lastDeletedIndex == -1) {
            index = valuePos++;
            if (index == values.length) {
                growKeysAndHashCodeArrays();
            }
        } else {
            index = lastDeletedIndex;
            lastDeletedIndex = hashCodesOrDeletedIndices[lastDeletedIndex];
            removedKeysCount--;
        }
        return index;
    }

    protected void growKeysAndHashCodeArrays() {
        var newSize = (values.length >> 1) + values.length;
        if (newSize < 0) {
            newSize = Integer.MAX_VALUE;
        }
        final var oldValues = this.values;
        this.values = newValuesArray(newSize);
        System.arraycopy(oldValues, 0, values, 0, oldValues.length);
        final var oldHashCodes = this.hashCodesOrDeletedIndices;
        this.hashCodesOrDeletedIndices = new int[newSize];
        System.arraycopy(oldHashCodes, 0, hashCodesOrDeletedIndices, 0, oldHashCodes.length);
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
    public final boolean tryRemove(K o) {
        return tryRemove(o, o.hashCode());
    }

    public final boolean tryRemove(K e, int hashCode) {
        final var index = findPosition(e, hashCode);
        if (index < 0) {
            return false;
        }
        removeFrom(index);
        return true;
    }

    /**
     * Removes the given element and returns its index.
     * If the element is not found, returns -1.
     */
    public final int removeAndGetIndex(final K e) {
        return removeAndGetIndex(e, e.hashCode());
    }

    public final int removeAndGetIndex(final K e, final int hashCode) {
        final var pIndex = findPosition(e, hashCode);
        if (pIndex < 0) {
            return -1;
        }
        final var eIndex = ~positions[pIndex];
        removeFrom(pIndex);
        return eIndex;
    }

    @Override
    public final void removeUnchecked(K e) {
        removeUnchecked(e, e.hashCode());
    }

    public final void removeUnchecked(K e, int hashCode) {
        removeFrom(findPosition(e, hashCode));
    }

    protected void removeFrom(int here) {
        final var pIndex = ~positions[here];
        hashCodesOrDeletedIndices[pIndex] = lastDeletedIndex;
        lastDeletedIndex = pIndex;
        removedKeysCount++;
        values[pIndex] = null;
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
                if (hashCode == hashCodesOrDeletedIndices[eIndex] && o.equals(getKey(values[eIndex]))) {
                    return true;
                } else if (--pIndex < 0) {
                    pIndex += positions.length;
                }
            }
        }
    }

    @Override
    public final boolean anyMatch(Predicate<K> predicate) {
        var pos = valuePos - 1;
        while (-1 < pos) {
            if (null != values[pos] && predicate.test(getKey(values[pos]))) {
                return true;
            }
            pos--;
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
        return new SparseArrayIterator<>(values, valuePos, checkForConcurrentModification).mapWith(this::getKey);
    }

    protected final int findPosition(final K e, final int hashCode) {
        var pIndex = calcStartIndexByHashCode(hashCode);
        while (true) {
            if (0 == positions[pIndex]) {
                return ~pIndex;
            } else {
                final var pos = ~positions[pIndex];
                if (hashCode == hashCodesOrDeletedIndices[pos] && e.equals(getKey(values[pos]))) {
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
        values = newValuesArray(MINIMUM_ELEMENTS_SIZE);
        hashCodesOrDeletedIndices = new int[MINIMUM_ELEMENTS_SIZE];
        valuePos = 0;
        lastDeletedIndex = -1;
        removedKeysCount = 0;
    }

    @Override
    public final Spliterator<K> keySpliterator() {
        final var initialSize = this.size();
        final Runnable checkForConcurrentModification = () ->
        {
            if (this.size() != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArraySubMappingSpliterator<>(values, 0, valuePos, this::getKey, checkForConcurrentModification);
    }

    protected abstract K getKey(final V value);

    @Override
    public boolean tryPut(K key, V value) {
        final var hashCode = key.hashCode();
        var pIndex = findPosition(key, hashCode);
        if (pIndex < 0) {
            if (tryGrowPositionsArrayIfNeeded()) {
                pIndex = findPosition(key, hashCode);
            }
            final var eIndex = getFreeKeyIndex();
            values[eIndex] = value;
            hashCodesOrDeletedIndices[eIndex] = hashCode;
            positions[~pIndex] = ~eIndex;
            return true;
        } else {
            values[~positions[pIndex]] = value;
            return false;
        }
    }

    @Override
    public void put(K key, V value) {
        final var hashCode = key.hashCode();
        var pIndex = findPosition(key, hashCode);
        if (pIndex < 0) {
            if (tryGrowPositionsArrayIfNeeded()) {
                pIndex = findPosition(key, hashCode);
            }
            final var eIndex = getFreeKeyIndex();
            values[eIndex] = value;
            hashCodesOrDeletedIndices[eIndex] = hashCode;
            positions[~pIndex] = ~eIndex;
        } else {
            values[~positions[pIndex]] = value;
        }
    }

    public V getValueAt(int i) {
        return values[i];
    }

    @Override
    public V get(K key) {
        var pIndex = findPosition(key, key.hashCode());
        if (pIndex < 0) {
            return null;
        } else {
            return values[~positions[pIndex]];
        }
    }

    @Override
    public V getOrDefault(K key, V defaultValue) {
        var pIndex = findPosition(key, key.hashCode());
        if (pIndex < 0) {
            return defaultValue;
        } else {
            return values[~positions[pIndex]];
        }
    }

    @Override
    public V computeIfAbsent(K key, Supplier<V> absentValueSupplier) {
        final var hashCode = key.hashCode();
        var pIndex = findPosition(key, hashCode);
        if (pIndex < 0) {
            if (tryGrowPositionsArrayIfNeeded()) {
                pIndex = findPosition(key, hashCode);
            }
            final var eIndex = getFreeKeyIndex();
            hashCodesOrDeletedIndices[eIndex] = hashCode;
            final var value = absentValueSupplier.get();
            values[eIndex] = value;
            positions[~pIndex] = ~eIndex;
            return value;
        } else {
            return values[~positions[pIndex]];
        }
    }

    @Override
    public void compute(K key, Function<V, V> valueProcessor) {
        final int hashCode = key.hashCode();
        var pIndex = findPosition(key, hashCode);
        if (pIndex < 0) {
            final var value = valueProcessor.apply(null);
            if (value == null)
                return;
            final var eIndex = getFreeKeyIndex();
            hashCodesOrDeletedIndices[eIndex] = hashCode;
            values[eIndex] = value;
            positions[~pIndex] = ~eIndex;
            tryGrowPositionsArrayIfNeeded();
        } else {
            var eIndex = ~positions[pIndex];
            final var value = valueProcessor.apply(values[eIndex]);
            if (value == null) {
                removeFrom(pIndex);
            } else {
                values[eIndex] = value;
            }
        }
    }


    @Override
    public ExtendedIterator<V> valueIterator() {
        final var initialSize = size();
        final Runnable checkForConcurrentModification = () ->
        {
            if (size() != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArrayIterator<>(values, valuePos, checkForConcurrentModification);
    }

    @Override
    public Spliterator<V> valueSpliterator() {
        final var initialSize = this.size();
        final Runnable checkForConcurrentModification = () ->
        {
            if (this.size() != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArraySubSpliterator<>(values, 0, valuePos, checkForConcurrentModification);
    }
}

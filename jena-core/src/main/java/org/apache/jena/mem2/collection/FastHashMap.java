/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *   SPDX-License-Identifier: Apache-2.0
 */

package org.apache.jena.mem2.collection;

import org.apache.jena.mem2.iterator.SparseArrayIterator;
import org.apache.jena.mem2.spliterator.SparseArraySpliterator;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.ConcurrentModificationException;
import java.util.Spliterator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * Map which grows, if needed but never shrinks.
 * This map does not guarantee any order. Although due to the way it is implemented the elements have a certain order.
 * This map does not allow null keys.
 * This map is not thread safe.
 * It´s purpose is to support fast add, remove, contains and stream / iterate operations.
 * Only remove operations are not as fast as in {@link java.util.HashMap}
 * Iterating over this map does not get much faster again after removing elements because the map is not compacted.
 */
public class FastHashMap<K, V> extends FastHashBase<K> implements JenaMapOptimized<K, V> {

    protected V[] values;

    public FastHashMap(int initialSize) {
        super(initialSize);
        //noinspection unchecked
        this.values = (V[])new Object[keys.length];
    }

    public FastHashMap() {
        super();
        //noinspection unchecked
        this.values = (V[])new Object[keys.length];
    }

    /**
     * Copy constructor.
     * The new map will contain all the same keys and values of the map to copy.
     *
     * @param mapToCopy
     */
    public FastHashMap(final FastHashMap<K, V> mapToCopy) {
        super(mapToCopy);
        //noinspection unchecked
        this.values = (V[])new Object[keys.length];
        System.arraycopy(mapToCopy.values, 0, this.values, 0, mapToCopy.values.length);
    }

    /**
     * Copy constructor with value processor.
     *
     * @param mapToCopy
     * @param valueProcessor
     */
    public FastHashMap(final FastHashMap<K, V> mapToCopy, final UnaryOperator<V> valueProcessor) {
        super(mapToCopy);
        //noinspection unchecked
        this.values = (V[])new Object[keys.length];
        for (int i = 0; i < mapToCopy.values.length; i++) {
            final var value = mapToCopy.values[i];
            if (value != null) {
                this.values[i] = valueProcessor.apply(value);
            }
        }
    }


    @Override
    protected void growKeysAndHashCodeArrays() {
        super.growKeysAndHashCodeArrays();
        final var oldValues = values;
        //noinspection unchecked
        values = (V[])new Object[keys.length];
        System.arraycopy(oldValues, 0, values, 0, oldValues.length);
    }

    @Override
    public void removeAt(int index) {
        values[index] = null;
        super.removeAt(index);
    }

    @Override
    public void clear() {
        super.clear();
        //noinspection unchecked
        this.values = (V[])new Object[keys.length];
    }

    @Override
    public boolean tryPut(K key,  V value) {
        return tryPut(key, key.hashCode(), value);
    }

    @Override
    public boolean tryPut(K key, int hashCode, V value) {
        var pIndex = findPosition(key, hashCode);
        if (pIndex < 0) {
            if (tryGrowPositionsArrayIfNeeded()) {
                pIndex = findPosition(key, hashCode);
            }
            final var eIndex = getFreeKeyIndex();
            keys[eIndex] = key;
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
    public int putAndGetIndex(K key, V value) {
        final int hashCode = key.hashCode();
        var pIndex = findPosition(key, hashCode);
        if (pIndex < 0) {
            if (tryGrowPositionsArrayIfNeeded()) {
                pIndex = findPosition(key, hashCode);
            }
            final var eIndex = getFreeKeyIndex();
            keys[eIndex] = key;
            values[eIndex] = value;
            hashCodesOrDeletedIndices[eIndex] = hashCode;
            positions[~pIndex] = ~eIndex;
            return eIndex;
        } else {
            final var eIndex = ~positions[pIndex];
            values[eIndex] = value;
            return eIndex;
        }
    }

    @Override
    public void put(K key, V value) {
        put(key, key.hashCode(), value);
    }

//    @Override
//    public void putAt(int index, K key, V value) {
//        if (index < 0) {
//            final var hashCode = key.hashCode();
//            if (tryGrowPositionsArrayIfNeeded()) {
//                index = findPosition(key, hashCode);
//            }
//            final var eIndex = getFreeKeyIndex();
//            keys[eIndex] = key;
//            values[eIndex] = value;
//            hashCodesOrDeletedIndices[eIndex] = hashCode;
//            positions[~index] = ~eIndex;
//        } else {
//            values[~positions[index]] = value;
//        }
//    }

    @Override
    public void put(K key, int hashCode, V value) {
        var pIndex = findPosition(key, hashCode);
        if (pIndex < 0) {
            if (tryGrowPositionsArrayIfNeeded()) {
                pIndex = findPosition(key, hashCode);
            }
            final var eIndex = getFreeKeyIndex();
            keys[eIndex] = key;
            values[eIndex] = value;
            hashCodesOrDeletedIndices[eIndex] = hashCode;
            positions[~pIndex] = ~eIndex;
        } else {
            values[~positions[pIndex]] = value;
        }
    }

    /**
     * Returns the value at the given index.
     *
     * @param i index
     * @return value
     */
    public V getValueAt(int i) {
        return values[i];
    }

    @Override
    public V get(K key) {
        return get(key, key.hashCode());
    }

    @Override
    public V get(K key, int hashCode) {
        final var pIndex = findPosition(key, hashCode);
        if (pIndex < 0) {
            return null;
        } else {
            return values[~positions[pIndex]];
        }
    }

    @Override
    public V getOrDefault(K key, V defaultValue) {
        return getOrDefault(key, key.hashCode(), defaultValue);
    }

    @Override
    public V getOrDefault(K key, int hashCode, V defaultValue) {
        var pIndex = findPosition(key, hashCode);
        if (pIndex < 0) {
            return defaultValue;
        } else {
            return values[~positions[pIndex]];
        }
    }

    @Override
    public V computeIfAbsent(K key, Supplier<V> absentValueSupplier) {
        return computeIfAbsent(key, key.hashCode(), absentValueSupplier);
    }

    @Override
    public V computeIfAbsent(K key, int hashCode, Supplier<V> absentValueSupplier) {
        var pIndex = findPosition(key, hashCode);
        if (pIndex < 0) {
            if (tryGrowPositionsArrayIfNeeded()) {
                pIndex = findPosition(key, hashCode);
            }
            final var eIndex = getFreeKeyIndex();
            keys[eIndex] = key;
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
    public void compute(K key, UnaryOperator<V> valueProcessor) {
        compute(key, key.hashCode(), valueProcessor);
    }

    @Override
    public void compute(K key, int hashCode, UnaryOperator<V> valueProcessor) {
        var pIndex = findPosition(key, hashCode);
        if (pIndex < 0) {
            final var value = valueProcessor.apply(null);
            if (value == null)
                return;
            final var eIndex = getFreeKeyIndex();
            keys[eIndex] = key;
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
        return new SparseArrayIterator<>(values, keysPos, this);
    }

    @Override
    public Spliterator<V> valueSpliterator() {
        return new SparseArraySpliterator<>(values, keysPos, this);
    }
}

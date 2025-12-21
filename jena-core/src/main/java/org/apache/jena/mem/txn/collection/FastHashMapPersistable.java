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

import org.apache.jena.mem.collection.JenaMap;
import org.apache.jena.mem.iterator.SparseArrayIterator;
import org.apache.jena.mem.spliterator.SparseArraySpliterator;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.ConcurrentModificationException;
import java.util.Spliterator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;


public class FastHashMapPersistable<K, V> extends FastHashPersistableBase<K> implements Persistable<FastHashMapPersistable<K, V>>, JenaMap<K, V> {

    protected V[] values;

    public FastHashMapPersistable(int initialSize) {
        super(initialSize);
        this.values = newValuesArray(keys.length);
    }

    public FastHashMapPersistable() {
        super();
        this.values = newValuesArray(keys.length);
    }

    protected FastHashMapPersistable(final FastHashMapPersistable<K, V> base, boolean createRevision) {
        super(base, createRevision);
        if(createRevision) {
            this.values = base.values;
        } else  {
            this.values = newValuesArray(keys.length);
            System.arraycopy(base.values, 0, this.values, 0, base.values.length);
        }
    }

    protected FastHashMapPersistable(final FastHashMapPersistable<K, V> mapToCopy, final UnaryOperator<V> valueProcessor) {
        super(mapToCopy, false);
        this.values = newValuesArray(keys.length);
        for (int i = 0; i < mapToCopy.values.length; i++) {
            final var value = mapToCopy.values[i];
            if (value != null) {
                this.values[i] = valueProcessor.apply(value);
            }
        }
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public FastHashMapPersistable<K, V> getMutableParent() {
        throw new UnsupportedOperationException("This map is already mutable");
    }

    @Override
    public FastHashMapPersistable<K, V> createImmutableChild() {
        return new FastHashMapPersistableImmutable<>(this, true);
    }

    protected final V[] newValuesArray(int size) {
        @SuppressWarnings("unchecked")
        final V[] newArray = (V[]) new Object[size];
        return newArray;
    }

    @Override
    protected final void growKeysAndHashCodeArrays() {
        super.growKeysAndHashCodeArrays();
        final var oldValues = values;
        values = newValuesArray(keys.length);
        System.arraycopy(oldValues, 0, values, 0, oldValues.length);
    }

    @Override
    protected final void afterGrowKeysAndHashCodeArraysProcessDeletedIndices(final boolean[] oldDeleted) {
        lastDeletedIndex = -1;
        var deletedIndex = oldDeleted.length;
        while (0 < deletedIndex--) {
            if (oldDeleted[deletedIndex]) {
                keys[deletedIndex] = null;
                values[deletedIndex] = null;
                hashCodesOrDeletedIndices[deletedIndex] = lastDeletedIndex;
                lastDeletedIndex = deletedIndex;
                break;
            }
        }
    }

    @Override
    public void clear() {
        super.clear();
        values = newValuesArray(keys.length);
    }

    @Override
    public boolean tryPut(K key, V value) {
        final var hashCode = key.hashCode();
        var pIndex = findPosition(key, hashCode);
        final var isNewKey = pIndex < 0;
        if(isNewKey) {
            if (tryGrowPositionsArrayIfNeeded()) {
                pIndex = findPosition(key, hashCode);
            }
        } else {
            deleted[~positions[pIndex]] = true;
            pIndex = ~pIndex;
            removedKeysCount++;
        }
        final var eIndex = getFreeKeyIndex();
        keys[eIndex] = key;
        values[eIndex] = value;
        hashCodesOrDeletedIndices[eIndex] = hashCode;
        positions[~pIndex] = ~eIndex;

        return isNewKey;
    }

    @Override
    public void put(K key, V value) {
        tryPut(key, value);
    }

    /**
     * Returns the value at the given index.
     *
     * @param i index
     * @return value
     */
    public final V getValueAt(int i) {
        return values[i];
    }

    @Override
    public final V get(K key) {
        var pIndex = findPosition(key, key.hashCode());
        if (pIndex < 0) {
            return null;
        } else {
            return values[~positions[pIndex]];
        }
    }

    @Override
    public final V getOrDefault(K key, V defaultValue) {
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
        final int hashCode = key.hashCode();
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
    public final ExtendedIterator<V> valueIterator() {
        final var initialSize = size();
        final Runnable checkForConcurrentModification = () ->
        {
            if (size() != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArrayIterator<>(values, keysPos, checkForConcurrentModification);
    }

    @Override
    public final Spliterator<V> valueSpliterator() {
        final var initialSize = this.size();
        final Runnable checkForConcurrentModification = () ->
        {
            if (this.size() != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArraySpliterator<>(values, keysPos, checkForConcurrentModification);
    }
}

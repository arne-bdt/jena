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
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.ConcurrentModificationException;
import java.util.Spliterator;
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
public abstract class FastHashMap<Key, Value> extends FastHashBase<Key> implements JenaMap<Key, Value> {

    protected Value[] values;

    public FastHashMap(int initialSize) {
        super(initialSize);
        this.values = newValuesArray(keys.length);
    }

    public FastHashMap() {
        super();
        this.values = newValuesArray(keys.length);
    }

    protected abstract Value[] newValuesArray(int size);

    @Override
    protected void growKeysAndHashCodeArrays() {
        super.growKeysAndHashCodeArrays();
        final var oldValues = values;
        values = newValuesArray(keys.length);
        System.arraycopy(oldValues, 0, values, 0, oldValues.length);
    }

    @Override
    protected void removeFrom(int here) {
        values[~positions[here]] = null;
        super.removeFrom(here);
    }

    @Override public void clear() {
        super.clear();
        values = newValuesArray(keys.length);
    }

    @Override
    public boolean tryPut(Key key, Value value) {
        growPositionsArrayIfNeeded();
        final var hashCode = key.hashCode();
        var pIndex = findPosition(key, hashCode);
        if (pIndex < 0) {
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
    public void put(Key key, Value value) {
        growPositionsArrayIfNeeded();
        final var hashCode = key.hashCode();
        var pIndex = findPosition(key, hashCode);
        if (pIndex < 0) {
            final var eIndex = getFreeKeyIndex();
            keys[eIndex] = key;
            values[eIndex] = value;
            hashCodesOrDeletedIndices[eIndex] = hashCode;
            positions[~pIndex] = ~eIndex;
        } else {
            values[~positions[pIndex]] = value;
        }
    }

    public Value getValueAt(int i) {
        return values[i];
    }

    @Override
    public Value get(Key key) {
        var pIndex = findPosition(key, key.hashCode());
        if (pIndex < 0) {
            return null;
        } else {
            return values[~positions[pIndex]];
        }
    }

    @Override
    public Value getOrDefault(Key key, Value defaultValue) {
        var pIndex = findPosition(key, key.hashCode());
        if (pIndex < 0) {
            return defaultValue;
        } else {
            return values[~positions[pIndex]];
        }
    }

    @Override
    public Value computeIfAbsent(Key key, Supplier<Value> absentValueSupplier) {
        var pIndex = findPosition(key, key.hashCode());
        if (pIndex < 0) {
            if(tryGrowPositionsArrayIfNeeded()) {
                pIndex = findPosition(key, key.hashCode());
            }
            final var eIndex = getFreeKeyIndex();
            keys[eIndex] = key;
            hashCodesOrDeletedIndices[eIndex] = key.hashCode();
            final var value = absentValueSupplier.get();
            values[eIndex] = value;
            positions[~pIndex] = ~eIndex;
            return value;
        } else {
            return values[~positions[pIndex]];
        }
    }


    @Override
    public ExtendedIterator<Value> valueIterator() {
        final var initialSize = size();
        final Runnable checkForConcurrentModification = () ->
        {
            if (size() != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArrayIterator<>(values, keysPos, checkForConcurrentModification);
    }

    @Override
    public Spliterator<Value> valueSpliterator() {
        final var initialSize = this.size();
        final Runnable checkForConcurrentModification = () ->
        {
            if (this.size() != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArraySubSpliterator<>(values, 0, keysPos, checkForConcurrentModification);
    }
}

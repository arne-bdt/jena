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

/**
 * Set which grows, if needed but never shrinks.
 * This set does not guarantee any order.
 * This set does not allow null values.
 * This set is not thread safe.
 * It´s purpose is to support fast add, remove, contains and stream / iterate operations.
 * Only remove operations are not as fast as in {@link:java.util.HashSet}
 * Iterating over this set not get much faster again after removing elements.
 */
public abstract class FastHashSet<E> extends FastHashBase<E> implements JenaSetHashOptimized<E> {

    public FastHashSet(int initialSize) {
        super(initialSize);
    }

    public FastHashSet() {
        super();
    }

    @Override
    public boolean tryAdd(E key) {
        return tryAdd(key, key.hashCode());
    }

    public boolean tryAdd(E value, int hashCode) {
        var pIndex = findPosition(value, hashCode);
        if (pIndex < 0) {
            final var eIndex = getFreeKeyIndex();
            keys[eIndex] = value;
            hashCodesOrDeletedIndices[eIndex] = hashCode;
            positions[~pIndex] = ~eIndex;
            growPositionsArrayIfNeeded();
            return true;
        }
        return false;
    }

    /* Add and get the index of the added element.
     * If the element already exists, return the inverse (~) index of the existing element.
     */
    public int addAndGetIndex(E value) {
        var pIndex = findPosition(value, value.hashCode());
        if (pIndex < 0) {
            final var eIndex = getFreeKeyIndex();
            keys[eIndex] = value;
            hashCodesOrDeletedIndices[eIndex] = value.hashCode();
            positions[~pIndex] = ~eIndex;
            growPositionsArrayIfNeeded();
            return eIndex;
        } else {
            return positions[pIndex];
        }
    }

    @Override
    public void addUnchecked(E key) {
        addUnchecked(key, key.hashCode());
    }

    public void addUnchecked(E value, int hashCode) {
        final var eIndex = getFreeKeyIndex();
        keys[eIndex] = value;
        hashCodesOrDeletedIndices[eIndex] = hashCode;
        positions[findEmptySlotWithoutEqualityCheck(hashCode)] = ~eIndex;
        growPositionsArrayIfNeeded();
    }

    public E getKeyAt(int i) {
        return keys[i];
    }
}

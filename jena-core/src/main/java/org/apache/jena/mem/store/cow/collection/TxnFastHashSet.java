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

package org.apache.jena.mem.store.cow.collection;

import org.apache.jena.mem.collection.JenaSetIndexed;

/**
 * Copy-on-write twin of {@link org.apache.jena.mem.collection.FastHashSet}
 * built on {@link TxnFastHashBase}. See the base class for the sharing and
 * tombstone discipline.
 *
 * @param <K> the element type
 */
public abstract class TxnFastHashSet<K> extends TxnFastHashBase<K> implements JenaSetIndexed<K> {

    public TxnFastHashSet(final int initialSize) {
        super(initialSize);
    }

    public TxnFastHashSet() {
        super();
    }

    /**
     * Fork constructor — see {@link TxnFastHashBase#TxnFastHashBase(TxnFastHashBase)}
     * for sharing and discipline.
     */
    protected TxnFastHashSet(final TxnFastHashSet<K> source) {
        super(source);
    }

    @Override
    public boolean tryAdd(K key) {
        return tryAdd(key, key.hashCode());
    }

    @Override
    public boolean tryAdd(K key, int hashCode) {
        growPositionsArrayIfNeeded();
        final var pIndex = findPosition(key, hashCode);
        if (pIndex < 0) {
            // Append at keysPos++ (see TxnFastHashBase#getFreeKeyIndex for why).
            final var eIndex = getFreeKeyIndex();
            keys[eIndex] = key;
            hashCodes[eIndex] = hashCode;
            // Liveness bit must be cleared in case the slot was tombstoned in
            // the snapshot we forked from (snapshot keeps deleted[i]=true for
            // slots beyond keysPos? — actually not relevant here since
            // getFreeKeyIndex always returns a fresh keysPos++ that is past
            // any inherited tombstone, but writing false defensively keeps
            // the invariant explicit).
            deleted[eIndex] = false;
            positions[~pIndex] = ~eIndex;
            return true;
        }
        return false;
    }

    @Override
    public int addAndGetIndex(K key) {
        growPositionsArrayIfNeeded();
        final var hashCode = key.hashCode();
        final var pIndex = findPosition(key, hashCode);
        if (pIndex < 0) {
            final var eIndex = getFreeKeyIndex();
            keys[eIndex] = key;
            hashCodes[eIndex] = hashCode;
            deleted[eIndex] = false;
            positions[~pIndex] = ~eIndex;
            return eIndex;
        } else {
            // Mirror FastHashSet: return the existing positions[] entry,
            // which is ~entryIndex (a negative number distinguishable from
            // the new-entry case above).
            return positions[pIndex];
        }
    }

    @Override
    public void addUnchecked(K key) {
        addUnchecked(key, key.hashCode());
    }

    @Override
    public void addUnchecked(K key, int hashCode) {
        growPositionsArrayIfNeeded();
        final var eIndex = getFreeKeyIndex();
        keys[eIndex] = key;
        hashCodes[eIndex] = hashCode;
        deleted[eIndex] = false;
        positions[findEmptySlotWithoutEqualityCheck(hashCode)] = ~eIndex;
    }
}

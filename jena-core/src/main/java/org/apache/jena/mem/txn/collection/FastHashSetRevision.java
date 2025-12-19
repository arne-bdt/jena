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

import org.apache.jena.mem.collection.JenaSetHashOptimized;
import org.apache.jena.mem.txn.iterator.SparseArrayIndexedIterator;
import org.apache.jena.mem.txn.spliterator.SparseArrayIndexedSpliterator;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.ConcurrentModificationException;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class FastHashSetRevision<K> extends FastHashRevisionedBase<K> implements Revision<FastHashSetRevisionReadonly<K>>, JenaSetHashOptimized<K> {

    protected FastHashSetRevision(int initialSize) {
        super(initialSize);
    }

    protected FastHashSetRevision() {
        super();
    }

    protected FastHashSetRevision(final FastHashSetRevision<K> base, boolean createRevision) {
        super(base, createRevision);
    }

    @Override
    public final FastHashSetRevisionReadonly<K> createRevision() {
        final var revision = new FastHashSetRevisionReadonly<>(this, true);
        this.revisionNumber++;
        return revision;
    }

    @Override
    public boolean tryAdd(K key) {
        return tryAdd(key, key.hashCode());
    }

    @Override
    public boolean tryAdd(K value, int hashCode) {
        tryGrowPositionsArrayIfNeeded();
        var pIndex = findPosition(value, hashCode);
        if (pIndex < 0) {
            final var eIndex = getFreeKeyIndex();
            keys[eIndex] = value;
            hashCodesOrDeletedIndices[eIndex] = hashCode;
            positions[~pIndex] = ~eIndex;
            return true;
        }
        return false;
    }

    /**
     * Add and get the index of the added element.
     *
     * @param value the value to add
     * @return the index of the added element or the inverse (~) index of the existing element
     */
    public int addAndGetIndex(K value) {
        return addAndGetIndex(value, value.hashCode());
    }

    /**
     * Add and get the index of the added element.
     *
     * @param value    the value to add
     * @param hashCode the hash code of the value. This is a performance optimization.
     * @return the index of the added element or the inverse (~) index of the existing element
     */
    public int addAndGetIndex(final K value, final int hashCode) {
        tryGrowPositionsArrayIfNeeded();
        final var pIndex = findPosition(value, hashCode);
        if (pIndex < 0) {
            final var eIndex = getFreeKeyIndex();
            keys[eIndex] = value;
            hashCodesOrDeletedIndices[eIndex] = hashCode;
            positions[~pIndex] = ~eIndex;
            return eIndex;
        } else {
            return positions[pIndex];
        }
    }

    @Override
    public void addUnchecked(K key) {
        addUnchecked(key, key.hashCode());
    }

    @Override
    public void addUnchecked(K value, int hashCode) {
        tryGrowPositionsArrayIfNeeded();
        final var eIndex = getFreeKeyIndex();
        keys[eIndex] = value;
        hashCodesOrDeletedIndices[eIndex] = hashCode;
        positions[findEmptySlotWithoutEqualityCheck(hashCode)] = ~eIndex;
    }

    /**
     * Get an iterator over pairs of keys and their indices in the set.
     * The iterator is not thread safe.
     *
     * @return an iterator over pairs of keys and their indices in the set
     */
    public final ExtendedIterator<IndexedKey<K>> indexedKeyIterator() {
        final var initialSize = size();
        final Runnable checkForConcurrentModification = () ->
        {
            if (size() != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArrayIndexedIterator<>(keys, deleted, keysPos, checkForConcurrentModification);
    }

    /**
     * Get a spliterator over pairs of keys and their indices in the set.
     * The spliterator is not thread safe.
     *
     * @return a spliterator over pairs of keys and their indices in the set
     */
    public final Spliterator<IndexedKey<K>> indexedKeySpliterator() {
        final var initialSize = this.size();
        final Runnable checkForConcurrentModification = () ->
        {
            if (this.size() != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArrayIndexedSpliterator<>(keys, deleted, keysPos, checkForConcurrentModification);
    }

    /**
     * Get a stream over pairs of keys and their indices in the set.
     * The stream is not thread safe.
     *
     * @return a stream over pairs of keys and their indices in the set
     */
    public final Stream<IndexedKey<K>> indexedKeyStream() {
        return StreamSupport.stream(indexedKeySpliterator(), false);
    }

    /**
     * Get a parallel stream over pairs of keys and their indices in the set.
     * The stream is not thread safe.
     *
     * @return a parallel stream over pairs of keys and their indices in the set
     */
    public final Stream<IndexedKey<K>> indexedKeyStreamParallel() {
        return StreamSupport.stream(indexedKeySpliterator(), true);
    }
}

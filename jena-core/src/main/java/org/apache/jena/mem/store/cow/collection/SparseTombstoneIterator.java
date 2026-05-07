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

import org.apache.jena.mem.collection.Sized;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

/**
 * Iterator over a sparse array slice {@code [0, toIndex)} that skips dead
 * slots according to an explicit {@code boolean[] deleted} tombstone bitmap
 * (instead of relying on a {@code null} check on the entry).
 * <p>
 * This is the iteration primitive used by the copy-on-write transactional
 * collections in {@link TxnFastHashBase}: live slots are determined by
 * {@code !deleted[i]}, never by {@code keys[i] != null}, because a snapshot
 * may share the underlying {@code keys[]} with a writer that has not (and
 * by design, must not) overwritten dead slots in shared arrays.
 * <p>
 * Walks from {@code toIndex - 1} down to {@code 0}, mirroring the order of
 * {@link org.apache.jena.mem.iterator.SparseArrayIterator}.
 *
 * @param <E> the type of the array elements
 */
public class SparseTombstoneIterator<E> extends NiceIterator<E> {

    private final E[] entries;
    private final boolean[] deleted;
    private final Sized owner;
    private final int sizeOfOwnerAtStart;
    private int pos;
    private boolean hasNext = false;

    /**
     * @param entries the backing key array (not copied)
     * @param deleted the tombstone bitmap parallel to {@code entries}; entry
     *                at index {@code i} is live iff {@code !deleted[i]}
     * @param toIndex exclusive upper bound on the iterated slice
     * @param owner   the owning collection used to detect concurrent modifications
     */
    public SparseTombstoneIterator(final E[] entries, final boolean[] deleted,
                                   final int toIndex, final Sized owner) {
        this.entries = entries;
        this.deleted = deleted;
        this.pos = toIndex - 1;
        this.owner = owner;
        this.sizeOfOwnerAtStart = owner.size();
    }

    @Override
    public boolean hasNext() {
        while (-1 < pos) {
            if (!deleted[pos]) {
                return hasNext = true;
            }
            pos--;
        }
        return hasNext = false;
    }

    @Override
    public E next() {
        if (sizeOfOwnerAtStart != owner.size()) throw new ConcurrentModificationException();
        if (hasNext || hasNext()) {
            hasNext = false;
            return entries[pos--];
        }
        throw new NoSuchElementException();
    }

    @Override
    public void forEachRemaining(Consumer<? super E> action) {
        while (-1 < pos) {
            if (!deleted[pos]) {
                action.accept(entries[pos]);
            }
            pos--;
        }
        if (sizeOfOwnerAtStart != owner.size()) throw new ConcurrentModificationException();
    }
}

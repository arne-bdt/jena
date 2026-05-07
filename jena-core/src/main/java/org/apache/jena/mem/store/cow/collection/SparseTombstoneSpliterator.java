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

import java.util.ConcurrentModificationException;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Spliterator counterpart to {@link SparseTombstoneIterator}. Walks a sparse
 * array slice {@code [from, to)} from high index down to low, skipping dead
 * slots according to an explicit {@code boolean[] deleted} tombstone bitmap
 * (instead of a {@code null} check).
 * <p>
 * Supports parallel splitting via {@link #trySplit()}, halving the slice on
 * each split. Concurrent-modification detection works as in
 * {@link org.apache.jena.mem.spliterator.SparseArraySpliterator}: the size
 * of the owning collection is snapshotted at construction and rechecked at
 * each advance/forEach boundary.
 *
 * @param <E> the type of the array elements
 */
public class SparseTombstoneSpliterator<E> implements Spliterator<E> {

    private final E[] entries;
    private final boolean[] deleted;
    private final int fromIndex;
    private int pos;
    private final Sized owner;
    private final int sizeOfOwnerAtStart;

    /**
     * @param entries the backing key array (not copied)
     * @param deleted the tombstone bitmap parallel to {@code entries}; entry
     *                at index {@code i} is live iff {@code !deleted[i]}
     * @param toIndex exclusive upper bound on the iterated slice
     * @param owner   the owning collection used to detect concurrent modifications
     */
    public SparseTombstoneSpliterator(final E[] entries, final boolean[] deleted,
                                      final int toIndex, final Sized owner) {
        this(entries, deleted, 0, toIndex, owner, owner.size());
    }

    private SparseTombstoneSpliterator(final E[] entries, final boolean[] deleted,
                                       final int fromIndex, final int toIndex,
                                       final Sized owner, final int sizeOfOwnerAtStart) {
        this.entries = entries;
        this.deleted = deleted;
        this.fromIndex = fromIndex;
        this.pos = toIndex;
        this.owner = owner;
        this.sizeOfOwnerAtStart = sizeOfOwnerAtStart;
    }

    @Override
    public boolean tryAdvance(Consumer<? super E> action) {
        if (sizeOfOwnerAtStart != owner.size()) throw new ConcurrentModificationException();
        while (fromIndex <= --pos) {
            if (!deleted[pos]) {
                action.accept(entries[pos]);
                return true;
            }
        }
        return false;
    }

    @Override
    public void forEachRemaining(Consumer<? super E> action) {
        pos--;
        while (fromIndex <= pos) {
            if (!deleted[pos]) {
                action.accept(entries[pos]);
            }
            pos--;
        }
        if (sizeOfOwnerAtStart != owner.size()) throw new ConcurrentModificationException();
    }

    @Override
    public Spliterator<E> trySplit() {
        // Halve the remaining slice. This produces a balanced split tree
        // for parallel streams; each child covers a disjoint range so they
        // can be drained concurrently without interlock.
        int remaining = pos - fromIndex;
        if (remaining < 2) {
            return null;
        }
        int mid = fromIndex + (remaining >>> 1);
        // Child takes the upper half [mid, pos); this spliterator keeps
        // [fromIndex, mid). Iteration order is descending, so the upper
        // half drains first overall.
        Spliterator<E> child = new SparseTombstoneSpliterator<>(
                entries, deleted, mid, this.pos, owner, sizeOfOwnerAtStart);
        this.pos = mid;
        return child;
    }

    @Override
    public long estimateSize() {
        // Upper bound; actual count after skipping tombstones may be lower.
        return pos - fromIndex;
    }

    @Override
    public int characteristics() {
        return DISTINCT | NONNULL | IMMUTABLE;
    }
}

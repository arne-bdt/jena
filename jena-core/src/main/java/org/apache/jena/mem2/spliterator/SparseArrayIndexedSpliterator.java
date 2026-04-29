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

package org.apache.jena.mem2.spliterator;

import org.apache.jena.mem2.collection.FastHashBase;
import org.apache.jena.mem2.collection.FastHashSet;
import org.apache.jena.mem2.collection.JenaMapSetCommon;

import java.util.ConcurrentModificationException;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Spliterator over a sparse array, iterating in ascending index order and
 * skipping {@code null} entries. Each element is reported as a
 * {@link FastHashBase.IndexedKey} pair so callers see both the value and the
 * stable index it occupies.
 * <p>
 * Supports recursive splitting for parallel traversal. Detects concurrent
 * modifications by snapshotting {@code set.size()} at construction time and
 * rechecking it at each advance/forEach boundary; throws
 * {@link ConcurrentModificationException} if the size has changed.
 *
 * @param <E> the type of the array elements
 */
@SuppressWarnings("all")
public class SparseArrayIndexedSpliterator<E> implements Spliterator<FastHashBase.IndexedKey<E>> {

    private final E[] entries;
    private int currentPositionMinusOne;
    private final int toIndexExclusive;
    private final JenaMapSetCommon<?> set;
    private final int sizeOfSetAtStart;

    /**
     * Create a spliterator over {@code entries[fromIndexInclusive .. toIndexExclusive)},
     * skipping nulls.
     *
     * @param entries            the backing array (not copied)
     * @param fromIndexInclusive inclusive lower bound on the iterated slice
     * @param toIndexExclusive   exclusive upper bound on the iterated slice
     * @param set                the owning collection used to detect concurrent modifications
     */
    public SparseArrayIndexedSpliterator(final E[] entries, final int fromIndexInclusive, final int toIndexExclusive, final JenaMapSetCommon<?> set) {
        this.entries = entries;
        this.currentPositionMinusOne = fromIndexInclusive-1; // Start at fromIndexInclusive - 1, so that the first call to tryAdvance will increment pos to fromIndexInclusive
        this.toIndexExclusive = toIndexExclusive;
        this.set = set;
        this.sizeOfSetAtStart = set.size();
    }

    /**
     * Create a spliterator over {@code entries[0 .. toIndexExclusive)}, skipping nulls.
     *
     * @param entries          the backing array (not copied)
     * @param toIndexExclusive exclusive upper bound on the iterated slice
     * @param set              the owning collection used to detect concurrent modifications
     */
    public SparseArrayIndexedSpliterator(final E[] entries, final int toIndexExclusive, final JenaMapSetCommon<?> set) {
        this(entries, 0, toIndexExclusive, set);
    }

    /**
     * Create a spliterator over the entire array, skipping nulls.
     *
     * @param entries the backing array (not copied)
     * @param set     the owning collection used to detect concurrent modifications
     */
    public SparseArrayIndexedSpliterator(final E[] entries, final JenaMapSetCommon<?> set) {
        this(entries, entries.length, set);
    }


    @Override
    public boolean tryAdvance(Consumer<? super FastHashSet.IndexedKey<E>> action) {
        if (sizeOfSetAtStart != set.size()) throw new ConcurrentModificationException();
        while (toIndexExclusive > ++currentPositionMinusOne) {
            if (null != entries[currentPositionMinusOne]) {
                action.accept(new FastHashSet.IndexedKey<>(currentPositionMinusOne, entries[currentPositionMinusOne]));
                return true;
            }
        }
        return false;
    }

    @Override
    public void forEachRemaining(Consumer<? super FastHashSet.IndexedKey<E>> action) {
        while (toIndexExclusive > ++currentPositionMinusOne) {
            if (null != entries[currentPositionMinusOne]) {
                action.accept(new FastHashSet.IndexedKey<>(currentPositionMinusOne, entries[currentPositionMinusOne]));
            }
        }
        if (sizeOfSetAtStart != set.size()) throw new ConcurrentModificationException();
    }

    @Override
    public Spliterator<FastHashSet.IndexedKey<E>> trySplit() {
        final var nextPos = currentPositionMinusOne + 1;
        final var remaining = toIndexExclusive - nextPos;
        if ( remaining < 2) {
            return null;
        }
        final var mid = nextPos + ( remaining >>> 1);
        final var fromIndexInclusive = nextPos;
        this.currentPositionMinusOne = mid-1;
        return new SparseArrayIndexedSpliterator<>(entries, fromIndexInclusive, mid, set);
    }

    @Override
    public long estimateSize() { return (long) toIndexExclusive - currentPositionMinusOne; }

    @Override
    public int characteristics() {
        return DISTINCT | NONNULL | IMMUTABLE;
    }
}

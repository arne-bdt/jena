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

import org.apache.jena.mem2.collection.JenaMapSetCommon;

import java.util.ConcurrentModificationException;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * A spliterator for sparse arrays. This spliterator will iterate over the array
 * skipping null entries.
 * <p>
 * This spliterator supports splitting into sub-spliterators.
 * <p>
 * The spliterator will check for concurrent modifications by invoking a {@link Runnable}
 * before each action.
 *
 * @param <E> the type of the array elements
 */
public class SparseArraySpliterator<E> implements Spliterator<E> {

    private final E[] entries;
    private int pos;
    private final JenaMapSetCommon<?> set;
    private final int sizeOfSetAtStart;

    /**
     * Create a spliterator for the given array, with the given size.
     *
     * @param entries                        the array
     * @param toIndex                        the index of the last element, exclusive
     * @param set                            the set to check for concurrent modifications
     */
    public SparseArraySpliterator(final E[] entries, final int toIndex, final JenaMapSetCommon<?> set) {
        this.entries = entries;
        this.pos = toIndex;
        this.set = set;
        this.sizeOfSetAtStart = set.size();
    }

    /**
     * Create a spliterator for the given array, with the given size.
     *
     * @param entries                        the array
     * @param set                            the set to check for concurrent modifications
     */
    public SparseArraySpliterator(final E[] entries, final JenaMapSetCommon<?> set) {
        this(entries, entries.length, set);
    }


    @Override
    public boolean tryAdvance(Consumer<? super E> action) {
        if (sizeOfSetAtStart != set.size()) throw new ConcurrentModificationException();
        while (-1 < --pos) {
            if (null != entries[pos]) {
                action.accept(entries[pos]);
                return true;
            }
        }
        return false;
    }

    @Override
    public void forEachRemaining(Consumer<? super E> action) {
        pos--;
        while (-1 < pos) {
            if (null != entries[pos]) {
                action.accept(entries[pos]);
            }
            pos--;
        }
        if (sizeOfSetAtStart != set.size()) throw new ConcurrentModificationException();
    }

    @Override
    public Spliterator<E> trySplit() {
        if (pos < 2) {
            return null;
        }
        final int toIndexOfSubIterator = this.pos;
        this.pos = pos >>> 1;
        return new SparseArraySubSpliterator<>(entries, this.pos, toIndexOfSubIterator, set);
    }

    @Override
    public long estimateSize() { return pos; }

    @Override
    public int characteristics() {
        return DISTINCT | NONNULL | IMMUTABLE;
    }
}

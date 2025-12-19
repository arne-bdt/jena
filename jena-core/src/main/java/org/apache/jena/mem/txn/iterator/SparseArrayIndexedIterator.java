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

package org.apache.jena.mem.txn.iterator;

import org.apache.jena.mem.txn.collection.IndexedKey;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

/**
 * An iterator over a sparse array, that skips null entries.
 * This iterator returns elements as {@link FastHashSet.IndexedKey} objects,
 * which contain both the index and the value of the element.
 *
 * The iterator works in ascending order, starting from index 0 up to the specified exclusive index.
 *
 * This iterator will check for concurrent modifications by invoking a {@link Runnable}
 *
 * @param <E> the type of the array elements
 */
@SuppressWarnings("all")
public class SparseArrayIndexedIterator<E> extends NiceIterator<IndexedKey<E>> implements Iterator<IndexedKey<E>> {

    private final E[] entries;
    private final boolean[] deleted;
    private final Runnable checkForConcurrentModification;
    private int pos = 0;
    private final int toIndexExclusive;
    private boolean hasNext = false;

    public SparseArrayIndexedIterator(final E[] entries, final boolean[] deleted, final Runnable checkForConcurrentModification) {
        this(entries, deleted, entries.length, checkForConcurrentModification);
    }

    public SparseArrayIndexedIterator(final E[] entries, final boolean[] deleted, int toIndexExclusive, final Runnable checkForConcurrentModification) {
        this.entries = entries;
        this.deleted = deleted;
        this.toIndexExclusive = toIndexExclusive;
        this.checkForConcurrentModification = checkForConcurrentModification;
    }

    /**
     * Returns {@code true} if the iteration has more elements.
     * (In other words, returns {@code true} if {@link #next} would
     * return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     */
    @Override
    public boolean hasNext() {
        while (toIndexExclusive > pos) {
            if (!deleted[pos]) {
                hasNext = true;
                return true;
            }
            pos++;
        }
        hasNext = false;
        return false;
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    @Override
    public IndexedKey<E> next() {
        this.checkForConcurrentModification.run();
        if (hasNext || hasNext()) {
            hasNext = false;
            return new IndexedKey<>(pos, entries[pos++]);
        }
        throw new NoSuchElementException();
    }

    @Override
    public void forEachRemaining(Consumer<? super IndexedKey<E>> action) {
        while (toIndexExclusive > pos) {
            if (!deleted[pos]) {
                action.accept(new IndexedKey<>(pos, entries[pos]));
            }
            pos++;
        }
        this.checkForConcurrentModification.run();
    }
}

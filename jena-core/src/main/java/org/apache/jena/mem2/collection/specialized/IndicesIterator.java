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

package org.apache.jena.mem2.collection.specialized;

import org.apache.jena.util.iterator.NiceIterator;

import java.util.NoSuchElementException;
import java.util.function.Consumer;

/**
 * An iterator over the indices of a {@link FastHashIndexSet}.
 * A valid index is always positive.
 * Negative entries are skipped.
 */
public class IndicesIterator extends NiceIterator<Integer> {

    private final int[] indices;
    private final Runnable checkForConcurrentModification;
    private int pos;

    private boolean hasNext=false;

    public IndicesIterator(final int[] indices, final Runnable checkForConcurrentModification) {
        this.indices = indices;
        this.pos = indices.length - 1;
        this.checkForConcurrentModification = checkForConcurrentModification;
    }

    public IndicesIterator(final int[] indices, int toIndexExclusive, final Runnable checkForConcurrentModification) {
        this.indices = indices;
        this.pos = toIndexExclusive - 1;
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
        while (-1 < pos) {
            if (-1 < indices[pos]) {
                return hasNext = true;
            }
            pos--;
        }
        return hasNext = false;
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    @Override
    public Integer next() {
        this.checkForConcurrentModification.run();
        if (hasNext || hasNext()) {
            hasNext = false;
            return indices[pos--];
        }
        throw new NoSuchElementException();
    }

    @Override
    public void forEachRemaining(Consumer<? super Integer> action) {
        if(pos == indices.length - 1) {
            for(int index: indices) {
                if(-1 < index) {
                    action.accept(indices[index]);
                }
            }
        } else {
            while (-1 < pos) {
                if (-1 < indices[pos]) {
                    action.accept(indices[pos]);
                }
                pos--;
            }
        }
        this.checkForConcurrentModification.run();
    }
}

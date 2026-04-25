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

package org.apache.jena.mem2.iterator;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.store.roaring.BlockSet;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class SparseBlockIterator extends NiceIterator<Triple> {

    private final Runnable checkForConcurrentModification;
    private final BlockSet.Block[] blocks;
    private final int lastBlock;
    private int block = 0;
    private int row = 0;
    private boolean hasNext = false;
    private Triple next;

    public SparseBlockIterator(final BlockSet.Block[] blocks, final int lastBlock, final Runnable checkForConcurrentModification) {
        this.blocks = blocks;
        this.lastBlock = lastBlock;
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
        if (hasNext) return true;
        while (block <= lastBlock) {
            while (row < blocks[block].currentIndexSize ) {
                if ((next = blocks[block].triples[row++]) != null) {
                    return hasNext = true;
                }
            }
            row = 0;
            block++;
        }
        return false;
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    @Override
    public Triple next() {
        this.checkForConcurrentModification.run();
        if (hasNext || hasNext()) {
            hasNext = false;
            return next;
        }
        throw new NoSuchElementException();
    }

    @Override
    public void forEachRemaining(Consumer<? super Triple> action) {
        while (block <= lastBlock) {
            while (row < blocks[block].currentIndexSize ) {
                if ((next = blocks[block].triples[row]) != null) {
                    action.accept(next);
                }
                row++;
            }
            row = 0;
            block++;
        }
        this.checkForConcurrentModification.run();
    }
}

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

import org.apache.jena.graph.Triple;
import org.apache.jena.mem.spliterator.SparseArraySubSpliterator;
import org.apache.jena.mem2.store.roaring.BlockSet;

import java.util.Spliterator;
import java.util.function.Consumer;

public class SparseBlockSpliterator implements Spliterator<Triple> {

    private final BlockSet.Block[] blocks;
    private int lastBlock;
    private int block = 0;
    private int row;
    private final Runnable checkForConcurrentModification;

    public SparseBlockSpliterator(final BlockSet.Block[] blocks, final int startBlock, final int lastBlock, final Runnable checkForConcurrentModification) {
        this.blocks = blocks;
        this.block = startBlock;
        this.lastBlock = lastBlock;
        this.checkForConcurrentModification = checkForConcurrentModification;
    }

    public SparseBlockSpliterator(final BlockSet.Block[] blocks, final int lastBlock, final Runnable checkForConcurrentModification) {
        this(blocks, 0, lastBlock, checkForConcurrentModification);
    }


    @Override
    public boolean tryAdvance(Consumer<? super Triple> action) {
        this.checkForConcurrentModification.run();
        while (block <= lastBlock) {
            while (row < blocks[block].currentIndexSize ) {
                final Triple triple;
                if ((triple = blocks[block].triples[row++]) != null) {
                    action.accept(triple);
                    return true;
                }
            }
            row = 0;
            block++;
        }
        return false;
    }

    @Override
    public void forEachRemaining(Consumer<? super Triple> action) {
        while (block <= lastBlock) {
            while (row < blocks[block].currentIndexSize ) {
                if (blocks[block].triples[row] != null) {
                    action.accept(blocks[block].triples[row]);
                }
                row++;
            }
            row = 0;
            block++;
        }
        this.checkForConcurrentModification.run();
    }

    @Override
    public Spliterator<Triple> trySplit() {
        var remainingBlocks = lastBlock - block;
        // if multiple blocks remain --> split blocks
        if (remainingBlocks > 0) {
            var oldLastBlock = lastBlock;
            this.lastBlock = block + (remainingBlocks >>> 1);
            return new SparseBlockSpliterator(blocks, this.lastBlock+1, oldLastBlock, checkForConcurrentModification);
        }
        // else split the remaining rows by increasing the current row
        // and using a SparseArraySubSpliterator for the skipped rows
        var remainingRows = blocks[block].currentIndexSize - row;
        if (remainingRows < 2) {
            return null;
        }
        var oldRow = this.row;
        this.row = row + (remainingRows >>> 1);
        return new SparseArraySubSpliterator<>(blocks[block].triples, oldRow, this.row, checkForConcurrentModification);
    }

    @Override
    public long estimateSize() {
        var remainingBlocks = lastBlock - block + 1;
        if (remainingBlocks == 1) {
            return blocks[block].currentIndexSize - row;
        } else {
            return (long) blocks[block].currentIndexSize * remainingBlocks;
        }
    }

    @Override
    public int characteristics() {
        return DISTINCT | NONNULL | IMMUTABLE;
    }
}

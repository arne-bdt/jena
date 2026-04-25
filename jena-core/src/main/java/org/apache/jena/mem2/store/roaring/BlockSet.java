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

package org.apache.jena.mem2.store.roaring;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.jena.atlas.lib.Copyable;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.iterator.SparseBlockIterator;
import org.apache.jena.mem2.spliterator.SparseBlockSpliterator;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.ConcurrentModificationException;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Set of triples that is backed by a {@link BlockSet}.
 */
public class BlockSet
        implements Copyable<BlockSet> {

    private static final int BLOCK_SIZE = 4096;
    private static final int MINIMUM_BLOCK_NUMBER = 8;
    private int currentBlock = 0;
    private int size = 0;

    private int lastDeletedIndex = -1;

    private static int calcIndex(int block, int indexInBlock) {
        return block * BLOCK_SIZE + indexInBlock;
    }

    private static int[] calcBlockAndIndexInBlock(int index) {
        return new int[] { index / BLOCK_SIZE, index % BLOCK_SIZE };
    }

    /**
     * The negative indices to the entries and hashCode arrays.
     * The indices of the positions array are derived from the hashCodes.
     * Any position 0 indicates an empty element.
     */
    private int[] positions;

    private Block[] blocks;

    private static int calculatePositionsSize() {
        var initialCapacity = BLOCK_SIZE;
        var positionsSize = Integer.highestOneBit(initialCapacity << 1);
        if (positionsSize < initialCapacity << 1) {
            positionsSize <<= 1;
        }
        return positionsSize;
    }

    public BlockSet() {
        this.positions = new int[calculatePositionsSize()];
        this.blocks = new Block[MINIMUM_BLOCK_NUMBER];
        this.blocks[0] = new Block();
    }

    public BlockRow getBlockRow(int index) {
        return new BlockRow(index, blocks[index / BLOCK_SIZE], index % BLOCK_SIZE);
    }

    private int findPosition(final Triple e, final int hashCode) {
        var pIndex = calcStartIndexByHashCode(hashCode);
        while (true) {
            if (0 == positions[pIndex]) {
                return ~pIndex;
            } else {
                final var row = getBlockRow(~positions[pIndex]);
                if (hashCode == row.getHashCodeOrDeletedIndex() && e.equals(row.getTriple())) {
                    return pIndex;
                } else if (--pIndex < 0) {
                    pIndex += positions.length;
                }
            }
        }
    }

    private BlockRow findExistingOrCreateNewRow(final Triple triple) {
        final var hashCode = triple.hashCode();
        var pIndex = calcStartIndexByHashCode(hashCode);
        while (true) {
            if (0 == positions[pIndex]) {
                final var newRow = getFreeRow();
                positions[pIndex] = ~newRow.index;
                newRow.setTriple(triple);
                newRow.setHashCodeOrDeletedIndex(hashCode);
                return newRow;
            } else {
                final var row = getBlockRow(~positions[pIndex]);
                if (hashCode == row.getHashCodeOrDeletedIndex() && triple.equals(row.getTriple())) {
                    return row;
                } else if (--pIndex < 0) {
                    pIndex += positions.length;
                }
            }
        }
    }



    private BlockRow getFreeRow() {
        final BlockRow row;
        if (lastDeletedIndex == -1) {
            var block = blocks[currentBlock];
            if(block.currentIndexSize == block.triples.length) {
                if(++currentBlock == blocks.length) {
                    var oldBlocks = blocks;
                    blocks = new Block[blocks.length + MINIMUM_BLOCK_NUMBER];
                    System.arraycopy(oldBlocks, 0, blocks, 0, oldBlocks.length);
                }
                block = new Block();
                blocks[currentBlock] = block;
            }
            row = new  BlockRow((currentBlock*BLOCK_SIZE)+block.currentIndexSize, blocks[currentBlock], block.currentIndexSize);
            block.currentIndexSize++;
        } else {
            row = getBlockRow(lastDeletedIndex);
            lastDeletedIndex = row.getHashCodeOrDeletedIndex();
        }
        size++;
        return row;
    }

    /**
     * Add and get the indexInBlock of the added element.
     *
     * @param value    the value to add
     * @return the indexInBlock of the added element or the inverse (~) indexInBlock of the existing element
     */
    public BlockRow addAndGetRow(final Triple value) {
        growPositionsArrayIfNeeded();
        return findExistingOrCreateNewRow(value);
    }

    public void addUnchecked(Triple value) {
        growPositionsArrayIfNeeded();
        final var hashCode = value.hashCode();
        final var row = getFreeRow();
        row.setTriple(value);
        row.setHashCodeOrDeletedIndex(hashCode);
        positions[findEmptySlotWithoutEqualityCheck(hashCode)] = ~row.index;
    }

    /**
     * Calculates a position in the positions array by the hashCode.
     *
     * @param hashCode the hashCode
     * @return the start indexInBlock in the positions array to search for the key
     */
    protected final int calcStartIndexByHashCode(final int hashCode) {
        return hashCode & (positions.length - 1);
    }

    /**
     * Calculates the new size of the positions array, if it needs to be grown.
     *
     * @return the new size or -1 if it does not need to be grown
     */
    private int calcNewPositionsSize() {
        if (size << 1 > positions.length) { /*grow*/
            final var newLength = positions.length << 1;
            return newLength < 0 ? Integer.MAX_VALUE : newLength;
        }
        return -1;
    }

    private void fillPositionsArray(int newSize) {
        this.positions = new int[newSize];
        for (var iBlock = 0; iBlock <= currentBlock; iBlock++) {
            var idx = iBlock * BLOCK_SIZE;
            final var block = this.blocks[iBlock];
            for (var i = 0; i < block.currentIndexSize; i++, idx++) {
                if(null != block.triples[i]) {
                    this.positions[findEmptySlotWithoutEqualityCheck(block.hashCodesOrDeletedIndices[i])] = ~idx;
                }
            }
        }
    }

    @FunctionalInterface
    public interface IndexedTripleConsumer {
        void accept(Triple triple, int index);
    }

    public void forEachTriple(IndexedTripleConsumer consumer) {
        for (var iBlock = 0; iBlock <= currentBlock; iBlock++) {
            var idx = iBlock * BLOCK_SIZE;
            final var block = this.blocks[iBlock];
            for (var i = 0; i < block.currentIndexSize; i++, idx++) {
                if(null != block.triples[i]) {
                    consumer.accept(block.triples[i], idx);
                }
            }
        }
    }

    public void forEachRow(Consumer<BlockRow> consumer) {
        for (var iBlock = 0; iBlock <= currentBlock; iBlock++) {
            var idx = iBlock * BLOCK_SIZE;
            final var block = this.blocks[iBlock];
            for (var i = 0; i < block.currentIndexSize; i++, idx++) {
                if(null != block.triples[i]) {
                    consumer.accept(new BlockRow(idx, block, i));
                }
            }
        }
    }

    /**
     * Grows the positions array if needed.
     */
    protected final void growPositionsArrayIfNeeded() {
        final var newSize = calcNewPositionsSize();
        if (newSize < 0) {
            return;
        }
        fillPositionsArray(newSize);
    }

    /**
     * Grow the positions array if needed.
     *
     * @return true if the positions array was grown
     */
    protected final boolean tryGrowPositionsArrayIfNeeded() {
        final var newSize = calcNewPositionsSize();
        if (newSize < 0) {
            return false;
        }
        fillPositionsArray(newSize);
        return true;
    }

    /**
     * Returns the number of elements in this collection.  If this collection
     * contains more than {@code Integer.MAX_VALUE} elements, returns
     * {@code Integer.MAX_VALUE}.
     *
     * @return the number of elements in this collection
     */
    public int size() {
        return size;
    }

    public void remove(final Triple triple, Consumer<BlockRow> onRemove) {
        final var hashCode = triple.hashCode();
        var pIndex = calcStartIndexByHashCode(hashCode);
        int here;
        BlockRow row;
        while (true) {
            if (0 == positions[pIndex]) {
                return;
            } else {
                row = getBlockRow(~positions[pIndex]);
                if (hashCode == row.getHashCodeOrDeletedIndex() && triple.equals(row.getTriple())) {
                    here = pIndex;
                    break;
                } else if (--pIndex < 0) {
                    pIndex += positions.length;
                }
            }
        }
        onRemove.accept(row);
        row.setTriple(null);
        row.setHashCodeOrDeletedIndex(lastDeletedIndex);
        lastDeletedIndex = row.index;
        size--;
        while (true) {
            positions[here] = 0;
            int scan = here;
            while (true) {
                if (--scan < 0) scan += positions.length;
                if (positions[scan] == 0) return;
                int r = calcStartIndexByHashCode(getHashCodeOrDeletedIndex(~positions[scan]));
                if ((scan > r || r >= here) && (r >= here || here >= scan) && (here >= scan || scan > r)) {
                    positions[here] = positions[scan];
                    here = scan;
                    break;
                }
            }
        }
    }

    private BlockRow findRow(final Triple triple) {
        final var hashCode = triple.hashCode();
        var pIndex = calcStartIndexByHashCode(hashCode);
        while (true) {
            if (0 == positions[pIndex]) {
                return null;
            } else {
                final var row = getBlockRow(~positions[pIndex]);
                if (hashCode == row.getHashCodeOrDeletedIndex() && triple.equals(row.getTriple())) {
                    return row;
                } else if (--pIndex < 0) {
                    pIndex += positions.length;
                }
            }
        }
    }

    public final boolean isEmpty() {
        return this.size() == 0;
    }

    public final boolean contains(Triple triple) {
        final int hashCode = triple.hashCode();
        var pIndex = calcStartIndexByHashCode(hashCode);
        while (true) {
            if (0 == positions[pIndex]) {
                return false;
            } else {
                final var row = getBlockRow(~positions[pIndex]);
                if (hashCode == row.getHashCodeOrDeletedIndex() && triple.equals(row.getTriple())) {
                    return true;
                } else if (--pIndex < 0) {
                    pIndex += positions.length;
                }
            }
        }
    }

    public final boolean anyMatch(Predicate<Triple> predicate) {
        for (var iBlock = 0; iBlock <= currentBlock; iBlock++) {
            var idx = iBlock * BLOCK_SIZE;
            final var block = this.blocks[iBlock];
            for (var i = 0; i < block.currentIndexSize; i++, idx++) {
                if(null != block.triples[i]
                    && predicate.test(block.triples[i])) {
                    return true;
                }
            }
        }
        return false;
    }

    public final ExtendedIterator<Triple> keyIterator() {
        final var initialSize = size();
        final Runnable checkForConcurrentModification = () ->
        {
            if (size() != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseBlockIterator(blocks, currentBlock, checkForConcurrentModification);
    }

    protected final int findEmptySlotWithoutEqualityCheck(final int hashCode) {
        var pIndex = calcStartIndexByHashCode(hashCode);
        while (true) {
            if (0 == positions[pIndex]) {
                return pIndex;
            } else if (--pIndex < 0) {
                pIndex += positions.length;
            }
        }
    }

    public void clear() {
        this.positions = new int[calculatePositionsSize()];
        this.blocks = new Block[MINIMUM_BLOCK_NUMBER];
        this.blocks[0] = new Block();
        this.size = 0;
        this.currentBlock = 0;
        this.lastDeletedIndex = -1;
    }

    public Spliterator<Triple> keySpliterator() {
        final var initialSize = this.size();
        final Runnable checkForConcurrentModification = () ->
        {
            if (this.size() != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseBlockSpliterator(blocks, currentBlock, checkForConcurrentModification);
    }

     public Stream<Triple> stream() {
        return StreamSupport.stream(keySpliterator(), false);
     }

    /**
     * Gets the key at the given indexInBlock.
     *
     * @param i the indexInBlock
     * @return the key at the given indexInBlock
     */
    public Triple getTriple(int i) {
        return blocks[i / BLOCK_SIZE].triples[i % BLOCK_SIZE];
    }


    private BlockSet(final BlockSet setToCopy) {
        this.positions = new int[setToCopy.positions.length];
        System.arraycopy(setToCopy.positions, 0, this.positions, 0, setToCopy.positions.length);

        this.currentBlock = setToCopy.currentBlock;
        this.lastDeletedIndex = setToCopy.lastDeletedIndex;
        this.size = setToCopy.size;
        this.blocks = new Block[setToCopy.blocks.length];
        for(var b = 0; b <= setToCopy.currentBlock; b++) {
            this.blocks[b] = new Block(setToCopy.blocks[b]);
        }
    }

    /**
     * Create a copy of this set.
     *
     * @return BlockSet
     */
    @Override
    public BlockSet copy() {
        return new BlockSet(this);
    }

    public void setSIndex(int index, int sIndex) {
        blocks[index / BLOCK_SIZE].sIndices[index % BLOCK_SIZE] = sIndex;
    }

    public void setPIndex(int index, int pIndex) {
        blocks[index / BLOCK_SIZE].pIndices[index % BLOCK_SIZE] = pIndex;
    }

    public void setOIndex(int index, int oIndex) {
        blocks[index / BLOCK_SIZE].oIndices[index % BLOCK_SIZE] = oIndex;
    }

    public int getSIndex(int index) {
        return blocks[index / BLOCK_SIZE].sIndices[index % BLOCK_SIZE];
    }

    public int getPIndex(int index) {
        return blocks[index / BLOCK_SIZE].pIndices[index % BLOCK_SIZE];
    }

    public int getOIndex(int index) {
        return blocks[index / BLOCK_SIZE].oIndices[index % BLOCK_SIZE];
    }

    public int getHashCodeOrDeletedIndex(int index) {
        return blocks[index / BLOCK_SIZE].hashCodesOrDeletedIndices[index % BLOCK_SIZE];
    }

    public void setTriple(int index, Triple triple) {
        blocks[index / BLOCK_SIZE].triples[index % BLOCK_SIZE] = triple;
    }

    public record BlockRow(int index, Block block, int indexInBlock) {
        public Triple getTriple() {
            return block.triples[indexInBlock];
        }
        public int getHashCodeOrDeletedIndex() {
            return block.hashCodesOrDeletedIndices[indexInBlock];
        }
        public int getSIndex() {
            return block.sIndices[indexInBlock];
        }
        public int getPIndex() {
            return block.pIndices[indexInBlock];
        }
        public int getOIndex() {
            return block.oIndices[indexInBlock];
        }

        public void setTriple(Triple triple) {
            block.triples[indexInBlock] = triple;
        }
        public void setHashCodeOrDeletedIndex(int hashCode) {
            block.hashCodesOrDeletedIndices[indexInBlock] = hashCode;
        }
        public void setSIndex(int sIndex) {
            block.sIndices[indexInBlock] = sIndex;
        }
        public void setPIndex(int pIndex) {
            block.pIndices[indexInBlock] = pIndex;
        }
        public void setOIndex(int oIndex) {
            block.oIndices[indexInBlock] = oIndex;
        }
    }

    public class Block {
        public final Triple[] triples;
        public final int[] hashCodesOrDeletedIndices;
        public final int[] sIndices;
        public final int[] pIndices;
        public final int[] oIndices;
        public int currentIndexSize;

        public Block() {
            triples = new Triple[BLOCK_SIZE];
            hashCodesOrDeletedIndices = new int[BLOCK_SIZE];
            sIndices = new int[BLOCK_SIZE];
            pIndices = new int[BLOCK_SIZE];
            oIndices = new int[BLOCK_SIZE];
        }
        public Block(final Block blockToCopy) {
            this();
            System.arraycopy(blockToCopy.triples, 0, triples, 0, BLOCK_SIZE);
            System.arraycopy(blockToCopy.hashCodesOrDeletedIndices, 0, hashCodesOrDeletedIndices, 0, BLOCK_SIZE);
            System.arraycopy(blockToCopy.sIndices, 0, sIndices, 0, BLOCK_SIZE);
            System.arraycopy(blockToCopy.pIndices, 0, pIndices, 0, BLOCK_SIZE);
            System.arraycopy(blockToCopy.oIndices, 0, oIndices, 0, BLOCK_SIZE);
            this.currentIndexSize = blockToCopy.currentIndexSize;
        }
    }
}

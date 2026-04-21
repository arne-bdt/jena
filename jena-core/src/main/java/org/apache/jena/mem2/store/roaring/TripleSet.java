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

import org.apache.jena.atlas.lib.Copyable;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.collection.FastHashSet;

/**
 * Set of triples that is backed by a {@link TripleSet}.
 */
public class TripleSet
        extends FastHashSet<Triple>
        implements Copyable<TripleSet> {

    private int[][] indexListPositions;

    public TripleSet() {
        super();
        indexListPositions = new int[3][keys.length];
    }

    private TripleSet(final TripleSet setToCopy) {
        super(setToCopy);
        indexListPositions = new int[3][keys.length];
        copyIndexPositions(setToCopy.indexListPositions, this.indexListPositions);
    }

    private static void copyIndexPositions(int [][] source, int [][] target) {
        System.arraycopy(source[0], 0, target[0], 0, source[0].length);
        System.arraycopy(source[1], 0, target[1], 0, source[1].length);
        System.arraycopy(source[2], 0, target[2], 0, source[2].length);
    }

    @Override
    protected Triple[] newKeysArray(int size) {
        return new Triple[size];
    }

    /**
     * Create a copy of this set.
     *
     * @return TripleSet
     */
    @Override
    public TripleSet copy() {
        return new TripleSet(this);
    }

    @Override
    protected void growKeysAndHashCodeArrays() {
        super.growKeysAndHashCodeArrays();
        final var oldPositions = indexListPositions;
        indexListPositions = new int[3][keys.length];
        copyIndexPositions(oldPositions, indexListPositions);
    }

    public Triple[] getTriples() {
        return keys;
    }

    public int getListPosition(final int tripleIndex, final int spoIndex) {
        return this.indexListPositions[spoIndex][tripleIndex];
    }

    public void setListPosition(final int tripleIndex, final int spoIndex, final int position) {
        this.indexListPositions[spoIndex][tripleIndex] = position;
    }

    public int[] getListPositions(final int spoIndex) {
        return this.indexListPositions[spoIndex];
    }

    public boolean intersects(final int spoIndexA, final IndexList a, final int spoIndexB, final IndexList b) {
        if (a.size() < b.size()) {
            return intersectsSmallerWithLarger(a, b, this.getListPositions(spoIndexB));
        } else {
            return intersectsSmallerWithLarger(b, a, this.getListPositions(spoIndexA));
        }
    }

    private boolean intersectsSmallerWithLarger(final IndexList smaller, final IndexList larger, final int[] positionsLarger) {
        final var largerSize = larger.size();
        var pos = smaller.lastPos();
        while (-1 < pos) {
            final var tripleIndex = smaller.getIndexAt(pos--);
            final var potentialIndexInLarger = positionsLarger[tripleIndex];
            if(potentialIndexInLarger < largerSize) {
                if(tripleIndex == larger.getIndexAt(potentialIndexInLarger)) {
                    return true;
                }
            }
        }
        return false;
    }
}

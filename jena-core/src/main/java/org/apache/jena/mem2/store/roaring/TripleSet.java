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

    private int[] sIndices;
    private int[] pIndices;
    private int[] oIndices;

    public TripleSet() {
        super();
        sIndices = new int[keys.length];
        pIndices = new int[keys.length];
        oIndices = new int[keys.length];
    }

    private TripleSet(final TripleSet setToCopy) {
        super(setToCopy);
        sIndices = new int[keys.length];
        pIndices = new int[keys.length];
        oIndices = new int[keys.length];
        System.arraycopy(setToCopy.sIndices, 0, sIndices, 0, setToCopy.keysPos);
        System.arraycopy(setToCopy.pIndices, 0, pIndices, 0, setToCopy.keysPos);
        System.arraycopy(setToCopy.oIndices, 0, oIndices, 0, setToCopy.keysPos);
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
        final var oldSIndices = sIndices;
        final var oldPIndices = pIndices;
        final var oldOIndices = oIndices;
        sIndices = new int[keys.length];
        pIndices = new int[keys.length];
        oIndices = new int[keys.length];
        System.arraycopy(oldSIndices, 0, sIndices, 0, oldSIndices.length);
        System.arraycopy(oldPIndices, 0, pIndices, 0, oldPIndices.length);
        System.arraycopy(oldOIndices, 0, oIndices, 0, oldOIndices.length);
    }

    public int getFilledLength() {
        return keysPos;
    }

    public Triple[] getTriples() {
        return keys;
    }

    public int getSIndex(final int tripleIndex) {
        return this.sIndices[tripleIndex];
    }

    public int getPIndex(final int tripleIndex) {
        return this.pIndices[tripleIndex];
    }

    public int getOIndex(final int tripleIndex) {
        return this.oIndices[tripleIndex];
    }

    public void setSIndex(final int tripleIndex, final int sIndex) {
        this.sIndices[tripleIndex] = sIndex;
    }
    public void setPIndex(final int tripleIndex, final int pIndex) {
        this.pIndices[tripleIndex] = pIndex;
    }
    public void setOIndex(final int tripleIndex, final int oIndex) {
        this.oIndices[tripleIndex] = oIndex;
    }

    public int[] getSIndices() {
        return this.sIndices;
    }
    public int[] getPIndices() {
        return this.pIndices;
    }
    public int[] getOIndices() {
        return this.oIndices;
    }

    public boolean intersects(final IndexList a, final int[] spoIndicesA, final IndexList b, final int[] spoIndicesB) {
        if (a.size() < b.size()) {
            return intersectsSmallerWithLarger(a, b, spoIndicesB);
        } else {
            return intersectsSmallerWithLarger(b, a, spoIndicesA);
        }
    }

    private boolean intersectsSmallerWithLarger(final IndexList smaller, final IndexList larger, final int[] spoIndicesLarger) {
        final var largerSize = larger.size();
        var pos = smaller.lastPos();
        while (-1 < pos) {
            final var tripleIndex = smaller.getIndexAt(pos--);
            final var potentialIndexInLarger = spoIndicesLarger[tripleIndex];
            if(potentialIndexInLarger < largerSize) {
                if(tripleIndex == larger.getIndexAt(potentialIndexInLarger)) {
                    return true;
                }
            }
        }
        return false;
    }
}

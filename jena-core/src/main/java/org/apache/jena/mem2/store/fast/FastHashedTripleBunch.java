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
package org.apache.jena.mem2.store.fast;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.collection.FastHashSet;
import org.apache.jena.mem2.collection.JenaSet;

/**
 * A set of triples - backed by {@link FastHashSet}.
 */
public class FastHashedTripleBunch extends FastHashSet<Triple> implements FastTripleBunch {

    private int[][] indexListPositions;

    /**
     * Create a new triple bunch from the given set of triples.
     *
     * @param set the set of triples
     */
    public FastHashedTripleBunch(final FastTripleBunch set) {
        super((set.size() >> 1) + set.size()); //it should not only fit but also have some space for growth
        set.keyIterator().forEachRemaining(this::addUnchecked);
        indexListPositions = new int[2][keys.length];
        FastTripleBunch.copyIndexPositions(set.getIndexListPositions(), this.indexListPositions, set.size());
    }

    /**
     * Copy constructor.
     * The new bunch will contain all the same triples of the bunch to copy.
     *
     * @param bunchToCopy
     */
    private FastHashedTripleBunch(final FastHashedTripleBunch bunchToCopy) {
        super(bunchToCopy);
        indexListPositions = new int[2][keys.length];
        FastTripleBunch.copyIndexPositions(bunchToCopy.indexListPositions, this.indexListPositions, bunchToCopy.size());
    }

    public FastHashedTripleBunch() {
        super();
        indexListPositions = new int[2][keys.length];
    }

    @Override
    protected Triple[] newKeysArray(int size) {
        return new Triple[size];
    }

    @Override
    protected void growKeysAndHashCodeArrays() {
        super.growKeysAndHashCodeArrays();
        final var oldPositions = indexListPositions;
        indexListPositions = new int[2][keys.length];
        FastTripleBunch.copyIndexPositions(oldPositions, indexListPositions, oldPositions[0].length);
    }

    @Override
    public boolean isArray() {
        return false;
    }

    @Override
    public void setIndices(int atIndex, int[] opIndices) {
        this.indexListPositions[0][atIndex] = opIndices[0];
        this.indexListPositions[1][atIndex] = opIndices[1];
    }

    @Override
    public void setIndices(int atIndex, int pIndex, int oIndex) {
        this.indexListPositions[0][atIndex] = pIndex;
        this.indexListPositions[1][atIndex] = oIndex;
    }

    @Override
    public void setPIndex(int atIndex, int pIndex) {
        this.indexListPositions[0][atIndex] = pIndex;
    }

    @Override
    public void setOIndex(int atIndex, int oIndex) {
        this.indexListPositions[1][atIndex] = oIndex;
    }

    @Override
    public int getPIndex(int atIndex) {
        return indexListPositions[0][atIndex];
    }

    @Override
    public int getOIndex(int atIndex) {
        return indexListPositions[1][atIndex];
    }

    @Override
    public int getIndex(int atIndex, int listIndex) {
        return indexListPositions[listIndex][atIndex];
    }

    @Override
    public int[] getIndices(int atIndex) {
        return new int[]{indexListPositions[0][atIndex], indexListPositions[1][atIndex]};
    }

    @Override
    public int[][] getIndexListPositions() {
        return indexListPositions;
    }

    @Override
    public FastHashedTripleBunch copy() {
        return new FastHashedTripleBunch(this);
    }
}

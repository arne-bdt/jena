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

/**
 * An ArrayBunch implements TripleBunch with a linear search of a short-ish
 * array of Triples. The array grows by factor 2.
 */
public class DoubleIndexList {

    private static final int[] EMPTY_ARRAY = new int[0];

    private static final int INITIAL_SIZE = 4;

    private int pos = -1;
    private int[] subjectIndex;
    private int[] elementIndex;

    public DoubleIndexList() {
        subjectIndex = new int[INITIAL_SIZE];
        elementIndex = new int[INITIAL_SIZE];
    }

    /**
     * Copy constructor.
     * The new bunch will contain all the same triples of the bunch to copy.
     * But it will reserve only the space needed to contain them. Growing is still possible.
     *
     * @param bunchToCopy
     */
    public DoubleIndexList(final DoubleIndexList bunchToCopy) {
        this.subjectIndex = new int[bunchToCopy.size()];
        System.arraycopy(bunchToCopy.subjectIndex, 0, this.subjectIndex, 0, bunchToCopy.size());
        this.elementIndex = new int[bunchToCopy.size()];
        System.arraycopy(bunchToCopy.elementIndex, 0, this.elementIndex, 0, bunchToCopy.size());
        this.pos = bunchToCopy.pos;
    }

    public int size() {
        return pos + 1;
    }

    public int lastPos() {
        return pos;
    }

    public boolean isEmpty() {
        return this.pos == -1;
    }

    public int[] getElementIndices() {
        return elementIndex;
    }

    public int[] getSubjectIndices() {
        return subjectIndex;
    }

    public int getCurrentPosition() {
        return pos;
    }

    public int getElementIndexAt(final int pos) {
        return this.elementIndex[pos];
    }

    public int getSubjectIndexAt(final int pos) {
        return this.subjectIndex[pos];
    }

    /**
     * Adds the given index to the end of the list, and returns the position of the inserted element.
     * @param elementIndex The element to add to the list
     * @return The position at which the element was added.
     */
    public int add(final int subjectIndex, final int elementIndex) {
        if (++pos == this.elementIndex.length) grow();
        this.subjectIndex[pos] = subjectIndex;
        this.elementIndex[pos] = elementIndex;
        return pos;
    }

    /**
     * Grows by approx. factor 1.5
     */
    private void grow() {
        final var oldSubjectIndex = subjectIndex;
        final var oldElements = elementIndex;
        final var newSize = elementIndex.length < 32
                ? elementIndex.length << 1
                : (elementIndex.length >> 1) + elementIndex.length;

        subjectIndex = new int[newSize];
        System.arraycopy(oldSubjectIndex, 0, subjectIndex, 0, oldSubjectIndex.length);
        elementIndex = new int[newSize];
        System.arraycopy(oldElements, 0, elementIndex, 0, oldElements.length);
    }

    /**
     * Removes the element at the given position, and returns an array of two ints:
     * The element that was moved to fill the gap, and the new position of that element.
     * @param position The position at which an element should be removed.
     * @return An array of two ints: The element that was moved to fill the gap, and the new position of that element.
     *         If no element was moved, an empty array is returned.
     */
    public int[] removeAt(final int position) {
        if(pos == position) {
            pos--;
            return EMPTY_ARRAY;
        } else {
            subjectIndex[position] = subjectIndex[pos];
            elementIndex[position] = elementIndex[pos--];
            return new int[] { subjectIndex[position], elementIndex[position] };
        }
    }

    public DoubleIndexList copy() {
        return new DoubleIndexList(this);
    }
}

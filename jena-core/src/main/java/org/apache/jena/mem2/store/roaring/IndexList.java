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

/**
 * An ArrayBunch implements TripleBunch with a linear search of a short-ish
 * array of Triples. The array grows by factor 2.
 */
public class IndexList {

    private static final int[] EMPTY_ARRAY = new int[0];

    private static final int INITIAL_SIZE = 4;

    private int pos = -1;
    private int[] elements;

    public IndexList() {
        elements = new int[INITIAL_SIZE];
    }

    /**
     * Copy constructor.
     * The new bunch will contain all the same triples of the bunch to copy.
     * But it will reserve only the space needed to contain them. Growing is still possible.
     *
     * @param bunchToCopy
     */
    public IndexList(final IndexList bunchToCopy) {
        this.elements = new int[bunchToCopy.size()];
        System.arraycopy(bunchToCopy.elements, 0, this.elements, 0, bunchToCopy.size());
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

    public int[] getIndices() {
        return elements;
    }

    public int getCurrentPosition() {
        return pos;
    }

    public int getIndexAt(final int pos) {
        return this.elements[pos];
    }

    /**
     * Adds the given index to the end of the list, and returns the position of the inserted element.
     * @param element The element to add to the list
     * @return The position at which the element was added.
     */
    public int add(final int element) {
        if (++pos == elements.length) grow();
        elements[pos] = element;
        return pos;
    }

    /**
     * Grows by approx. factor 1.5
     */
    private void grow() {
        final var oldElements = elements;
        final var newSize = elements.length < 32
                ? elements.length << 1
                : (elements.length >> 1) + elements.length;
        elements = new int[newSize];
        System.arraycopy(oldElements, 0, elements, 0, pos);
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
            elements[position] = --pos;
            return EMPTY_ARRAY;
        } else {
            elements[position] = elements[pos--];
            return new int[] { elements[position], position };
        }
    }

    public IndexList clone() {
        return new IndexList(this);
    }
}

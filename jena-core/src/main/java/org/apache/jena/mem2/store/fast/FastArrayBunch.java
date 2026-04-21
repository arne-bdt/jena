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
import org.apache.jena.mem2.spliterator.ArraySpliterator;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * An ArrayBunch implements TripleBunch with a linear search of a short-ish
 * array of Triples. The array grows by factor 2.
 */
public abstract class FastArrayBunch implements FastTripleBunch {

    private static final int INITIAL_SIZE = 4;

    protected int size = 0;
    protected Triple[] elements;
    private int[][] indexListPositions;

    protected FastArrayBunch() {
        elements = new Triple[INITIAL_SIZE];
        indexListPositions = new int[2][INITIAL_SIZE];
    }

    /**
     * Copy constructor.
     * The new bunch will contain all the same triples of the bunch to copy.
     * But it will reserve only the space needed to contain them. Growing is still possible.
     *
     * @param bunchToCopy
     */
    protected FastArrayBunch(final FastArrayBunch bunchToCopy) {
        this.elements = new Triple[bunchToCopy.size];
        System.arraycopy(bunchToCopy.elements, 0, this.elements, 0, bunchToCopy.size);
        this.size = bunchToCopy.size;
        this.indexListPositions = new int[2][bunchToCopy.indexListPositions.length];
        FastTripleBunch.copyIndexPositions(bunchToCopy.indexListPositions, this.indexListPositions, bunchToCopy.size);
    }

    public abstract boolean areEqual(final Triple a, final Triple b);

    @Override
    public boolean containsKey(Triple t) {
        int i = size;
        while (i > 0) if (areEqual(t, elements[--i])) return true;
        return false;
    }

    @Override
    public boolean anyMatch(final Predicate<Triple> predicate) {
        int i = size;
        while (i > 0) if (predicate.test(elements[--i])) return true;
        return false;
    }

    @Override
    public boolean anyMatchRandomOrder(Predicate<Triple> predicate) {
        return anyMatch(predicate);
    }

    @Override
    public void clear() {
        this.elements = new Triple[INITIAL_SIZE];
        this.size = 0;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return this.size == 0;
    }

    @Override
    public boolean tryAdd(final Triple t) {
        if (this.containsKey(t)) return false;
        if (size == elements.length) grow();
        elements[size++] = t;
        return true;
    }

    @Override
    public int addAndGetIndex(Triple t, int hashCode) {
        if (this.containsKey(t)) return -1;
        if (size == elements.length) grow();
        final var index = size++;
        elements[index] = t;
        return index;
    }

    @Override
    public void addUnchecked(final Triple t) {
        if (size == elements.length) grow();
        elements[size++] = t;
    }

    /**
     * Note: linear growth is suboptimal (order n<sup>2</sup>) normally, but
     * ArrayBunch's are meant for <i>small</i> sets and are replaced by some
     * sort of hash- or tree- set when they get big; currently "big" means more
     * than 9 elements, so that's only one growth spurt anyway.
     */
    protected void grow() {
        final var oldElements = elements;
        elements = new Triple[size << 1];
        System.arraycopy(oldElements, 0, elements, 0, size);

        final var oldIndexListPositions = indexListPositions;
        indexListPositions = new int[2][elements.length];
        FastTripleBunch.copyIndexPositions(oldIndexListPositions, indexListPositions, size);
    }

    @Override
    public boolean tryRemove(final Triple t) {
        for (int i = 0; i < size; i++) {
            if (areEqual(t, elements[i])) {
                elements[i] = elements[--size];
                elements[size] = null;
                return true;
            }
        }
        return false;
    }

    @Override
    public int removeAt(int index) {
        if(--size == index) {
            elements[size] = null;
            return -1;
        } else {
            elements[index] = elements[size];
            elements[size] = null;
            return size;
        }
    }

    @Override
    public void removeUnchecked(final Triple t) {
        for (int i = 0; i < size; i++) {
            if (areEqual(t, elements[i])) {
                elements[i] = elements[--size];
                elements[size] = null;
                return;
            }
        }
    }

    @Override
    public ExtendedIterator<Triple> keyIterator() {
        return new NiceIterator<>() {
            private final int initialSize = size;

            private int i = 0;

            @Override
            public boolean hasNext() {
                return i < size;
            }

            @Override
            public Triple next() {
                if (size != initialSize) throw new ConcurrentModificationException();
                if (i == size) throw new NoSuchElementException();
                return elements[i++];
            }

            @Override
            public void forEachRemaining(Consumer<? super Triple> action) {
                while (i < size) action.accept(elements[i++]);
                if (size != initialSize) throw new ConcurrentModificationException();
            }
        };

    }


    @Override
    public Spliterator<Triple> keySpliterator() {
        final var initialSize = size;
        final Runnable checkForConcurrentModification = () -> {
            if (size != initialSize) throw new ConcurrentModificationException();
        };
        return new ArraySpliterator<>(elements, size, checkForConcurrentModification);
    }

    @Override
    public boolean isArray() {
        return true;
    }

    @Override
    public boolean tryAdd(Triple key, int hashCode) {
        return tryAdd(key);
    }

    @Override
    public void addUnchecked(Triple key, int hashCode) {
        addUnchecked(key);
    }

    @Override
    public boolean tryRemove(Triple key, int hashCode) {
        return tryRemove(key);
    }

    @Override
    public void removeUnchecked(Triple key, int hashCode) {
        removeUnchecked(key);
    }

    @Override
    public int indexOf(Triple key) {
        int i = size;
        while (-1 < --i) if (areEqual(key, elements[i])) return i;
        return -1;
    }

    @Override
    public Triple getKeyAt(int index) {
        return elements[index];
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
    public int getIndex(int atIndex, int listIndex) {
        return indexListPositions[listIndex][atIndex];
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
    public int[] getIndices(int atIndex) {
        return new int[]{indexListPositions[0][atIndex], indexListPositions[1][atIndex]};
    }

    @Override
    public int[][] getIndexListPositions() {
        return indexListPositions;
    }
}

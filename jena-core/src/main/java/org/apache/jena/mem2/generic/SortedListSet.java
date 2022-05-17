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

package org.apache.jena.mem2.generic;

import java.util.*;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * ArrayList which is sorted when the size reaches a given size.
 * @param <E>
 */
public class SortedListSet<E> extends SortedListSetBase<E> {
    private final Comparator<E> comparator;
    private final int sizeToStartSorting;
    private static int DEFAULT_SIZE_TO_START_SORTING = 15;

    /**
     * Constructs an empty list with the specified initial capacity.
     *
     * @param initialCapacity the initial capacity of the list
     * @throws IllegalArgumentException if the specified initial capacity
     *                                  is negative
     */
    public SortedListSet(final int initialCapacity, final Comparator<E> comparator, final int sizeToStartSorting) {
        super(initialCapacity);
        this.comparator = comparator;
        this.sizeToStartSorting = sizeToStartSorting;
    }

    /**
     * Constructs a list containing the elements of the specified
     * collection, in the order they are returned by the collection's
     * iterator.
     *
     * @param c the collection whose elements are to be placed into this list
     * @throws NullPointerException if the specified collection is null
     */
    public SortedListSet(final Set<? extends E> c, final Comparator<E> comparator, final int sizeToStartSorting) {
        super(c);
        this.comparator = comparator;
        this.sizeToStartSorting = sizeToStartSorting;
        super.sort(comparator);
    }

    /**
     * Constructs an empty list with an initial capacity of ten.
     */
    public SortedListSet(final Comparator<E> comparator, final int sizeToStartSorting) {
        super();
        this.comparator = comparator;
        this.sizeToStartSorting = sizeToStartSorting;
    }

    /**
     * Constructs an empty list with the specified initial capacity.
     *
     * @param initialCapacity the initial capacity of the list
     * @throws IllegalArgumentException if the specified initial capacity
     *                                  is negative
     */
    public SortedListSet(final int initialCapacity, final Comparator<E> comparator) {
        this(initialCapacity, comparator, DEFAULT_SIZE_TO_START_SORTING);
    }

    /**
     * Constructs a list containing the elements of the specified
     * collection, in the order they are returned by the collection's
     * iterator.
     *
     * @param c the collection whose elements are to be placed into this list
     * @throws NullPointerException if the specified collection is null
     */
    public SortedListSet(final Set<? extends E> c, final Comparator<E> comparator) {
        this(c, comparator, DEFAULT_SIZE_TO_START_SORTING);
    }

    /**
     * Constructs an empty list with an initial capacity of ten.
     */
    public SortedListSet(final Comparator<E> comparator) {
        this(comparator, DEFAULT_SIZE_TO_START_SORTING);
    }

    @Override
    protected int getSizeToStartSorting() {
        return this.sizeToStartSorting;
    }

    @Override
    protected Comparator<E> getComparator() {
        return this.comparator;
    }
}

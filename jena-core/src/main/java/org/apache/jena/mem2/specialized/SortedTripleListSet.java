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

package org.apache.jena.mem2.specialized;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.generic.SortedListSetBase;
import org.apache.jena.mem2.helper.TripleEqualsOrMatches;

import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.function.Predicate;

public class SortedTripleListSet extends SortedListSetBase<Triple> {

    /**
     * Constructs an empty list with the specified initial capacity.
     *
     * @param initialCapacity the initial capacity of the list
     * @throws IllegalArgumentException if the specified initial capacity
     *                                  is negative
     */
    public SortedTripleListSet(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * Constructs a list containing the elements of the specified
     * collection, in the order they are returned by the collection's
     * iterator.
     *
     * @param c the collection whose elements are to be placed into this list
     * @throws NullPointerException if the specified collection is null
     */
    public SortedTripleListSet(Set<? extends Triple> c) {
        super(c);
    }

    /**
     * Constructs an empty list with an initial capacity of ten.
     */
    public SortedTripleListSet() {
    }

    private static int DEFAULT_SIZE_TO_START_SORTING = 15;

    private static Comparator<Triple> TRIPLE_INDEXING_VALUE_HASH_CODE_COMPARATOR =
            Comparator.comparingInt((Triple t) -> t.getSubject().getIndexingValue().hashCode())
            .thenComparing(t -> t.getObject().getIndexingValue().hashCode())
            .thenComparing(t -> t.getPredicate().getIndexingValue().hashCode());



    @Override
    protected int getSizeToStartSorting() {
        return DEFAULT_SIZE_TO_START_SORTING;
    }

    @Override
    protected Comparator<Triple> getComparator() {
        return TRIPLE_INDEXING_VALUE_HASH_CODE_COMPARATOR;
    }

    @Override
    protected Predicate<Object> getContainsPredicate(Triple value) {
        if(TripleEqualsOrMatches.isEqualsForObjectOk(value.getObject())) {
            return t -> value.equals(t);
        }
        return t -> value.matches((Triple) t);
    }
}

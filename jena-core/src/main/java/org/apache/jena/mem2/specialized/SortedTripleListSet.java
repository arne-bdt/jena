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
import org.apache.jena.mem2.generic.SortedListSet;
import org.apache.jena.mem2.helper.TripleEqualsOrMatches;

import java.util.Comparator;
import java.util.function.Predicate;

public class SortedTripleListSet extends SortedListSet<Triple> {

    private static Comparator<Triple> TRIPLE_INDEXING_VALUE_HASH_CODE_COMPARATOR =
            Comparator.comparingInt((Triple t) -> t.getSubject().getIndexingValue().hashCode())
            .thenComparing(t -> t.getObject().getIndexingValue().hashCode())
            .thenComparing(t -> t.getPredicate().getIndexingValue().hashCode());

    /**
     * Constructs an empty list with the specified initial capacity.
     *
     * @param initialCapacity the initial capacity of the list
     * @throws IllegalArgumentException if the specified initial capacity
     *                                  is negative
     */
    public SortedTripleListSet(int initialCapacity) {
        super(initialCapacity, TRIPLE_INDEXING_VALUE_HASH_CODE_COMPARATOR);
    }

    /**
     * Constructs an empty list with the specified initial capacity.
     *
     * @param initialCapacity    the initial capacity of the list
     * @param sizeToStartSorting
     * @throws IllegalArgumentException if the specified initial capacity
     *                                  is negative
     */
    public SortedTripleListSet(int initialCapacity, int sizeToStartSorting) {
        super(initialCapacity, TRIPLE_INDEXING_VALUE_HASH_CODE_COMPARATOR, sizeToStartSorting);
    }



    @Override
    protected Predicate<Object> getContainsPredicate(Triple value) {
        if(TripleEqualsOrMatches.isEqualsForObjectOk(value.getObject())) {
            return t -> value.equals(t);
        }
        return t -> value.matches((Triple) t);
    }
}

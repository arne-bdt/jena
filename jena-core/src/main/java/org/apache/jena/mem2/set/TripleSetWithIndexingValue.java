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

package org.apache.jena.mem2.set;

import org.apache.jena.graph.Triple;

import java.util.Set;

/**
 * A set of triples that can be indexed by a value.
 */
public interface TripleSetWithIndexingValue extends Set<Triple> {

    /**
     * The indexing value is a node that all triples in the set have in common.
     * It should be either a subject,a predicate or an object that all triples in the set have in common.
     */
    Object getIndexingValue();

    void addUnsafe(Triple t);

    boolean areOperationsWithHashCodesSupported();

    default boolean add(Triple t, int hashCode) {
        throw new UnsupportedOperationException("This default implementation only exists to avoid casts and keep the compiler calm.");
    }

    default void addUnsafe(Triple t, int hashCode) {
        throw new UnsupportedOperationException("This default implementation only exists to avoid casts and keep the compiler calm.");
    }

    default boolean remove(Triple t, int hashCode) {
        throw new UnsupportedOperationException("This default implementation only exists to avoid casts and keep the compiler calm.");
    }

    void removeUnsafe(Triple t);

    default void removeUnsafe(Triple t, int hashCode) {
        throw new UnsupportedOperationException("This default implementation only exists to avoid casts and keep the compiler calm.");
    }
}

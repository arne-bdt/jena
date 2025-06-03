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

package org.apache.jena.mem2;

import org.apache.jena.graph.Graph;

/**
 * An enumeration that represents different indexing strategies for a graph.
 * The indexing strategy determines how triples are indexed to support pattern matching.
 * It is assumed that the graph contains a set of triples, and all operations that do not involve
 * pattern matching are performed directly on this set, not on the indices.
 * <br>
 * Pattern matching refers to operations like {@link Graph#find}, {@link Graph#remove} or {@link Graph#contains}
 * that may take a triple pattern as argument, such as "S__", "SP_", "S_O", "_P_", "_PO", or "__O",
 * instead of a concrete triple "SPO".
 * In the case of a concrete triple these operations should be performed directly on the set of triples
 * and not rely on the indices.
 */
public enum IndexingStrategy {

    /**
     * Starts with all indices as any other in-memory graph.
     * {@link Graph#add}, {@link Graph#delete} and {@link Graph#clear()} update all indices immediately.
     * Clearing the indices is not possible.
     */
    EAGER,

    /**
     * Starts with no indices and builds them on demand when pattern matches are requested.
     * After initialization, the indices behave like EAGER.
     * Indices may be cleared manually, then they are rebuilt on demand.
     */
    LAZY,

    /**
     * Starts with no indices and throws an exception if a pattern match is requested,
     * but indices have not been initialized manually yet.
     * After initialization, the indices behave like EAGER.
     * Indices may be cleared manually, then they have to be initialized again manually.
     */
    MANUAL,

    /**
     * Starts with no indices and uses filtering on the triple set,
     * as long indices have not been initialized.
     * After initialization, the indices behave like EAGER.
     * Indices may be cleared manually, then filtering is used again until indices are initialized again.
     */
    MINIMAL
}

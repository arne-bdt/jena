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
 * An enumeration representing different indexing strategies for a graph.
 */
public enum IndexingStrategy {

    /**
     * Starts with all indices as any other in-memory graph.
     * {@link Graph#add}, {@link Graph#remove} and {@link Graph#clear()} update all indices immediately.
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

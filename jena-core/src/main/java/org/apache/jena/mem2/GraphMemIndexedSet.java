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

package org.apache.jena.mem2;

import org.apache.jena.mem2.store.TripleStore;
import org.apache.jena.mem2.store.indexed.IndexedSetTripleStore;

/**
 * A graph that stores triples in memory. This class is not thread-safe.
 * This in-memory graph supports different indexing strategies to balance
 * RAM usage and performance for various operations.
 * See {@link IndexingStrategy} for details on the available strategies.
 * <p>
 * As long as the indexInBlock has not been initialized, the memory consumption
 * is very low and the following operations are extremely fast:
 * <ul>
 *     <li>{@link GraphMem#add} - adds a triple to the graph</li>
 *     <li>{@link GraphMem#delete} - removes a triple from the graph</li>
 *</ul>
 * One could start without the indexInBlock, add all triples, and then initialize the indexInBlock using
 * {@link #initializeIndexParallel()} for maximum performance.
 */
public class GraphMemIndexedSet extends GraphMem {

    public GraphMemIndexedSet() {
        this(IndexingStrategy.EAGER);
    }

    public GraphMemIndexedSet(IndexingStrategy indexingStrategy) {
        super(new IndexedSetTripleStore(indexingStrategy));
    }

    private GraphMemIndexedSet(final TripleStore tripleStore) {
        super(tripleStore);
    }

    @Override
    public GraphMemIndexedSet copy() {
        return new GraphMemIndexedSet(this.tripleStore.copy());
    }

    private IndexedSetTripleStore getIndexedSetTripleStore() {
        return (IndexedSetTripleStore) this.tripleStore;
    }

    /**
     * Returns the indexing strategy used by this graph.
     *
     * @return the indexing strategy
     */
    public IndexingStrategy getIndexingStrategy() {
        return getIndexedSetTripleStore().getIndexingStrategy();
    }

    /**
     * Clear the indexInBlock of this graph.
     * This will remove all triples from the indexInBlock and reset the current strategy to the initial one.
     */
    public void clearIndex() {
        getIndexedSetTripleStore().clearIndex();
    }

    public void initializeIndex() {
        getIndexedSetTripleStore().initializeIndex();
    }

    /**
     * Initialize the indexInBlock of this graph in parallel.
     * This will build the indexInBlock based on the current set of triples using parallel processing.
     * After this call, the graph will behave like an EAGER indexed graph.
     */
    public void initializeIndexParallel() {
        getIndexedSetTripleStore().initializeIndexParallel();
    }

    /**
     * Check if the indexInBlock of this graph is initialized.
     * This method returns true if the indexInBlock has been initialized and is ready for use.
     *
     * @return true if the indexInBlock is initialized, false otherwise
     */
    public boolean isIndexInitialized() {
        return getIndexedSetTripleStore().isIndexInitialized();
    }
}

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

package org.apache.jena.mem;

import org.apache.jena.mem.store.mvcc.MvccTripleStore;

/**
 * Non-transactional, in-memory {@link GraphMem} backed by an
 * {@link MvccTripleStore}. This is the plain-graph counterpart of the
 * transactional {@code GraphMemIndexedSetMvccTxn}: it shares the same
 * version-stamped store engine but exposes only the ordinary {@link org.apache.jena.graph.Graph}
 * surface, with every {@code add}/{@code delete} applied as its own atomic commit
 * and every read served from the latest committed version. Like all
 * {@link GraphMem} variants it is <em>not</em> thread-safe.
 * <p>
 * Splitting the non-transactional behaviour out of the transactional graph lets
 * it be tested by {@link org.apache.jena.mem.AbstractGraphMemTest} alongside the
 * other in-memory graphs, and keeps the transactional class focused on
 * transaction management.
 * <p>
 * The MVCC store supports the {@link IndexingStrategy#EAGER EAGER},
 * {@link IndexingStrategy#MINIMAL MINIMAL} and {@link IndexingStrategy#MANUAL MANUAL}
 * strategies (read-triggered {@code LAZY} builds are unsafe for the store's
 * lock-free readers and are rejected). With {@code MANUAL}, pattern lookups fall
 * back to a dense scan until {@link #initializeIndex()} is called.
 */
public class GraphMemMvcc extends GraphMem {

    private final MvccTripleStore mvccTripleStore;

    /**
     * Creates a new graph using the {@link IndexingStrategy#EAGER} default
     * indexing strategy.
     */
    public GraphMemMvcc() {
        this(IndexingStrategy.EAGER);
    }

    /**
     * Creates a new graph that uses the given indexing strategy.
     *
     * @param indexingStrategy the indexing strategy to use; the MVCC store
     *                         supports {@code EAGER}, {@code MINIMAL} and
     *                         {@code MANUAL}
     */
    public GraphMemMvcc(IndexingStrategy indexingStrategy) {
        this(new MvccTripleStore(indexingStrategy));
    }

    /**
     * Internal constructor used by {@link #copy()} to wrap an already populated
     * store.
     *
     * @param tripleStore the store to wrap
     */
    private GraphMemMvcc(final MvccTripleStore tripleStore) {
        super(tripleStore);
        this.mvccTripleStore = tripleStore;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns an independent copy that preserves the indexing strategy and holds
     * the currently-live triples.
     */
    @Override
    public GraphMemMvcc copy() {
        return new GraphMemMvcc(this.mvccTripleStore.copy());
    }

    /**
     * Returns the indexing strategy this graph was created with. The strategy is
     * fixed for the lifetime of the graph.
     *
     * @return the indexing strategy
     */
    public IndexingStrategy getIndexingStrategy() {
        return mvccTripleStore.getIndexingStrategy();
    }

    /**
     * Reports whether the auxiliary index is currently built and serving pattern
     * lookups directly. For {@code EAGER} this is always {@code true}; for
     * {@code MINIMAL} and {@code MANUAL} it is {@code false} until
     * {@link #initializeIndex()} (or {@link #initializeIndexParallel()}) is called,
     * and {@code false} again after {@link #clearIndex()}.
     *
     * @return {@code true} iff the index is initialized
     */
    public boolean isIndexInitialized() {
        return mvccTripleStore.isIndexInitialized();
    }

    /**
     * Builds the index over the current triples. After this call pattern lookups
     * are served from the index, like an {@code EAGER} graph. A no-op for
     * {@code EAGER} (always indexed) and when the index is already built.
     */
    public void initializeIndex() {
        mvccTripleStore.versionControl().lockWriter();
        try {
            mvccTripleStore.initializeIndex();
        } finally {
            mvccTripleStore.versionControl().unlockWriter();
        }
    }

    /**
     * Like {@link #initializeIndex()} but builds the index in parallel, which can be
     * faster for larger graphs.
     */
    public void initializeIndexParallel() {
        mvccTripleStore.versionControl().lockWriter();
        try {
            mvccTripleStore.initializeIndexParallel();
        } finally {
            mvccTripleStore.versionControl().unlockWriter();
        }
    }

    /**
     * Clears the index and reverts to the configured indexing strategy's behaviour
     * ({@code MANUAL}/{@code MINIMAL} stop serving lookups from an index until it is
     * rebuilt). A no-op for {@code EAGER}, which stays fully indexed.
     */
    public void clearIndex() {
        mvccTripleStore.versionControl().lockWriter();
        try {
            mvccTripleStore.clearIndex();
        } finally {
            mvccTripleStore.versionControl().unlockWriter();
        }
    }
}

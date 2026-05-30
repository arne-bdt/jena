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

package org.apache.jena.sparql.core.mem.txn.helper;

import org.apache.jena.graph.Graph;
import org.apache.jena.mem.GraphMemIndexedSet;
import org.apache.jena.mem.IndexingStrategy;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.sparql.core.mem.DatasetGraphInMemory;
import org.apache.jena.sparql.core.mem.GraphMemIndexedSetCowTxn;
import org.apache.jena.sparql.core.mem.GraphMemIndexedSetCowTxn.ForkMode;
import org.apache.jena.sparql.core.mem.GraphMemIndexedSetMvccTxn;
import org.apache.jena.sparql.core.mem.GraphMemIndexedSetTxn;

/**
 * Helper for the transactional-graph JMH benchmarks under
 * {@code sparql.core.mem.txn}. Resolves the {@code @Param} string the
 * benchmark exposes to a fresh {@link Graph} instance and exposes a
 * couple of utility methods for driving load / read inside transactions.
 *
 * <h2>Variant strings</h2>
 * Each benchmark exposes one or more of these names as
 * {@code @Param} values. See {@link #createGraph(String)} for the full
 * list. The naming follows the convention used by the existing
 * {@code mem.graph.helper.Context}.
 */
public final class TxnGraphContext {

    private TxnGraphContext() {}

    // ----- Variant catalogue --------------------------------------

    /** Non-transactional baseline (no begin/commit needed). */
    public static final String GMIS_BASELINE_EAGER  = "GraphMemIndexedSet EAGER (current)";
    public static final String GMIS_BASELINE_LAZY   = "GraphMemIndexedSet LAZY (current)";

    /** DatasetGraphInMemory */
    public static final String GMIS_DEFAULT_IN_MEMORY = "DatasetGraphInMemory (current)";

    /** deep-copy on begin(WRITE). */
    public static final String GMIS_TXN_EAGER       = "GraphMemIndexedSetTxn EAGER";
    public static final String GMIS_TXN_LAZY        = "GraphMemIndexedSetTxn LAZY";

    /** copy-on-write fork at begin(WRITE). */
    public static final String GMIS_COW_TXN_EAGER_SEQ      = "GraphMemIndexedSetCowTxn EAGER SEQ";
    public static final String GMIS_COW_TXN_EAGER_PARALLEL = "GraphMemIndexedSetCowTxn EAGER PARALLEL";
    public static final String GMIS_COW_TXN_LAZY           = "GraphMemIndexedSetCowTxn LAZY";
    public static final String GMIS_COW_TXN_LAZY_PARALLEL  = "GraphMemIndexedSetCowTxn LAZY_PARALLEL";

    /** MVCC: version-stamped shared store, O(1) begin (no copy). */
    public static final String GMIS_MVCC_TXN_EAGER   = "GraphMemIndexedSetMvccTxn EAGER";
    public static final String GMIS_MVCC_TXN_MINIMAL = "GraphMemIndexedSetMvccTxn MINIMAL";
    public static final String GMIS_MVCC_TXN_MANUAL  = "GraphMemIndexedSetMvccTxn MANUAL";

    /**
     * @return a freshly constructed graph for the given variant name.
     * Throws {@link IllegalArgumentException} for an unknown name.
     */
    public static Graph createGraph(String variant) {
        return switch (variant) {
            case GMIS_BASELINE_EAGER ->
                    new GraphMemIndexedSet(IndexingStrategy.EAGER);
            case GMIS_BASELINE_LAZY ->
                    new GraphMemIndexedSet(IndexingStrategy.LAZY);

            case GMIS_DEFAULT_IN_MEMORY ->
                    new DatasetGraphInMemory().getDefaultGraph();

            case GMIS_TXN_EAGER ->
                    new GraphMemIndexedSetTxn(IndexingStrategy.EAGER);
            case GMIS_TXN_LAZY ->
                    new GraphMemIndexedSetTxn(IndexingStrategy.LAZY);

            case GMIS_COW_TXN_EAGER_SEQ ->
                    new GraphMemIndexedSetCowTxn(IndexingStrategy.EAGER, ForkMode.SEQUENTIAL);
            case GMIS_COW_TXN_EAGER_PARALLEL ->
                    new GraphMemIndexedSetCowTxn(IndexingStrategy.EAGER, ForkMode.PARALLEL);
            case GMIS_COW_TXN_LAZY ->
                    new GraphMemIndexedSetCowTxn(IndexingStrategy.LAZY, ForkMode.SEQUENTIAL);
            case GMIS_COW_TXN_LAZY_PARALLEL ->
                    new GraphMemIndexedSetCowTxn(IndexingStrategy.LAZY_PARALLEL, ForkMode.PARALLEL);

            case GMIS_MVCC_TXN_EAGER ->
                    new GraphMemIndexedSetMvccTxn(IndexingStrategy.EAGER);
            case GMIS_MVCC_TXN_MINIMAL ->
                    new GraphMemIndexedSetMvccTxn(IndexingStrategy.MINIMAL);
            case GMIS_MVCC_TXN_MANUAL ->
                    new GraphMemIndexedSetMvccTxn(IndexingStrategy.MANUAL);

            default -> throw new IllegalArgumentException(
                    "Unknown TxnGraphContext variant: " + variant);
        };
    }

    /**
     * Run {@code work} on {@code graph} inside a write transaction if the
     * graph is {@link Transactional}; otherwise just run the lambda.
     * Thin wrapper over {@link Transactional#executeWrite(Runnable)} that
     * handles the non-transactional baseline graphs.
     */
    public static void writeTxn(Graph graph, Runnable work) {
        if (graph instanceof Transactional t) {
            t.executeWrite(work);
        } else {
            work.run();
        }
    }

    /** Read-transaction wrapper analogue to {@link #writeTxn}. */
    public static void readTxn(Graph graph, Runnable work) {
        if (graph instanceof Transactional t) {
            t.executeRead(work);
        } else {
            work.run();
        }
    }
}

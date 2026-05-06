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

import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.mem.DatasetGraphInMemory;
import org.apache.jena.sparql.core.mem.DatasetGraphInMemoryCowTxn;
import org.apache.jena.sparql.core.mem.GraphMemIndexedSetCowTxn;

/**
 * Helper for the dataset-level transactional JMH benchmarks. Mirrors
 * {@link TxnGraphContext} one level up: maps a {@code @Param} variant
 * string to a fresh {@link DatasetGraph} so the same benchmark code can
 * compare {@link DatasetGraphInMemory} against the new
 * {@link DatasetGraphInMemoryCowTxn} (SEQUENTIAL and PARALLEL fork modes).
 */
public final class TxnDatasetContext {

    private TxnDatasetContext() {}

    /** Current default — used by {@code DatasetGraphFactory.createTxnMem()}. */
    public static final String DSG_IN_MEMORY = "DatasetGraphInMemory (current)";

    /** New copy-on-write dataset, sequential per-graph fork. */
    public static final String DSG_COW_TXN_SEQ = "DatasetGraphInMemoryCowTxn SEQ";

    /** New copy-on-write dataset, parallel per-graph fork. */
    public static final String DSG_COW_TXN_PARALLEL = "DatasetGraphInMemoryCowTxn PARALLEL";

    /** @return a freshly constructed {@link DatasetGraph} for the given variant. */
    public static DatasetGraph createDataset(String variant) {
        return switch (variant) {
            case DSG_IN_MEMORY ->
                    new DatasetGraphInMemory();
            case DSG_COW_TXN_SEQ ->
                    new DatasetGraphInMemoryCowTxn(GraphMemIndexedSetCowTxn.ForkMode.SEQUENTIAL);
            case DSG_COW_TXN_PARALLEL ->
                    new DatasetGraphInMemoryCowTxn(GraphMemIndexedSetCowTxn.ForkMode.PARALLEL);
            default ->
                    throw new IllegalArgumentException(
                            "Unknown TxnDatasetContext variant: " + variant);
        };
    }
}

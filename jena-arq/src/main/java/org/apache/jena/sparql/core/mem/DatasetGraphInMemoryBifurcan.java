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

package org.apache.jena.sparql.core.mem;

import org.apache.jena.sparql.core.DatasetGraph;

/**
 * A fully transactional, in-memory {@link DatasetGraph} that stores quads and triples in
 * <a href="https://github.com/lacuna/bifurcan">Bifurcan</a> CHAMP indexes ({@link BifurcanHexTable} +
 * {@link BifurcanTriTable}).
 * <p>
 * This is a drop-in alternative to {@link DatasetGraphInMemory} (which uses HAMT-based persistent maps): it maintains
 * the same six quad-index orders and three triple-index orders, and reuses {@code DatasetGraphInMemory}'s transaction
 * machinery, so it has the same query-pattern coverage and isolation semantics. The difference is the storage layer,
 * which uses Bifurcan's <em>linear</em> (transient) collections during write transactions to reduce per-operation
 * allocation in write-heavy phases, publishing an immutable snapshot at commit for concurrent readers.
 *
 * @see DatasetGraphInMemory
 * @see BifurcanTupleTable
 */
public class DatasetGraphInMemoryBifurcan extends DatasetGraphInMemory {

    /**
     * Default constructor: a {@link BifurcanHexTable} for quads and a {@link BifurcanTriTable} for the default graph.
     */
    public DatasetGraphInMemoryBifurcan() {
        super(new BifurcanHexTable(), new BifurcanTriTable());
    }
}

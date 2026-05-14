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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.Test;

/**
 * Regression: after a READ_*PROMOTE-style dataset transaction promotes to
 * WRITE and commits (or aborts), the next dataset transaction on the same
 * thread must not encounter "nested transactions" errors on the per-graph
 * stores. {@code begin(READ)} opens a per-graph READ txn eagerly on every
 * pinned graph; if the dataset writes to only some of them, the rest must
 * still be ended at commit/abort or their per-graph activeTxn ThreadLocal
 * is stranded.
 */
public class TestDatasetGraphInMemoryCowTxnReuseThread {

    private static Node uri(String x) { return NodeFactory.createURI("http://ex/" + x); }
    private static Quad nq(String g, String s, String p, String o) {
        return Quad.create(uri(g), uri(s), uri(p), uri(o));
    }

    private static DatasetGraph datasetWithThreeNamedGraphs() {
        DatasetGraph dsg = new DatasetGraphInMemoryCowTxn();
        dsg.executeWrite(() -> {
            dsg.add(nq("g1", "s", "p", "o"));
            dsg.add(nq("g2", "s", "p", "o"));
            dsg.add(nq("g3", "s", "p", "o"));
        });
        return dsg;
    }

    @Test
    public void promotedTxnReleasesNonEnlistedReads_commit() {
        DatasetGraph dsg = datasetWithThreeNamedGraphs();

        // Promote-style begin: per-graph READs are opened on g1/g2/g3.
        dsg.begin(TxnType.READ_PROMOTE);
        try {
            dsg.promote();
            dsg.add(nq("g1", "s2", "p", "o"));     // enlists g1 only
            dsg.commit();
        } finally {
            dsg.end();
        }

        // If the per-graph READs on g2/g3 were not ended, the next begin()
        // on this thread will throw "Nested transactions are not supported".
        assertDoesNotThrow(() -> dsg.executeRead(() ->
                assertEquals(2L, dsg.getGraph(uri("g1")).size())));
    }

    @Test
    public void promotedTxnReleasesNonEnlistedReads_abort() {
        DatasetGraph dsg = datasetWithThreeNamedGraphs();

        dsg.begin(TxnType.READ_PROMOTE);
        try {
            dsg.promote();
            dsg.add(nq("g1", "s2", "p", "o"));
            dsg.abort();
        } finally {
            dsg.end();
        }

        assertDoesNotThrow(() -> dsg.executeRead(() ->
                assertEquals(1L, dsg.getGraph(uri("g1")).size())));
    }

    @Test
    public void plainReadTxnReleasesPinnedReads() {
        // A pure READ that touches one graph should still end the per-graph
        // READ on every pinned graph at end() time. This was already handled
        // by endReadTxn, but check it here for regression-protection.
        DatasetGraph dsg = datasetWithThreeNamedGraphs();
        dsg.executeRead(() -> assertTrue(dsg.containsGraph(uri("g2"))));
        assertDoesNotThrow(() -> dsg.executeRead(() ->
                assertTrue(dsg.containsGraph(uri("g3")))));
    }

    @Test
    public void writeOnlyTxnIsCheapAndReusable() {
        // Pure begin(WRITE) does not open per-graph reads, so cleanup is a
        // no-op. Verify several back-to-back WRITE txns reuse the thread.
        DatasetGraph dsg = datasetWithThreeNamedGraphs();
        for (int i = 0; i < 5; i++) {
            final int n = i;
            dsg.executeWrite(() -> dsg.add(nq("g1", "s" + n, "p", "o")));
        }
        dsg.executeRead(() -> assertEquals(6L, dsg.getGraph(uri("g1")).size()));
    }
}

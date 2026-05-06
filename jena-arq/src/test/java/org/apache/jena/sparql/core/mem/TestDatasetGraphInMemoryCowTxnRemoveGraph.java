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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.Test;

/**
 * Tests that {@link DatasetGraphInMemoryCowTxn#removeGraph(Node)} actually
 * removes the named graph from the dataset's published topology, not just
 * empties its contents. Without the explicit removeGraph override, the
 * inherited behaviour would deleteAny and leave an empty per-graph store
 * pinned in the topology indefinitely.
 */
public class TestDatasetGraphInMemoryCowTxnRemoveGraph {

    private static Node uri(String x) { return NodeFactory.createURI("http://ex/" + x); }
    private static Quad nq(String g, String s, String p, String o) {
        return Quad.create(uri(g), uri(s), uri(p), uri(o));
    }

    @Test
    public void removeGraphDropsFromTopology() {
        DatasetGraph dsg = new DatasetGraphInMemoryCowTxn();
        dsg.executeWrite(() -> dsg.add(nq("g1", "s", "p", "o")));
        assertTrue(dsg.containsGraph(uri("g1")));

        dsg.executeWrite(() -> dsg.removeGraph(uri("g1")));

        // Topology must no longer list g1, and listGraphNodes must not
        // report it. Verify via a fresh read transaction.
        dsg.executeRead(() -> {
            assertFalse(dsg.containsGraph(uri("g1")));
            assertEquals(0L, Iter.count(dsg.listGraphNodes()));
        });
    }

    @Test
    public void removeGraphAbortRestoresGraph() {
        DatasetGraph dsg = new DatasetGraphInMemoryCowTxn();
        dsg.executeWrite(() -> dsg.add(nq("g1", "s", "p", "o")));

        dsg.begin(TxnType.WRITE);
        try {
            dsg.removeGraph(uri("g1"));
            assertFalse(dsg.containsGraph(uri("g1")));
            dsg.abort();
        } finally {
            dsg.end();
        }
        assertTrue(dsg.containsGraph(uri("g1")));
    }

    @Test
    public void removeAddedInSameTxnDropsCleanly() {
        DatasetGraph dsg = new DatasetGraphInMemoryCowTxn();
        dsg.executeWrite(() -> {
            dsg.add(nq("g1", "s", "p", "o"));      // adds g1
            dsg.removeGraph(uri("g1"));             // immediately removes
        });
        // The topology must not contain g1 — the addition was undone, the
        // pre-txn topology had no g1, so post-commit topology has none.
        assertFalse(dsg.containsGraph(uri("g1")));
    }

    @Test
    public void removeThenReAddInSameTxnYieldsFreshGraph() {
        DatasetGraph dsg = new DatasetGraphInMemoryCowTxn();
        dsg.executeWrite(() -> {
            dsg.add(nq("g1", "s1", "p", "o"));
            dsg.add(nq("g1", "s2", "p", "o"));
        });

        dsg.executeWrite(() -> {
            dsg.removeGraph(uri("g1"));
            // The original triples are gone; only the re-add lands.
            dsg.add(nq("g1", "s3", "p", "o"));
        });

        dsg.executeRead(() -> {
            assertTrue(dsg.containsGraph(uri("g1")));
            assertTrue(dsg.contains(
                    Quad.create(uri("g1"), uri("s3"), uri("p"), uri("o"))));
            assertFalse(dsg.contains(
                    Quad.create(uri("g1"), uri("s1"), uri("p"), uri("o"))));
            assertFalse(dsg.contains(
                    Quad.create(uri("g1"), uri("s2"), uri("p"), uri("o"))));
        });
    }

    @Test
    public void removeDefaultGraphClearsInPlace() {
        DatasetGraph dsg = new DatasetGraphInMemoryCowTxn();
        dsg.executeWrite(() -> dsg.getDefaultGraph().add(Triple.create(uri("s"), uri("p"), uri("o"))));
        dsg.executeWrite(() -> dsg.removeGraph(Quad.defaultGraphIRI));
        dsg.executeRead(() -> assertTrue(dsg.getDefaultGraph().isEmpty()));
    }

    @Test
    public void removeNonExistentGraphIsNoOp() {
        DatasetGraph dsg = new DatasetGraphInMemoryCowTxn();
        // Must not throw, must not leave the dataset in an inconsistent state.
        dsg.executeWrite(() -> dsg.removeGraph(uri("ghost")));
        assertFalse(dsg.containsGraph(uri("ghost")));
    }

    /**
     * Pins down the <i>topology</i>-level effect of {@code removeGraph}, not
     * just the user-visible content. {@code listGraphNodes} and
     * {@code containsGraph} both filter out empty graphs ({@code !g.isEmpty()}),
     * so even a buggy inherited {@code removeGraph} that merely empties the
     * per-graph store looks correct through those APIs. The actual bug —
     * the empty per-graph store stays pinned in {@code namedTopology}
     * forever — is invisible without peeking at the internal topology map.
     * Read it via reflection and assert the map shrinks.
     */
    @Test
    public void removeGraphActuallyShrinksTheTopologyMap() throws Exception {
        DatasetGraphInMemoryCowTxn dsg = new DatasetGraphInMemoryCowTxn();
        dsg.executeWrite(() -> dsg.add(nq("g1", "s", "p", "o")));
        assertEquals(1, namedTopologySize(dsg));

        dsg.executeWrite(() -> dsg.removeGraph(uri("g1")));

        assertEquals(0, namedTopologySize(dsg),
                "removeGraph must drop the entry from namedTopology, not just empty it");
    }

    /**
     * Same idea, in-transaction: add a graph and immediately remove it.
     * Pre-fix, the inherited {@code deleteAny} would empty the freshly
     * added graph but the addition would still be enlisted; the committed
     * topology would contain an empty pinned store. Post-fix, the addition
     * is dropped and the topology is unchanged from its pre-txn state.
     */
    @Test
    public void addThenRemoveInSameTxnLeavesTopologyUntouched() throws Exception {
        DatasetGraphInMemoryCowTxn dsg = new DatasetGraphInMemoryCowTxn();
        dsg.executeWrite(() -> {
            dsg.add(nq("g1", "s", "p", "o"));
            dsg.removeGraph(uri("g1"));
        });
        assertEquals(0, namedTopologySize(dsg));
    }

    /** Reflect into the volatile {@code namedTopology} slot and report its size. */
    private static int namedTopologySize(DatasetGraphInMemoryCowTxn dsg) throws Exception {
        Field f = DatasetGraphInMemoryCowTxn.class.getDeclaredField("namedTopology");
        f.setAccessible(true);
        Object topology = f.get(dsg);
        Method graphs = topology.getClass().getDeclaredMethod("graphs");
        graphs.setAccessible(true);
        Map<?, ?> m = (Map<?, ?>) graphs.invoke(topology);
        return m.size();
    }
}

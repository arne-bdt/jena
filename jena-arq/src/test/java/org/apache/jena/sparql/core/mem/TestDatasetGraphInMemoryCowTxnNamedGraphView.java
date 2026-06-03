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

import static org.apache.jena.sparql.core.mem.CowTxnTestHelper.t;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.GraphView;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.mem.GraphMemIndexedSetCowTxn.ForkMode;
import org.apache.jena.system.Txn;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.junit.jupiter.api.Test;

/**
 * Behaviour checks for the direct-to-store default and <em>named</em> graph views of
 * {@link DatasetGraphInMemoryCowTxn} (analogous to the MVCC dataset's views). The full
 * view contract is exercised by {@link TestDatasetGraphInMemoryCowTxnViewsDirect}; here
 * we pin down COW-specific behaviour: {@code getGraph} dispatch / caching, the absent /
 * lazily-created named graph, the no-op delete, cross-graph isolation, triple-for-triple
 * parity with a generic {@link GraphView} under both {@link ForkMode}s, read-your-writes
 * within an explicit write transaction, and graph&harr;quad-API consistency.
 */
public class TestDatasetGraphInMemoryCowTxnNamedGraphView {

    private static final Node G1 = NodeFactory.createURI("http://ex/g1");
    private static final Node G2 = NodeFactory.createURI("http://ex/g2");

    private static Node n(String s) {
        return NodeFactory.createURI("http://ex/" + s);
    }

    private static void populateNamed(DatasetGraphInMemoryCowTxn dsg, Node g, int n) {
        Txn.executeWrite(dsg, () -> {
            Graph graph = dsg.getGraph(g);
            for (int i = 0; i < n; i++) {
                graph.add(t("s" + i, "p", "o" + i));
            }
        });
    }

    private static Set<Triple> toSet(ExtendedIterator<Triple> it) {
        try {
            return it.toSet();
        } finally {
            it.close();
        }
    }

    private static Set<Triple> streamSet(Graph g, Triple pattern) {
        return g.stream(pattern.getSubject(), pattern.getPredicate(), pattern.getObject())
                .collect(Collectors.toSet());
    }

    // ----- identity / dispatch ----------------------------------------------

    @Test
    public void defaultGraphViewIsCachedAndDatasetBacked() {
        DatasetGraphInMemoryCowTxn dsg = new DatasetGraphInMemoryCowTxn();
        Graph d = dsg.getDefaultGraph();
        assertTrue(d instanceof GraphView);
        assertSame(dsg, ((GraphView) d).getDataset());
        assertNull(((GraphView) d).getGraphName());
        assertSame(d, dsg.getDefaultGraph(), "default graph view is cached");
    }

    @Test
    public void getGraphRoutesDefaultUnionAndNullTerms() {
        DatasetGraphInMemoryCowTxn dsg = new DatasetGraphInMemoryCowTxn();
        assertSame(dsg.getDefaultGraph(), dsg.getGraph(Quad.defaultGraphNodeGenerated));
        assertSame(dsg.getDefaultGraph(), dsg.getGraph(Quad.defaultGraphIRI));
        assertSame(dsg.getDefaultGraph(), dsg.getGraph(null));
        Graph union = dsg.getGraph(Quad.unionGraph);
        assertTrue(union instanceof GraphView);
        assertTrue(Quad.isUnionGraph(((GraphView) union).getGraphName()));
    }

    @Test
    public void namedGraphIsAGraphViewBackedByTheDataset() {
        DatasetGraphInMemoryCowTxn dsg = new DatasetGraphInMemoryCowTxn();
        Graph g = dsg.getGraph(G1);
        assertTrue(g instanceof GraphView);
        assertSame(dsg, ((GraphView) g).getDataset());
        assertEquals(G1, ((GraphView) g).getGraphName());
    }

    // ----- absent / lazily-created named graph ------------------------------

    @Test
    public void absentNamedGraphReadsEmpty() {
        DatasetGraphInMemoryCowTxn dsg = new DatasetGraphInMemoryCowTxn();
        Graph g = dsg.getGraph(G1);
        assertEquals(0, g.size());
        assertTrue(g.isEmpty());
        assertFalse(g.contains(t("s0", "p", "o0")));
        assertFalse(g.find(Node.ANY, Node.ANY, Node.ANY).hasNext());
        assertEquals(0L, g.stream().count());
        assertFalse(dsg.containsGraph(G1));
    }

    @Test
    public void autoCommitAddCreatesNamedGraphAndIsReadable() {
        DatasetGraphInMemoryCowTxn dsg = new DatasetGraphInMemoryCowTxn();
        Graph g = dsg.getGraph(G1);
        g.add(t("a", "b", "c"));   // no surrounding txn -> auto-commit, lazy create
        assertEquals(1, g.size());
        assertTrue(g.contains(t("a", "b", "c")));
        assertEquals(1L, g.stream().count());
        assertTrue(dsg.containsGraph(G1));
    }

    @Test
    public void deleteAgainstAbsentNamedGraphIsNoOp() {
        DatasetGraphInMemoryCowTxn dsg = new DatasetGraphInMemoryCowTxn();
        Graph g = dsg.getGraph(G1);
        g.delete(t("never", "added", "x"));
        assertEquals(0, g.size());
        assertFalse(dsg.containsGraph(G1));
    }

    @Test
    public void namedGraphsAreIsolatedFromEachOther() {
        DatasetGraphInMemoryCowTxn dsg = new DatasetGraphInMemoryCowTxn();
        populateNamed(dsg, G1, 3);   // s0/p/o0, s1/p/o1, s2/p/o2
        populateNamed(dsg, G2, 1);   // s0/p/o0 only
        assertEquals(3, dsg.getGraph(G1).size());
        assertEquals(1, dsg.getGraph(G2).size());
        assertTrue(dsg.getGraph(G1).contains(t("s1", "p", "o1")));
        assertFalse(dsg.getGraph(G2).contains(t("s1", "p", "o1")));
        assertTrue(dsg.getGraph(G2).contains(t("s0", "p", "o0")));
        assertFalse(dsg.getDefaultGraph().contains(t("s0", "p", "o0")));
    }

    // ----- equivalence with the generic GraphView ---------------------------

    @Test
    public void namedViewMatchesGenericGraphViewAcrossForkModes() {
        Triple[] patterns = {
                Triple.ANY,
                Triple.createMatch(n("s3"), null, null),
                Triple.createMatch(null, n("p"), null),
                Triple.createMatch(null, null, n("o3")),
                Triple.createMatch(n("s3"), n("p"), n("o3")),
                Triple.createMatch(n("absent"), null, null),
        };
        for (ForkMode fork : ForkMode.values()) {
            DatasetGraphInMemoryCowTxn dsg = new DatasetGraphInMemoryCowTxn(fork);
            populateNamed(dsg, G1, 8);
            Graph optimized = dsg.getGraph(G1);
            Graph generic = GraphView.createNamedGraph(dsg, G1);

            assertEquals(generic.size(), optimized.size(), "size, fork=" + fork);
            assertEquals(generic.isEmpty(), optimized.isEmpty(), "isEmpty, fork=" + fork);
            for (Triple pat : patterns) {
                assertEquals(toSet(generic.find(pat)), toSet(optimized.find(pat)),
                        "find " + pat + " fork=" + fork);
                assertEquals(generic.contains(pat), optimized.contains(pat),
                        "contains " + pat + " fork=" + fork);
                assertEquals(streamSet(generic, pat), streamSet(optimized, pat),
                        "stream " + pat + " fork=" + fork);
            }
        }
    }

    @Test
    public void defaultViewMatchesGenericGraphView() {
        DatasetGraphInMemoryCowTxn dsg = new DatasetGraphInMemoryCowTxn();
        Txn.executeWrite(dsg, () -> {
            Graph d = dsg.getDefaultGraph();
            d.add(t("s0", "p", "o0"));
            d.add(t("s1", "p", "o1"));
        });
        Graph optimized = dsg.getDefaultGraph();
        Graph generic = GraphView.createDefaultGraph(dsg);
        assertEquals(generic.size(), optimized.size());
        assertEquals(toSet(generic.find(Triple.ANY)), toSet(optimized.find(Triple.ANY)));
        assertTrue(optimized.contains(t("s0", "p", "o0")));
        assertEquals(streamSet(generic, Triple.ANY), streamSet(optimized, Triple.ANY));
    }

    // ----- read-your-writes / quad consistency ------------------------------

    @Test
    public void readYourWritesInNamedGraphWithinWriteTransaction() {
        DatasetGraphInMemoryCowTxn dsg = new DatasetGraphInMemoryCowTxn();
        populateNamed(dsg, G1, 3);
        Graph g = dsg.getGraph(G1);
        dsg.begin(TxnType.WRITE);
        try {
            g.add(t("new", "p", "o"));
            g.delete(t("s0", "p", "o0"));
            assertEquals(3, g.size());
            assertTrue(g.contains(t("new", "p", "o")));
            assertFalse(g.contains(t("s0", "p", "o0")));
            assertEquals(3L, g.stream().count());
            dsg.commit();
        } finally {
            dsg.end();
        }
        assertEquals(3, g.size());
        assertTrue(g.contains(t("new", "p", "o")));
    }

    @Test
    public void namedGraphViewAndQuadApiAgree() {
        DatasetGraphInMemoryCowTxn dsg = new DatasetGraphInMemoryCowTxn();
        Graph g = dsg.getGraph(G1);
        Txn.executeWrite(dsg, () -> g.add(t("a", "b", "c")));
        assertTrue(dsg.contains(Quad.create(G1, t("a", "b", "c"))));
        Txn.executeWrite(dsg, () -> dsg.add(Quad.create(G1, t("d", "e", "f"))));
        assertTrue(g.contains(t("d", "e", "f")));
        assertEquals(2, g.size());
    }
}

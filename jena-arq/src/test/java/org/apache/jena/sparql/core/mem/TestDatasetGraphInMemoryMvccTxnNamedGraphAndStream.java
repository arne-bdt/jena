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

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.IndexingStrategy;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.GraphView;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.system.Txn;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.junit.jupiter.api.Test;

/**
 * Behaviour checks for the direct-to-store <em>named</em> graph view of
 * {@link DatasetGraphInMemoryMvccTxn} and for its native quad {@code stream(g,s,p,o)}.
 * <p>
 * Named graphs share the {@code StoreBackedGraphView} machinery with the default graph
 * but resolve their store lazily from {@code namedStores} (absent until first written).
 * These tests pin down that the shortcut stays a {@link GraphView} backed by the dataset,
 * agrees triple-for-triple with a generic {@code GraphView} over the same dataset, treats
 * an absent graph as empty, creates the store on first write, isolates graphs from each
 * other, and that {@code getGraph} still routes the default / union / null graph terms
 * correctly. The dataset-level {@code stream(g,s,p,o)} is checked to return exactly the
 * same quads as {@code find(g,s,p,o)} for default, named, absent, union and wildcard
 * graph terms.
 */
public class TestDatasetGraphInMemoryMvccTxnNamedGraphAndStream {

    private static final IndexingStrategy[] STRATEGIES = {
            IndexingStrategy.EAGER, IndexingStrategy.MINIMAL, IndexingStrategy.MANUAL
    };

    private static final Node G1 = NodeFactory.createURI("http://ex/g1");
    private static final Node G2 = NodeFactory.createURI("http://ex/g2");

    private static Node n(String s) {
        return NodeFactory.createURI("http://ex/" + s);
    }

    private static DatasetGraphInMemoryMvccTxn dataset(IndexingStrategy s) {
        return new DatasetGraphInMemoryMvccTxn(s);
    }

    /** Add {@code n} triples to named graph {@code g} in one committed write. */
    private static void populateNamed(DatasetGraphInMemoryMvccTxn dsg, Node g, int n) {
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
    public void namedGraphIsAGraphViewBackedByTheDataset() {
        DatasetGraphInMemoryMvccTxn dsg = dataset(IndexingStrategy.EAGER);
        Graph g = dsg.getGraph(G1);
        assertTrue(g instanceof GraphView, "named graph must remain a GraphView");
        assertSame(dsg, ((GraphView) g).getDataset());
        assertEquals(G1, ((GraphView) g).getGraphName());
    }

    @Test
    public void getGraphRoutesDefaultUnionAndNullTerms() {
        DatasetGraphInMemoryMvccTxn dsg = dataset(IndexingStrategy.EAGER);
        // default graph node (generated or explicit) and null map to the cached default graph
        assertSame(dsg.getDefaultGraph(), dsg.getGraph(Quad.defaultGraphNodeGenerated));
        assertSame(dsg.getDefaultGraph(), dsg.getGraph(Quad.defaultGraphIRI));
        assertSame(dsg.getDefaultGraph(), dsg.getGraph(null));
        // union node yields a union-capable GraphView (not a single named store)
        Graph union = dsg.getGraph(Quad.unionGraph);
        assertTrue(union instanceof GraphView);
        assertTrue(Quad.isUnionGraph(((GraphView) union).getGraphName()),
                "union node must yield a union GraphView");
    }

    // ----- absent / lazily-created named graph ------------------------------

    @Test
    public void absentNamedGraphReadsEmpty() {
        DatasetGraphInMemoryMvccTxn dsg = dataset(IndexingStrategy.EAGER);
        Graph g = dsg.getGraph(G1);     // never written
        assertEquals(0, g.size());
        assertTrue(g.isEmpty());
        assertFalse(g.contains(t("s0", "p", "o0")));
        assertFalse(g.find(Node.ANY, Node.ANY, Node.ANY).hasNext());
        assertEquals(0L, g.stream().count());
        assertFalse(dsg.containsGraph(G1));
    }

    @Test
    public void autoCommitAddCreatesNamedGraphAndIsReadable() {
        DatasetGraphInMemoryMvccTxn dsg = dataset(IndexingStrategy.EAGER);
        Graph g = dsg.getGraph(G1);
        g.add(t("a", "b", "c"));        // no surrounding txn -> auto-commit, lazy create
        assertEquals(1, g.size());
        assertTrue(g.contains(t("a", "b", "c")));
        assertEquals(1L, g.stream().count());
        assertTrue(dsg.containsGraph(G1));
    }

    @Test
    public void deleteAgainstAbsentNamedGraphIsNoOp() {
        DatasetGraphInMemoryMvccTxn dsg = dataset(IndexingStrategy.EAGER);
        Graph g = dsg.getGraph(G1);
        g.delete(t("never", "added", "x"));   // must not throw, must not create the graph
        assertEquals(0, g.size());
        assertFalse(dsg.containsGraph(G1));
    }

    @Test
    public void namedGraphsAreIsolatedFromEachOther() {
        DatasetGraphInMemoryMvccTxn dsg = dataset(IndexingStrategy.EAGER);
        populateNamed(dsg, G1, 3);   // s0/p/o0, s1/p/o1, s2/p/o2
        populateNamed(dsg, G2, 1);   // s0/p/o0 only
        assertEquals(3, dsg.getGraph(G1).size());
        assertEquals(1, dsg.getGraph(G2).size());
        // s1/p/o1 is unique to G1; G2 must not see it.
        assertTrue(dsg.getGraph(G1).contains(t("s1", "p", "o1")));
        assertFalse(dsg.getGraph(G2).contains(t("s1", "p", "o1")));
        // s0/p/o0 is in both named graphs but not the (never-written) default graph.
        assertTrue(dsg.getGraph(G2).contains(t("s0", "p", "o0")));
        assertFalse(dsg.getDefaultGraph().contains(t("s0", "p", "o0")));
    }

    // ----- equivalence with the generic GraphView ---------------------------

    @Test
    public void namedViewMatchesGenericGraphViewAcrossStrategies() {
        Triple[] patterns = {
                Triple.ANY,
                Triple.createMatch(n("s3"), null, null),
                Triple.createMatch(null, n("p"), null),
                Triple.createMatch(null, null, n("o3")),
                Triple.createMatch(n("s3"), n("p"), n("o3")),
                Triple.createMatch(n("absent"), null, null),
        };
        for (IndexingStrategy s : STRATEGIES) {
            DatasetGraphInMemoryMvccTxn dsg = dataset(s);
            populateNamed(dsg, G1, 8);
            Graph optimized = dsg.getGraph(G1);
            Graph generic = GraphView.createNamedGraph(dsg, G1);

            assertEquals(generic.size(), optimized.size(), "size, strategy=" + s);
            assertEquals(generic.isEmpty(), optimized.isEmpty(), "isEmpty, strategy=" + s);
            for (Triple pat : patterns) {
                // MANUAL throws on partial-pattern lookups until the index is built,
                // and the dataset has no initializeIndex hook, so skip those here
                // (the throwing contract is covered at the graph/store level).
                if (s == IndexingStrategy.MANUAL && isPartialPattern(pat)) {
                    continue;
                }
                assertEquals(toSet(generic.find(pat)), toSet(optimized.find(pat)),
                        "find " + pat + " strategy=" + s);
                assertEquals(generic.contains(pat), optimized.contains(pat),
                        "contains " + pat + " strategy=" + s);
                assertEquals(streamSet(generic, pat), streamSet(optimized, pat),
                        "stream " + pat + " strategy=" + s);
            }
        }
    }

    /** @return whether {@code pat} binds one or two of S/P/O (a partial pattern). */
    private static boolean isPartialPattern(Triple pat) {
        int bound = (pat.getSubject().isConcrete() ? 1 : 0)
                + (pat.getPredicate().isConcrete() ? 1 : 0)
                + (pat.getObject().isConcrete() ? 1 : 0);
        return bound == 1 || bound == 2;
    }

    // ----- read-your-writes / quad consistency for a named graph ------------

    @Test
    public void readYourWritesInNamedGraphWithinWriteTransaction() {
        DatasetGraphInMemoryMvccTxn dsg = dataset(IndexingStrategy.EAGER);
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
        DatasetGraphInMemoryMvccTxn dsg = dataset(IndexingStrategy.EAGER);
        Graph g = dsg.getGraph(G1);
        // write via the graph; observe through the dataset's quad API
        Txn.executeWrite(dsg, () -> g.add(t("a", "b", "c")));
        assertTrue(dsg.contains(Quad.create(G1, t("a", "b", "c"))));
        // write via the dataset's quad API; observe through the graph
        Txn.executeWrite(dsg, () -> dsg.add(Quad.create(G1, t("d", "e", "f"))));
        assertTrue(g.contains(t("d", "e", "f")));
        assertEquals(2, g.size());
    }

    // ----- dataset-level stream(g,s,p,o) == find(g,s,p,o) -------------------

    @Test
    public void datasetStreamMatchesFindForAllGraphTerms() {
        DatasetGraphInMemoryMvccTxn dsg = dataset(IndexingStrategy.EAGER);
        // default graph
        Txn.executeWrite(dsg, () -> {
            Graph d = dsg.getDefaultGraph();
            d.add(t("s0", "p", "o0"));
            d.add(t("s1", "p", "o1"));
        });
        populateNamed(dsg, G1, 4);
        populateNamed(dsg, G2, 2);

        Node absent = n("gAbsent");
        Node[] graphTerms = {
                Quad.defaultGraphIRI, Quad.defaultGraphNodeGenerated,
                G1, G2, absent, Node.ANY, Quad.unionGraph
        };
        Triple[] patterns = {
                Triple.ANY,
                Triple.createMatch(n("s0"), null, null),
                Triple.createMatch(null, n("p"), null),
        };
        for (Node g : graphTerms) {
            for (Triple pat : patterns) {
                Set<Quad> viaFind = Iter.toSet(dsg.find(g, pat.getSubject(), pat.getPredicate(), pat.getObject()));
                Set<Quad> viaStream = dsg.stream(g, pat.getSubject(), pat.getPredicate(), pat.getObject())
                        .collect(Collectors.toSet());
                assertEquals(viaFind, viaStream, "stream vs find for g=" + g + " pattern=" + pat);
            }
        }
    }

    @Test
    public void datasetStreamTagsDefaultGraphQuadsWithDefaultGraphIRI() {
        DatasetGraphInMemoryMvccTxn dsg = dataset(IndexingStrategy.EAGER);
        Txn.executeWrite(dsg, () -> dsg.getDefaultGraph().add(t("a", "b", "c")));
        Set<Quad> quads = dsg.stream(Quad.defaultGraphNodeGenerated, Node.ANY, Node.ANY, Node.ANY)
                .collect(Collectors.toSet());
        assertEquals(Set.of(Quad.create(Quad.defaultGraphIRI, t("a", "b", "c"))), quads);
    }

    @Test
    public void datasetStreamReadYourWritesUnderWrite() {
        DatasetGraphInMemoryMvccTxn dsg = dataset(IndexingStrategy.EAGER);
        populateNamed(dsg, G1, 2);
        dsg.begin(TxnType.WRITE);
        try {
            dsg.add(Quad.create(G1, t("extra", "p", "o")));
            long n = dsg.stream(G1, Node.ANY, Node.ANY, Node.ANY).count();
            assertEquals(3L, n, "native stream must see the write overlay");
            dsg.commit();
        } finally {
            dsg.end();
        }
    }
}

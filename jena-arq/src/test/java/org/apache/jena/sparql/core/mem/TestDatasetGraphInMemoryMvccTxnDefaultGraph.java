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
 * Behaviour checks for the direct-to-store default graph of
 * {@link DatasetGraphInMemoryMvccTxn} (its {@code DefaultGraphView}).
 * <p>
 * The view bypasses the generic {@link GraphView}'s triple&harr;quad mapping and
 * {@code find()}-based {@code contains}/{@code size}, so these tests pin down that the
 * shortcut preserves semantics: it stays a {@link GraphView} backed by the dataset,
 * agrees triple-for-triple with a generic {@code GraphView} over the same dataset, and
 * honours the dataset's transaction state (read-your-writes under WRITE, a stable
 * snapshot under READ, abort discards, and transient latest-committed reads outside any
 * transaction). {@code stream()} / {@code stream(s,p,o)} are exercised explicitly as the
 * newly added fast paths.
 */
public class TestDatasetGraphInMemoryMvccTxnDefaultGraph {

    /** The indexing strategies supported by the MVCC store. */
    private static final IndexingStrategy[] STRATEGIES = {
            IndexingStrategy.EAGER, IndexingStrategy.MINIMAL, IndexingStrategy.MANUAL
    };

    private static Node n(String s) {
        return NodeFactory.createURI("http://ex/" + s);
    }

    private static DatasetGraphInMemoryMvccTxn dataset(IndexingStrategy s) {
        return new DatasetGraphInMemoryMvccTxn(s);
    }

    /** Add {@code 2*n} triples to the default graph in a single committed write. */
    private static void populate(DatasetGraphInMemoryMvccTxn dsg, int n) {
        Txn.executeWrite(dsg, () -> {
            Graph g = dsg.getDefaultGraph();
            for (int i = 0; i < n; i++) {
                g.add(t("s" + i, "p", "o" + i));
                g.add(t("s" + i, "p2", "o"));
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

    // ----- identity / backward compatibility --------------------------------

    @Test
    public void defaultGraphIsAStableGraphViewBackedByTheDataset() {
        DatasetGraphInMemoryMvccTxn dsg = dataset(IndexingStrategy.EAGER);
        Graph g = dsg.getDefaultGraph();
        // Callers (and the JMH harness) rely on getDefaultGraph() being a GraphView
        // whose getDataset() is the controlling Transactional.
        assertTrue(g instanceof GraphView, "default graph must remain a GraphView");
        assertSame(dsg, ((GraphView) g).getDataset());
        assertNull(((GraphView) g).getGraphName(), "default graph has no graph name");
        assertSame(g, dsg.getDefaultGraph(), "default graph view should be cached");
    }

    // ----- out-of-transaction (auto-commit / transient read) ----------------

    @Test
    public void autoCommitAddThenReadWithoutTransaction() {
        DatasetGraphInMemoryMvccTxn dsg = dataset(IndexingStrategy.EAGER);
        Graph g = dsg.getDefaultGraph();
        g.add(t("a", "b", "c"));        // no surrounding txn -> auto-commit
        assertEquals(1, g.size());
        assertFalse(g.isEmpty());
        assertTrue(g.contains(t("a", "b", "c")));
        assertTrue(g.find(t("a", "b", "c")).hasNext());
        assertEquals(1L, g.stream().count());
    }

    @Test
    public void emptyDefaultGraphReads() {
        DatasetGraphInMemoryMvccTxn dsg = dataset(IndexingStrategy.EAGER);
        Graph g = dsg.getDefaultGraph();
        assertEquals(0, g.size());
        assertTrue(g.isEmpty());
        assertFalse(g.contains(t("a", "b", "c")));
        assertFalse(g.find(Node.ANY, Node.ANY, Node.ANY).hasNext());
        assertEquals(0L, g.stream().count());
    }

    @Test
    public void deleteThroughGraphAutoCommits() {
        DatasetGraphInMemoryMvccTxn dsg = dataset(IndexingStrategy.EAGER);
        populate(dsg, 2);               // 4 triples
        Graph g = dsg.getDefaultGraph();
        g.delete(t("s0", "p", "o0"));   // auto-commit delete
        assertEquals(3, g.size());
        assertFalse(g.contains(t("s0", "p", "o0")));
    }

    // ----- semantic equivalence with a generic GraphView --------------------

    @Test
    public void optimizedViewMatchesGenericGraphViewAcrossStrategies() {
        Triple[] patterns = {
                Triple.ANY,
                Triple.createMatch(n("s3"), null, null),
                Triple.createMatch(null, n("p2"), null),
                Triple.createMatch(null, null, n("o")),
                Triple.createMatch(n("s3"), n("p"), n("o3")),
                Triple.createMatch(n("absent"), null, null),
        };
        for (IndexingStrategy s : STRATEGIES) {
            DatasetGraphInMemoryMvccTxn dsg = dataset(s);
            populate(dsg, 8);           // 16 triples
            Graph optimized = dsg.getDefaultGraph();
            Graph generic = GraphView.createDefaultGraph(dsg);

            assertEquals(generic.size(), optimized.size(), "size, strategy=" + s);
            assertEquals(generic.isEmpty(), optimized.isEmpty(), "isEmpty, strategy=" + s);

            for (Triple pat : patterns) {
                assertEquals(toSet(generic.find(pat)), toSet(optimized.find(pat)),
                        "find " + pat + " strategy=" + s);
                assertEquals(generic.contains(pat), optimized.contains(pat),
                        "contains " + pat + " strategy=" + s);
                assertEquals(streamSet(generic, pat), streamSet(optimized, pat),
                        "stream " + pat + " strategy=" + s);
            }
        }
    }

    // ----- stream fast paths (newly added) ----------------------------------

    @Test
    public void streamFullAndPartialPatterns() {
        DatasetGraphInMemoryMvccTxn dsg = dataset(IndexingStrategy.EAGER);
        populate(dsg, 5);               // 10 triples: s0..s4 each with (p,oi) and (p2,o)
        Graph g = dsg.getDefaultGraph();
        assertEquals(10L, g.stream().count());
        assertEquals(2L, g.stream(n("s3"), Node.ANY, Node.ANY).count());
        assertEquals(5L, g.stream(Node.ANY, n("p2"), Node.ANY).count());
        assertEquals(5L, g.stream(Node.ANY, Node.ANY, n("o")).count());
        // null in a stream pattern is normalised to ANY, like find.
        assertEquals(2L, g.stream(n("s3"), null, null).count());
        assertEquals(1L, g.stream(n("s3"), n("p"), n("o3")).count());
    }

    // ----- read-your-writes / abort under explicit transactions -------------

    @Test
    public void readYourWritesWithinWriteTransaction() {
        DatasetGraphInMemoryMvccTxn dsg = dataset(IndexingStrategy.EAGER);
        populate(dsg, 3);               // 6 triples committed
        Graph g = dsg.getDefaultGraph();
        dsg.begin(TxnType.WRITE);
        try {
            g.add(t("new", "p", "o"));
            g.delete(t("s0", "p", "o0"));
            assertEquals(6, g.size(), "overlay (+1 -1) visible to size");
            assertTrue(g.contains(t("new", "p", "o")), "added triple visible to contains");
            assertFalse(g.contains(t("s0", "p", "o0")), "deleted triple hidden from contains");
            assertTrue(g.find(t("new", "p", "o")).hasNext(), "added triple visible to find");
            assertEquals(6L, g.stream().count(), "overlay visible to stream");
            dsg.commit();
        } finally {
            dsg.end();
        }
        assertEquals(6, g.size());
        assertTrue(g.contains(t("new", "p", "o")));
        assertFalse(g.contains(t("s0", "p", "o0")));
    }

    @Test
    public void abortDiscardsGraphWrites() {
        DatasetGraphInMemoryMvccTxn dsg = dataset(IndexingStrategy.EAGER);
        populate(dsg, 3);               // 6 triples
        Graph g = dsg.getDefaultGraph();
        dsg.begin(TxnType.WRITE);
        g.add(t("new", "p", "o"));
        g.delete(t("s0", "p", "o0"));
        dsg.abort();
        dsg.end();
        assertEquals(6, g.size());
        assertTrue(g.contains(t("s0", "p", "o0")));
        assertFalse(g.contains(t("new", "p", "o")));
    }

    // ----- snapshot isolation under a READ transaction ----------------------

    @Test
    public void readSnapshotIsStableWhileAnotherThreadCommits() throws Exception {
        DatasetGraphInMemoryMvccTxn dsg = dataset(IndexingStrategy.EAGER);
        populate(dsg, 10);              // 20 triples
        Graph g = dsg.getDefaultGraph();

        dsg.begin(TxnType.READ);
        try {
            int initial = g.size();
            assertEquals(20, initial);
            Thread writer = new Thread(() -> {
                for (int i = 0; i < 50; i++) {
                    Triple tr = t("w" + i, "p", "o");
                    Txn.executeWrite(dsg, () -> dsg.getDefaultGraph().add(tr));
                }
            });
            writer.start();
            for (int k = 0; k < 25; k++) {
                assertEquals(initial, g.size(),
                        "READ snapshot size must not change while a writer commits");
                Thread.sleep(1);
            }
            writer.join(10_000);
            assertEquals(initial, g.size(), "snapshot still stable after writer finished");
        } finally {
            dsg.end();
        }
        // A fresh reader (outside any txn) sees the writer's commits.
        assertEquals(20 + 50, dsg.getDefaultGraph().size());
    }

    // ----- the direct triple view and the quad API stay consistent ----------

    @Test
    public void directGraphViewAndQuadApiAgree() {
        DatasetGraphInMemoryMvccTxn dsg = dataset(IndexingStrategy.EAGER);
        Graph g = dsg.getDefaultGraph();

        // Write through the graph; observe through the dataset's quad API.
        Txn.executeWrite(dsg, () -> g.add(t("a", "b", "c")));
        assertTrue(dsg.contains(Quad.create(Quad.defaultGraphIRI, t("a", "b", "c"))));
        assertEquals(1, Iter.count(dsg.find(Node.ANY, Node.ANY, Node.ANY, Node.ANY)));

        // Write through the dataset's quad API; observe through the graph.
        Txn.executeWrite(dsg, () -> dsg.add(Quad.create(Quad.defaultGraphIRI, t("d", "e", "f"))));
        assertTrue(g.contains(t("d", "e", "f")));
        assertEquals(2, g.size());
    }
}

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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Transactional;
import org.junit.jupiter.api.Test;

/**
 * Smoke tests for {@link DatasetGraphInMemoryCowTxn} Phase 1: verifies the
 * core wiring (transaction lifecycle, default + named graph routing, basic
 * isolation between concurrent readers and a writer, abort semantics, and
 * snapshot consistency across multiple named graphs).
 * <p>
 * Full contract conformance and stress tests live in Phase 2.
 */
public class TestDatasetGraphInMemoryCowTxnSmoke {

    private static Node uri(String x) { return NodeFactory.createURI("http://ex/" + x); }
    private static Quad dq(String s, String p, String o) {
        return Quad.create(Quad.defaultGraphIRI, uri(s), uri(p), uri(o));
    }
    private static Quad nq(String g, String s, String p, String o) {
        return Quad.create(uri(g), uri(s), uri(p), uri(o));
    }

    @Test
    public void supportsTransactions() {
        DatasetGraph ds = new DatasetGraphInMemoryCowTxn();
        assertTrue(ds.supportsTransactions());
        assertTrue(ds.supportsTransactionAbort());
    }

    @Test
    public void prefixesRoundTrip() {
        DatasetGraph ds = new DatasetGraphInMemoryCowTxn();
        ds.begin(TxnType.WRITE);
        ds.prefixes().add("ex", "http://example/");
        ds.commit();
        ds.end();

        ds.begin(TxnType.READ);
        assertEquals("http://example/", ds.prefixes().get("ex"));
        ds.end();
    }

    @Test
    public void writeAndReadDefaultGraph() {
        DatasetGraph ds = new DatasetGraphInMemoryCowTxn();
        ds.begin(TxnType.WRITE);
        ds.add(dq("s", "p", "o"));
        ds.commit();
        ds.end();

        ds.begin(TxnType.READ);
        try {
            assertTrue(ds.contains(dq("s", "p", "o")));
            assertEquals(1, ds.getDefaultGraph().size());
        } finally {
            ds.end();
        }
    }

    @Test
    public void writeAndReadNamedGraph() {
        DatasetGraph ds = new DatasetGraphInMemoryCowTxn();
        ds.begin(TxnType.WRITE);
        ds.add(nq("g1", "s", "p", "o"));
        ds.commit();
        ds.end();

        ds.begin(TxnType.READ);
        try {
            assertTrue(ds.contains(nq("g1", "s", "p", "o")));
            assertEquals(1, ds.getGraph(uri("g1")).size());
            List<Node> graphs = Iter.toList(ds.listGraphNodes());
            assertEquals(1, graphs.size());
            assertEquals(uri("g1"), graphs.get(0));
        } finally {
            ds.end();
        }
    }

    @Test
    public void abortDiscardsAdds() {
        DatasetGraph ds = new DatasetGraphInMemoryCowTxn();

        ds.begin(TxnType.WRITE);
        ds.add(dq("s", "p", "o"));
        ds.add(nq("g1", "s", "p", "o"));
        ds.abort();
        ds.end();

        ds.begin(TxnType.READ);
        try {
            assertFalse(ds.contains(dq("s", "p", "o")));
            assertFalse(ds.contains(nq("g1", "s", "p", "o")));
            assertFalse(ds.listGraphNodes().hasNext());
        } finally {
            ds.end();
        }
    }

    /**
     * After we add a quad to a never-seen-before named graph, abort the write,
     * and start a fresh read, the graph must not exist in the dataset.
     * Exercises the topology delta (additions) rollback path.
     */
    @Test
    public void abortRollsBackTopologyAdditions() {
        DatasetGraph ds = new DatasetGraphInMemoryCowTxn();

        ds.begin(TxnType.WRITE);
        ds.add(nq("g1", "s", "p", "o"));
        ds.abort();
        ds.end();

        ds.begin(TxnType.READ);
        try {
            assertFalse(ds.containsGraph(uri("g1")));
            assertFalse(ds.listGraphNodes().hasNext());
        } finally {
            ds.end();
        }
    }

    /**
     * A reader that began before the writer must see the dataset as it was
     * at its own begin — not the writer's later changes. This is the
     * cross-graph snapshot isolation property.
     */
    @Test
    public void readerSeesPreCommitSnapshotAcrossGraphs() throws Exception {
        DatasetGraph ds = new DatasetGraphInMemoryCowTxn();

        // Seed: one quad in default, one in g1.
        ds.begin(TxnType.WRITE);
        ds.add(dq("s0", "p", "o0"));
        ds.add(nq("g1", "s0", "p", "o0"));
        ds.commit();
        ds.end();

        // Start a reader that will hold its READ txn open while a writer commits.
        CountDownLatch readerStarted = new CountDownLatch(1);
        CountDownLatch writerCommitted = new CountDownLatch(1);
        AtomicReference<Throwable> readerError = new AtomicReference<>();
        AtomicReference<Long> defaultSizeSeen = new AtomicReference<>();
        AtomicReference<Long> g1SizeSeen = new AtomicReference<>();

        Thread reader = new Thread(() -> {
            try {
                ds.begin(TxnType.READ);
                try {
                    readerStarted.countDown();
                    writerCommitted.await(5, TimeUnit.SECONDS);
                    defaultSizeSeen.set(ds.getDefaultGraph().size() + 0L);
                    g1SizeSeen.set(ds.getGraph(uri("g1")).size() + 0L);
                } finally {
                    ds.end();
                }
            } catch (Throwable t) {
                readerError.set(t);
            }
        });
        reader.start();
        assertTrue(readerStarted.await(5, TimeUnit.SECONDS));

        // Writer adds quads to both default and named graphs, then commits.
        ds.begin(TxnType.WRITE);
        ds.add(dq("s1", "p", "o1"));
        ds.add(nq("g1", "s1", "p", "o1"));
        ds.commit();
        ds.end();
        writerCommitted.countDown();

        reader.join(5_000);
        if (readerError.get() != null)
            throw new AssertionError("reader threw", readerError.get());

        // Reader pinned its view at its own begin: must see 1 quad per graph.
        assertEquals(1L, defaultSizeSeen.get());
        assertEquals(1L, g1SizeSeen.get());

        // A fresh reader after the writer must see the post-commit state.
        ds.begin(TxnType.READ);
        try {
            assertEquals(2, ds.getDefaultGraph().size());
            assertEquals(2, ds.getGraph(uri("g1")).size());
        } finally {
            ds.end();
        }
    }

    /**
     * Writes inside a write transaction are visible to reads inside the same
     * transaction (the writer sees its own working copy).
     */
    @Test
    public void writerSeesOwnUncommittedWrites() {
        DatasetGraph ds = new DatasetGraphInMemoryCowTxn();
        ds.begin(TxnType.WRITE);
        try {
            ds.add(dq("s", "p", "o"));
            ds.add(nq("g1", "s", "p", "o"));
            assertTrue(ds.contains(dq("s", "p", "o")));
            assertTrue(ds.contains(nq("g1", "s", "p", "o")));
            assertEquals(1, ds.getDefaultGraph().size());
            assertEquals(1, ds.getGraph(uri("g1")).size());
        } finally {
            ds.commit();
            ds.end();
        }
    }

    @Test
    public void promoteReadToWrite() {
        DatasetGraph ds = new DatasetGraphInMemoryCowTxn();
        ds.begin(TxnType.READ_PROMOTE);
        try {
            assertEquals(ReadWrite.READ, ds.transactionMode());
            assertTrue(ds.promote());
            assertEquals(ReadWrite.WRITE, ds.transactionMode());
            ds.add(dq("s", "p", "o"));
            ds.commit();
        } finally {
            ds.end();
        }
        ds.begin(TxnType.READ);
        try {
            assertTrue(ds.contains(dq("s", "p", "o")));
        } finally {
            ds.end();
        }
    }

    @Test
    public void readOutsideTransactionAutoWraps() {
        DatasetGraph ds = new DatasetGraphInMemoryCowTxn();
        Txn.executeWriteVia(ds, () -> ds.add(dq("s", "p", "o")));
        // No surrounding txn — find() must auto-wrap via the access() helper.
        List<Quad> seen = Iter.toList(ds.find());
        assertEquals(1, seen.size());
    }

    @Test
    public void writeOutsideTransactionAutoWraps() {
        DatasetGraph ds = new DatasetGraphInMemoryCowTxn();
        // Direct add() with no surrounding txn — mutate() must auto-wrap.
        ds.add(dq("s", "p", "o"));
        ds.begin(TxnType.READ);
        try {
            assertTrue(ds.contains(dq("s", "p", "o")));
        } finally {
            ds.end();
        }
    }

    @Test
    public void multipleNamedGraphsIndependent() {
        DatasetGraph ds = new DatasetGraphInMemoryCowTxn();
        ds.begin(TxnType.WRITE);
        ds.add(nq("g1", "s", "p", "o1"));
        ds.add(nq("g2", "s", "p", "o2"));
        ds.add(nq("g3", "s", "p", "o3"));
        ds.commit();
        ds.end();

        ds.begin(TxnType.READ);
        try {
            assertEquals(1, ds.getGraph(uri("g1")).size());
            assertEquals(1, ds.getGraph(uri("g2")).size());
            assertEquals(1, ds.getGraph(uri("g3")).size());
            List<Node> graphs = new ArrayList<>(Iter.toList(ds.listGraphNodes()));
            assertEquals(3, graphs.size());
            assertTrue(graphs.contains(uri("g1")));
            assertTrue(graphs.contains(uri("g2")));
            assertTrue(graphs.contains(uri("g3")));
        } finally {
            ds.end();
        }
    }

    @Test
    public void parallelForkModeProducesSameResults() {
        DatasetGraph ds = new DatasetGraphInMemoryCowTxn(
                GraphMemIndexedSetCowTxn.ForkMode.PARALLEL);
        ds.begin(TxnType.WRITE);
        for (int i = 0; i < 50; i++)
            ds.add(nq("g" + (i % 5), "s" + i, "p", "o" + i));
        ds.commit();
        ds.end();

        ds.begin(TxnType.READ);
        try {
            long total = 0;
            for (Node g : Iter.toList(ds.listGraphNodes()))
                total += ds.getGraph(g).size();
            assertEquals(50, total);
        } finally {
            ds.end();
        }
    }

    /** Small helper so the test file doesn't depend on Txn directly. */
    private static final class Txn {
        static void executeWriteVia(Transactional t, Runnable r) {
            org.apache.jena.system.Txn.executeWrite(t, r);
        }
    }
}

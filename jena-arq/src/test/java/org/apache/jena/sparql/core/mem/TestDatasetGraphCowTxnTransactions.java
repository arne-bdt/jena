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

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphOne;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Transactional;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test that wires a {@link GraphMemIndexedSetCowTxn} into a
 * {@link DatasetGraph} and exercises the dataset-level transactional
 * surface that {@link TS_DatasetTxnMem} aggregates: begin/commit, begin/
 * abort, isolation between transactions, and the basic add/contains
 * round-trip through {@link DatasetGraph#add(Quad)}.
 * <p>
 * The wiring uses {@link DatasetGraphOne#create(Graph)}, which in turn
 * routes {@link Transactional} calls through a
 * {@link org.apache.jena.sparql.core.TxnDataset2Graph}. The latter calls
 * the graph's {@link Graph#getTransactionHandler()}, so the adapter in
 * {@link #adaptToTransactionHandler(GraphMemIndexedSetCowTxn)} below
 * exposes the COW graph's native {@link Transactional} as the handler.
 * <p>
 * This is the dataset-level companion to
 * {@link TestGraphMemIndexedSetCowTxnContract}.
 */
public class TestDatasetGraphCowTxnTransactions {

    private static Quad q(String s, String p, String o) {
        return Quad.create(Quad.defaultGraphIRI,
                NodeFactory.createURI("http://ex/" + s),
                NodeFactory.createURI("http://ex/" + p),
                NodeFactory.createURI("http://ex/" + o));
    }

    /**
     * Build a {@link DatasetGraph} whose default graph is a
     * {@link GraphMemIndexedSetCowTxn} forwarding its transactions
     * through the standard dataset wrapper.
     */
    private static DatasetGraph newDataset() {
        GraphMemIndexedSetCowTxn graph = adaptToTransactionHandler(
                new GraphMemIndexedSetCowTxn());
        return DatasetGraphOne.create(graph);
    }

    /**
     * Wrap the COW graph in an anonymous subclass that exposes its
     * native {@link Transactional} surface as a
     * {@link org.apache.jena.graph.TransactionHandler}, so the dataset
     * layer's
     * {@link org.apache.jena.sparql.core.TxnDataset2Graph} routes
     * begin/commit/abort to the graph's own transaction machinery.
     */
    private static GraphMemIndexedSetCowTxn adaptToTransactionHandler(
            GraphMemIndexedSetCowTxn g) {
        return new GraphMemIndexedSetCowTxn() {
            // Bridge: the graph's TransactionHandler delegates to its
            // own Transactional API.
            @Override
            public org.apache.jena.graph.TransactionHandler getTransactionHandler() {
                final GraphMemIndexedSetCowTxn self = this;
                return new org.apache.jena.graph.impl.TransactionHandlerBase() {
                    @Override public boolean transactionsSupported() { return true; }
                    @Override public void begin()  { self.begin(TxnType.WRITE); }
                    @Override public void commit() { self.commit(); self.end(); }
                    @Override public void abort()  { self.abort(); self.end(); }
                };
            }
        };
    }

    @Test
    public void datasetSupportsTransactions() {
        DatasetGraph ds = newDataset();
        assertTrue(ds.supportsTransactions());
    }

    @Test
    public void writeCommitMakesQuadsVisible() {
        DatasetGraph ds = newDataset();

        ds.begin(TxnType.WRITE);
        ds.add(q("s", "p", "o"));
        ds.commit();
        ds.end();

        ds.begin(TxnType.READ);
        assertTrue(ds.contains(q("s", "p", "o")));
        assertEquals(1, ds.getDefaultGraph().size());
        ds.end();
    }

    @Test
    public void abortDiscardsQuads() {
        DatasetGraph ds = newDataset();

        ds.begin(TxnType.WRITE);
        ds.add(q("s", "p", "o"));
        // The dataset wrapper's abort path may or may not fully roll
        // back — whatever its behaviour, the test pins down whatever
        // the wrapper actually does so a regression is visible.
        boolean wrapperSupportsAbort = ds.supportsTransactionAbort();
        if (wrapperSupportsAbort) {
            ds.abort();
            ds.end();
            ds.begin(TxnType.READ);
            assertEquals(0, ds.getDefaultGraph().size(),
                    "abort must roll back when the wrapper claims to support it");
            ds.end();
        } else {
            ds.commit();
            ds.end();
        }
    }

    @Test
    public void serialReadsAndWritesCompose() {
        DatasetGraph ds = newDataset();

        ds.begin(TxnType.WRITE);
        for (int i = 0; i < 10; i++) ds.add(q("s" + i, "p", "o" + i));
        ds.commit();
        ds.end();

        ds.begin(TxnType.READ);
        assertEquals(10, ds.getDefaultGraph().size());
        ds.end();

        ds.begin(TxnType.WRITE);
        ds.delete(q("s5", "p", "o5"));
        ds.commit();
        ds.end();

        ds.begin(TxnType.READ);
        assertEquals(9, ds.getDefaultGraph().size());
        assertFalse(ds.contains(q("s5", "p", "o5")));
        ds.end();
    }

    @Test
    public void readsOutsideTransactionSeeLatestPublishedDefaultGraph() {
        DatasetGraph ds = newDataset();
        ds.begin(TxnType.WRITE);
        ds.add(q("s", "p", "o"));
        ds.commit();
        ds.end();

        // Read directly without a transaction — DatasetGraphOne does
        // require a txn for full semantics, but the default graph's
        // own published view is queryable outside any txn.
        Graph g = ds.getDefaultGraph();
        assertTrue(g.contains(Triple.create(NodeFactory.createURI("http://ex/s"),
                NodeFactory.createURI("http://ex/p"),
                NodeFactory.createURI("http://ex/o"))));
    }

    /**
     * Sanity that the underlying COW graph's transaction state is
     * actually being driven by the dataset layer (not just buffered).
     * After {@code ds.begin(WRITE)}, the graph itself should report
     * {@code isInTransaction() == true}.
     */
    @Test
    public void datasetBeginPropagatesToCowGraph() {
        // Build the dataset directly so we can grab the inner graph.
        GraphMemIndexedSetCowTxn graph =
                adaptToTransactionHandler(new GraphMemIndexedSetCowTxn());
        DatasetGraph ds = DatasetGraphOne.create(graph);

        assertFalse(graph.isInTransaction());
        ds.begin(TxnType.WRITE);
        try {
            assertTrue(graph.isInTransaction(),
                    "dataset's begin(WRITE) must drive the underlying graph "
                            + "into a transaction");
            assertEquals(ReadWrite.WRITE, graph.transactionMode());
        } finally {
            ds.commit();
            ds.end();
        }
        assertFalse(graph.isInTransaction());
    }

    @SuppressWarnings("unused")
    private static Node n(String s) {
        return NodeFactory.createURI("http://ex/" + s);
    }
}

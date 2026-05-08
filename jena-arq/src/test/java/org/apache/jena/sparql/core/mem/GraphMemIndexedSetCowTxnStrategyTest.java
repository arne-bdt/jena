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

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.IndexingStrategy;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.JenaTransactionException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Graph-level tests for {@link GraphMemIndexedSetCowTxn} covering the
 * non-EAGER indexing strategies and the {@code clearIndex} /
 * {@code initializeIndex} / {@code initializeIndexParallel} controls.
 * <p>
 * Strategy correctness in isolation is covered by
 * {@code CowStoreStrategiesTest} at the store layer; these tests focus on
 * the transactional semantics: index mutations are write-transaction
 * scoped, lookups inside read transactions see the published strategy
 * state, and committed strategy changes survive across transactions.
 */
public class GraphMemIndexedSetCowTxnStrategyTest {

    private static Triple t(String s, String p, String o) {
        return Triple.create(NodeFactory.createURI("http://ex/" + s),
                             NodeFactory.createURI("http://ex/" + p),
                             NodeFactory.createURI("http://ex/" + o));
    }

    private static GraphMemIndexedSetCowTxn populated(IndexingStrategy s) {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn(s);
        g.begin(TxnType.WRITE);
        for (int i = 0; i < 10; i++) {
            g.add(t("s" + i, "p", "o" + i));
            g.add(t("s" + i, "p2", "o"));
        }
        g.commit();
        g.end();
        return g;
    }

    // ----- LAZY ------------------------------------------------------

    @Test
    public void lazyAutoBuildOnFirstReadInsideReadTransaction() {
        GraphMemIndexedSetCowTxn g = populated(IndexingStrategy.LAZY);
        assertEquals(IndexingStrategy.LAZY, g.getIndexingStrategy());
        assertFalse(g.isIndexInitialized());

        g.begin(TxnType.READ);
        // First pattern lookup triggers the auto-build on the published
        // store (via the LazyStrategy's CAS).
        assertTrue(g.contains(t("s3", "p", "o3")));
        assertEquals(2, g.stream(NodeFactory.createURI("http://ex/s3"), null, null).count());
        g.end();

        // After the read transaction ends, the published store's strategy
        // has been upgraded — visible from outside any transaction.
        assertTrue(g.isIndexInitialized());
    }

    @Test
    public void lazyAutoBuildInsideWriteTransactionPropagatesAtCommit() {
        GraphMemIndexedSetCowTxn g = populated(IndexingStrategy.LAZY);
        assertFalse(g.isIndexInitialized());

        g.begin(TxnType.WRITE);
        // First *partial* pattern lookup triggers the auto-build. A fully
        // concrete (SUB_PRE_OBJ) lookup goes straight to triples.containsKey
        // and bypasses the strategy.
        g.contains(Triple.createMatch(
                NodeFactory.createURI("http://ex/s3"), null, null));
        assertTrue(g.isIndexInitialized(),
                "writer's working copy must report initialized after lookup");
        g.commit();
        g.end();

        // Published now reflects the writer's upgraded strategy.
        assertTrue(g.isIndexInitialized());
    }

    @Test
    public void lazyParallelInsideWriteTransaction() {
        GraphMemIndexedSetCowTxn g = populated(IndexingStrategy.LAZY_PARALLEL);
        g.begin(TxnType.WRITE);
        g.contains(Triple.createMatch(
                NodeFactory.createURI("http://ex/s3"), null, null));
        assertTrue(g.isIndexInitialized());
        g.commit();
        g.end();
    }

    // ----- MANUAL ----------------------------------------------------

    @Test
    public void manualThrowsOnPatternLookupUntilInitialized() {
        GraphMemIndexedSetCowTxn g = populated(IndexingStrategy.MANUAL);

        g.begin(TxnType.READ);
        assertThrows(UnsupportedOperationException.class,
                () -> g.contains(Triple.createMatch(
                        NodeFactory.createURI("http://ex/s3"), null, null)));
        // Fully-concrete lookup bypasses the strategy.
        assertTrue(g.contains(t("s3", "p", "o3")));
        g.end();

        // initializeIndex must run inside a write transaction.
        g.begin(TxnType.WRITE);
        g.initializeIndex();
        assertTrue(g.isIndexInitialized());
        g.commit();
        g.end();

        // Now pattern lookups work in a fresh read transaction.
        g.begin(TxnType.READ);
        assertEquals(2, g.stream(NodeFactory.createURI("http://ex/s3"), null, null).count());
        g.end();
    }

    @Test
    public void manualInitializeIndexParallelInsideWriteTransaction() {
        GraphMemIndexedSetCowTxn g = populated(IndexingStrategy.MANUAL);
        g.begin(TxnType.WRITE);
        g.initializeIndexParallel();
        assertTrue(g.isIndexInitialized());
        g.commit();
        g.end();

        g.begin(TxnType.READ);
        assertEquals(2, g.stream(NodeFactory.createURI("http://ex/s3"), null, null).count());
        g.end();
    }

    @Test
    public void initializeIndexOutsideTransactionThrows() {
        GraphMemIndexedSetCowTxn g = populated(IndexingStrategy.MANUAL);
        assertThrows(JenaTransactionException.class, g::initializeIndex);
        assertThrows(JenaTransactionException.class, g::initializeIndexParallel);
        assertThrows(JenaTransactionException.class, g::clearIndex);
    }

    @Test
    public void initializeIndexInsideReadTransactionFails() {
        GraphMemIndexedSetCowTxn g = populated(IndexingStrategy.MANUAL);
        g.begin(TxnType.READ);
        try {
            // READ-only transaction: writeStore() must reject the call.
            assertThrows(JenaTransactionException.class, g::initializeIndex);
        } finally {
            g.end();
        }
    }

    // ----- MINIMAL ---------------------------------------------------

    @Test
    public void minimalAnswersLookupsByLinearScan() {
        GraphMemIndexedSetCowTxn g = populated(IndexingStrategy.MINIMAL);

        g.begin(TxnType.READ);
        assertFalse(g.isIndexInitialized(), "minimal stays uninitialized");
        assertEquals(2, g.stream(NodeFactory.createURI("http://ex/s3"), null, null).count());
        // No upgrade triggered.
        assertFalse(g.isIndexInitialized());
        g.end();
    }

    @Test
    public void minimalCanBeUpgradedExplicitlyInsideWriteTransaction() {
        GraphMemIndexedSetCowTxn g = populated(IndexingStrategy.MINIMAL);
        g.begin(TxnType.WRITE);
        g.initializeIndex();
        assertTrue(g.isIndexInitialized());
        g.commit();
        g.end();

        g.begin(TxnType.READ);
        assertTrue(g.isIndexInitialized());
        g.end();
    }

    // ----- clearIndex semantics ------------------------------------

    @Test
    public void clearIndexRevertsLazyToPending() {
        GraphMemIndexedSetCowTxn g = populated(IndexingStrategy.LAZY);

        g.begin(TxnType.READ);
        // Trigger auto-build via a partial pattern lookup.
        g.contains(Triple.createMatch(
                NodeFactory.createURI("http://ex/s3"), null, null));
        g.end();
        assertTrue(g.isIndexInitialized());

        g.begin(TxnType.WRITE);
        g.clearIndex();
        assertFalse(g.isIndexInitialized(),
                "clearIndex must revert lazy to pending in the working copy");
        g.commit();
        g.end();

        // Published now reverted; next lookup re-builds.
        assertFalse(g.isIndexInitialized());
        g.begin(TxnType.READ);
        g.contains(Triple.createMatch(
                NodeFactory.createURI("http://ex/s3"), null, null));
        g.end();
        assertTrue(g.isIndexInitialized());
    }

    // ----- Snapshot isolation around strategy changes ---------------

    @Test
    public void writerStrategyChangesAreInvisibleToConcurrentReader() throws Exception {
        // A concurrent reader holding a snapshot taken before a writer's
        // initializeIndex must not observe the writer's strategy change
        // until commit.
        GraphMemIndexedSetCowTxn g = populated(IndexingStrategy.MANUAL);

        // Reader: start a READ txn (captures the published store as
        // its snapshot — currently MANUAL).
        g.begin(TxnType.READ);
        try {
            // Run the write in a separate thread so it can take the
            // write lock concurrently.
            Thread writer = new Thread(() -> {
                g.begin(TxnType.WRITE);
                g.initializeIndex();
                g.commit();
                g.end();
            });
            writer.start();
            writer.join(5_000);

            // The reader's snapshot still has the MANUAL strategy.
            assertFalse(g.isIndexInitialized(),
                    "reader's snapshot must not observe writer's index build");
            assertThrows(UnsupportedOperationException.class,
                    () -> g.contains(Triple.createMatch(
                            NodeFactory.createURI("http://ex/s3"), null, null)));
        } finally {
            g.end();
        }

        // After the reader ends, a fresh read sees the new published
        // strategy.
        g.begin(TxnType.READ);
        assertTrue(g.isIndexInitialized());
        g.end();
    }
}

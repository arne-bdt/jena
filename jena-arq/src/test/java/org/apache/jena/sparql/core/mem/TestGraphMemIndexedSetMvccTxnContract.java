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

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.IndexingStrategy;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.jena.sparql.core.mem.CowTxnTestHelper.t;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Graph-level contract checks for {@link GraphMemIndexedSetMvccTxn}, mirroring
 * {@link TestGraphMemIndexedSetCowTxnContract}: it walks the Graph surface
 * (add / delete / find / contains / size / stream) within explicit transactions
 * across the three indexing strategies the MVCC store supports
 * (EAGER / MINIMAL / MANUAL).
 */
public class TestGraphMemIndexedSetMvccTxnContract {

    /** The indexing strategies supported by the MVCC store. */
    private static final IndexingStrategy[] STRATEGIES = {
            IndexingStrategy.EAGER, IndexingStrategy.MINIMAL, IndexingStrategy.MANUAL
    };

    private static Node n(String s) {
        return NodeFactory.createURI("http://ex/" + s);
    }

    private static GraphMemIndexedSetMvccTxn populated(IndexingStrategy s, int n) {
        GraphMemIndexedSetMvccTxn g = new GraphMemIndexedSetMvccTxn(s);
        g.begin(TxnType.WRITE);
        for (int i = 0; i < n; i++) {
            g.add(t("s" + i, "p", "o" + i));
            g.add(t("s" + i, "p2", "o"));
        }
        g.commit();
        g.end();
        return g;
    }

    // ----- add / delete / size --------------------------------------

    @Test
    public void addThenDeleteIsSizeZero() {
        for (IndexingStrategy s : STRATEGIES) {
            GraphMemIndexedSetMvccTxn g = new GraphMemIndexedSetMvccTxn(s);
            g.begin(TxnType.WRITE);
            g.add(t("a", "b", "c"));
            assertEquals(1, g.size());
            g.delete(t("a", "b", "c"));
            assertEquals(0, g.size());
            g.commit();
            g.end();

            g.begin(TxnType.READ);
            assertEquals(0, g.size(), "strategy=" + s);
            g.end();
        }
    }

    @Test
    public void duplicateAddIsIdempotent() {
        for (IndexingStrategy s : STRATEGIES) {
            GraphMemIndexedSetMvccTxn g = new GraphMemIndexedSetMvccTxn(s);
            g.begin(TxnType.WRITE);
            g.add(t("a", "b", "c"));
            g.add(t("a", "b", "c"));
            g.add(t("a", "b", "c"));
            assertEquals(1, g.size(), "strategy=" + s);
            g.commit();
            g.end();
        }
    }

    @Test
    public void deleteOfAbsentTripleIsNoOp() {
        GraphMemIndexedSetMvccTxn g = new GraphMemIndexedSetMvccTxn();
        g.begin(TxnType.WRITE);
        g.delete(t("never", "added", "triple"));   // must not throw
        assertEquals(0, g.size());
        g.commit();
        g.end();
    }

    // ----- contains / find / stream ---------------------------------

    @Test
    public void containsAndFindAgreeAcrossAllStrategiesForFullyConcrete() {
        for (IndexingStrategy s : STRATEGIES) {
            GraphMemIndexedSetMvccTxn g = populated(s, 5);
            g.begin(TxnType.READ);
            assertTrue(g.contains(t("s2", "p", "o2")), "strategy=" + s);
            assertFalse(g.contains(t("s2", "p", "missing")), "strategy=" + s);
            ExtendedIterator<Triple> it = g.find(t("s2", "p", "o2"));
            try {
                assertTrue(it.hasNext());
                assertEquals(t("s2", "p", "o2"), it.next());
                assertFalse(it.hasNext());
            } finally {
                it.close();
            }
            g.end();
        }
    }

    @Test
    public void streamPartialPatternsAgreeAcrossStrategies() {
        // MANUAL is excluded: partial-pattern lookups throw until the index is
        // built (see manualThrowsOnPatternLookupUntilInitialized).
        IndexingStrategy[] nonManual = {IndexingStrategy.EAGER, IndexingStrategy.MINIMAL};
        Set<Triple> goldStandard;
        {
            GraphMemIndexedSetMvccTxn eager = populated(IndexingStrategy.EAGER, 8);
            eager.begin(TxnType.READ);
            goldStandard = eager.stream(n("s3"), null, null)
                    .collect(Collectors.toCollection(HashSet::new));
            eager.end();
        }
        for (IndexingStrategy s : nonManual) {
            GraphMemIndexedSetMvccTxn g = populated(s, 8);
            g.begin(TxnType.READ);
            Set<Triple> got = g.stream(n("s3"), null, null)
                    .collect(Collectors.toCollection(HashSet::new));
            g.end();
            assertEquals(goldStandard, got, "strategy=" + s);
        }
    }

    /**
     * MANUAL: a partial-pattern lookup throws {@link UnsupportedOperationException}
     * until {@link GraphMemIndexedSetMvccTxn#initializeIndex()} is called (in a
     * write transaction), while fully-concrete lookups bypass the index throughout.
     * Mirrors {@code GraphMemIndexedSetCowTxnStrategyTest}.
     */
    @Test
    public void manualThrowsOnPatternLookupUntilInitialized() {
        GraphMemIndexedSetMvccTxn g = populated(IndexingStrategy.MANUAL, 5);

        g.begin(TxnType.READ);
        try {
            assertThrows(UnsupportedOperationException.class,
                    () -> g.contains(n("s3"), null, null));
            // Fully-concrete lookup bypasses the strategy.
            assertTrue(g.contains(t("s3", "p", "o3")));
        } finally {
            g.end();
        }

        // initializeIndex must run inside a write transaction.
        g.begin(TxnType.WRITE);
        g.initializeIndex();
        assertTrue(g.isIndexInitialized());
        g.commit();
        g.end();

        // Now pattern lookups work in a fresh read transaction.
        g.begin(TxnType.READ);
        assertEquals(2, g.stream(n("s3"), null, null).count());
        g.end();
    }

    // ----- transaction discipline: aborts / read-isolation -----------

    @Test
    public void abortDiscardsBothAddsAndDeletes() {
        GraphMemIndexedSetMvccTxn g = populated(IndexingStrategy.EAGER, 3);

        g.begin(TxnType.WRITE);
        g.add(t("new", "p", "o"));
        g.delete(t("s0", "p", "o0"));
        assertEquals(3 * 2 + 1 - 1, g.size());        // 6 originals + 1 new - 1 deleted
        g.abort();
        g.end();

        g.begin(TxnType.READ);
        assertEquals(3 * 2, g.size());                // back to 6
        assertTrue(g.contains(t("s0", "p", "o0")));
        assertFalse(g.contains(t("new", "p", "o")));
        g.end();
    }

    @Test
    public void readSnapshotIsStableWhileWriterRunsFreshTxns() throws Exception {
        GraphMemIndexedSetMvccTxn g = populated(IndexingStrategy.EAGER, 10);

        g.begin(TxnType.READ);
        try {
            int initialSize = g.size();
            Thread writer = new Thread(() -> {
                for (int i = 0; i < 100; i++) {
                    g.begin(TxnType.WRITE);
                    g.add(t("ws" + i, "p", "o"));
                    g.commit();
                    g.end();
                }
            });
            writer.start();
            for (int k = 0; k < 50; k++) {
                if (g.size() != initialSize) {
                    fail("snapshot size changed mid-read transaction");
                }
                Thread.sleep(2);
            }
            writer.join(5_000);
            assertEquals(initialSize, g.size(),
                    "reader's snapshot must remain stable across writer commits");
        } finally {
            g.end();
        }

        g.begin(TxnType.READ);
        assertEquals(10 * 2 + 100, g.size());
        g.end();
    }

    // ----- iterator behaviour ---------------------------------------

    @Test
    public void findWithAnyAnyAnyEnumeratesAllTriples() {
        GraphMemIndexedSetMvccTxn g = populated(IndexingStrategy.EAGER, 5);
        g.begin(TxnType.READ);
        ExtendedIterator<Triple> it = g.find(Node.ANY, Node.ANY, Node.ANY);
        try {
            int count = 0;
            while (it.hasNext()) {
                assertNotNull(it.next());
                count++;
            }
            assertEquals(10, count);
        } finally {
            it.close();
        }
        g.end();
    }

    @Test
    public void streamOutsideTransactionSeesPublished() {
        GraphMemIndexedSetMvccTxn g = populated(IndexingStrategy.EAGER, 3);
        long count = g.stream().count();
        assertEquals(6L, count);
    }

    // ----- transaction-mode introspection ---------------------------

    @Test
    public void transactionModeAndTypeReflectState() {
        GraphMemIndexedSetMvccTxn g = new GraphMemIndexedSetMvccTxn();

        assertNull(g.transactionMode());
        assertNull(g.transactionType());

        g.begin(TxnType.READ);
        assertEquals(ReadWrite.READ, g.transactionMode());
        assertEquals(TxnType.READ, g.transactionType());
        g.end();

        g.begin(TxnType.WRITE);
        assertEquals(ReadWrite.WRITE, g.transactionMode());
        assertEquals(TxnType.WRITE, g.transactionType());
        g.commit();
        g.end();

        assertNull(g.transactionMode());
    }

    // ----- MVCC-specific: O(1) begin doesn't copy; multi-version readd ----

    @Test
    public void multiVersionReaddAcrossTransactions() {
        GraphMemIndexedSetMvccTxn g = new GraphMemIndexedSetMvccTxn(IndexingStrategy.EAGER);
        g.begin(TxnType.WRITE);
        g.add(t("a", "b", "c"));
        g.commit();
        g.end();

        // Pin a reader, then delete and re-add on another transaction.
        g.begin(TxnType.READ);                       // reader sees the original
        boolean readerSeesOriginal = g.contains(t("a", "b", "c"));
        g.end();
        assertTrue(readerSeesOriginal);

        g.begin(TxnType.WRITE);
        g.delete(t("a", "b", "c"));
        g.commit();
        g.end();

        g.begin(TxnType.WRITE);
        g.add(t("a", "b", "c"));                     // new slot
        g.commit();
        g.end();

        g.begin(TxnType.READ);
        assertTrue(g.contains(t("a", "b", "c")));
        assertEquals(1, g.size());                   // not 2 — only one visible
        assertEquals(1, g.find(t("a", "b", "c")).toList().size());
        g.end();
    }
}

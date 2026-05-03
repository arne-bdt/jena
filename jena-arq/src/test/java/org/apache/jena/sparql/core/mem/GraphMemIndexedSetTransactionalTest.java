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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.shared.AddDeniedException;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.Transactional;
import org.junit.jupiter.api.Test;

public class GraphMemIndexedSetTransactionalTest {

    private static Node uri(String s) {
        return NodeFactory.createURI("http://example/" + s);
    }

    private static Triple t(String s, String p, String o) {
        return Triple.create(uri(s), uri(p), uri(o));
    }

    private static final Triple T1 = t("s1", "p", "o1");
    private static final Triple T2 = t("s2", "p", "o2");
    private static final Triple T3 = t("s3", "p", "o3");

    // -------- basic single-thread CRUD --------

    @Test
    public void writeThenCommit_visibleToReader() {
        var g = new GraphMemIndexedSetTransactional();
        g.begin(TxnType.WRITE);
        g.add(T1);
        g.add(T2);
        assertEquals(2, g.size());
        g.commit();
        g.end();

        g.begin(TxnType.READ);
        try {
            assertEquals(2, g.size());
            assertTrue(g.contains(T1));
            assertTrue(g.contains(T2));
            assertFalse(g.contains(T3));
        } finally {
            g.end();
        }
    }

    @Test
    public void abortDiscardsChanges() {
        var g = new GraphMemIndexedSetTransactional();
        g.begin(TxnType.WRITE);
        g.add(T1);
        g.commit();
        g.end();

        g.begin(TxnType.WRITE);
        g.add(T2);
        g.delete(T1);
        assertEquals(1, g.size());
        g.abort();
        g.end();

        g.begin(TxnType.READ);
        try {
            assertEquals(1, g.size());
            assertTrue(g.contains(T1));
            assertFalse(g.contains(T2));
        } finally {
            g.end();
        }
    }

    @Test
    public void clearInsideWriteTransaction() {
        var g = new GraphMemIndexedSetTransactional();
        g.begin(TxnType.WRITE);
        g.add(T1);
        g.add(T2);
        g.add(T3);
        g.clear();
        assertEquals(0, g.size());
        g.commit();
        g.end();

        g.begin(TxnType.READ);
        try {
            assertEquals(0, g.size());
            assertTrue(g.isEmpty());
        } finally {
            g.end();
        }
    }

    @Test
    public void writeDoesNotAffectReaderHoldingEarlierSnapshot() throws Exception {
        var g = new GraphMemIndexedSetTransactional();
        g.begin(TxnType.WRITE);
        g.add(T1);
        g.commit();
        g.end();

        // Reader on thread 1: take a snapshot, hold it open
        CountDownLatch readerSnapshotted = new CountDownLatch(1);
        CountDownLatch writerCommitted = new CountDownLatch(1);
        AtomicReference<Throwable> readerErr = new AtomicReference<>();

        Thread reader = new Thread(() -> {
            try {
                g.begin(TxnType.READ);
                try {
                    assertEquals(1, g.size());
                    assertTrue(g.contains(T1));
                    readerSnapshotted.countDown();
                    if (!writerCommitted.await(10, TimeUnit.SECONDS))
                        throw new AssertionError("writer never committed");
                    // Snapshot must still see exactly the original state
                    assertEquals(1, g.size());
                    assertTrue(g.contains(T1));
                    assertFalse(g.contains(T2));
                } finally {
                    g.end();
                }
            } catch (Throwable e) {
                readerErr.set(e);
            }
        });
        reader.start();
        readerSnapshotted.await(10, TimeUnit.SECONDS);

        // Writer commits a new triple
        g.begin(TxnType.WRITE);
        g.add(T2);
        g.commit();
        g.end();
        writerCommitted.countDown();

        reader.join(10_000);
        if (readerErr.get() != null)
            throw new AssertionError(readerErr.get());

        // Subsequent reader must see both
        g.begin(TxnType.READ);
        try {
            assertEquals(2, g.size());
            assertTrue(g.contains(T2));
        } finally {
            g.end();
        }
    }

    // -------- transaction state queries --------

    @Test
    public void transactionTypeAndModeOutsideTxnAreNull() {
        var g = new GraphMemIndexedSetTransactional();
        assertNull(g.transactionType());
        assertNull(g.transactionMode());
        assertFalse(g.isInTransaction());
    }

    @Test
    public void transactionTypeAndModeReportCurrentState() {
        var g = new GraphMemIndexedSetTransactional();
        g.begin(TxnType.READ);
        try {
            assertEquals(TxnType.READ, g.transactionType());
            assertEquals(ReadWrite.READ, g.transactionMode());
            assertTrue(g.isInTransaction());
        } finally {
            g.end();
        }
        g.begin(TxnType.WRITE);
        try {
            assertEquals(TxnType.WRITE, g.transactionType());
            assertEquals(ReadWrite.WRITE, g.transactionMode());
        } finally {
            g.commit();
            g.end();
        }
    }

    // -------- error paths --------

    @Test
    public void nestedBeginThrows() {
        var g = new GraphMemIndexedSetTransactional();
        g.begin(TxnType.READ);
        try {
            assertThrows(JenaTransactionException.class, () -> g.begin(TxnType.READ));
        } finally {
            g.end();
        }
    }

    @Test
    public void commitOutsideTxnThrows() {
        var g = new GraphMemIndexedSetTransactional();
        assertThrows(JenaTransactionException.class, g::commit);
    }

    @Test
    public void abortOutsideTxnThrows() {
        var g = new GraphMemIndexedSetTransactional();
        assertThrows(JenaTransactionException.class, g::abort);
    }

    @Test
    public void endOutsideTxnIsHarmless() {
        new GraphMemIndexedSetTransactional().end();
    }

    @Test
    public void writeInsideReadTxnThrows() {
        var g = new GraphMemIndexedSetTransactional();
        g.begin(TxnType.READ);
        try {
            assertThrows(AddDeniedException.class, () -> g.add(T1));
        } finally {
            g.end();
        }
    }

    @Test
    public void endAfterDirtyWriteThrowsButStillUnlocks() throws Exception {
        var g = new GraphMemIndexedSetTransactional();
        g.begin(TxnType.WRITE);
        g.add(T1);
        try {
            g.end();
            fail("expected JenaTransactionException");
        } catch (JenaTransactionException expected) {
            // good
        }
        // Lock must have been released: another thread can acquire WRITE.
        AtomicReference<Throwable> err = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                g.begin(TxnType.WRITE);
                g.commit();
                g.end();
            } catch (Throwable e) {
                err.set(e);
            }
        });
        t.start();
        t.join(5_000);
        if (err.get() != null) throw new AssertionError(err.get());
        assertFalse(t.isAlive());
    }

    @Test
    public void endAfterCleanWriteIsAllowed() {
        // Plan: end() after a write transaction with no work is fine
        // (no commit/abort needed because there is nothing to discard).
        var g = new GraphMemIndexedSetTransactional();
        g.begin(TxnType.WRITE);
        g.end();
        // Lock released - another begin must succeed.
        g.begin(TxnType.WRITE);
        g.commit();
        g.end();
    }

    // -------- promote --------

    @Test
    public void readPromote_succeedsWhenNoConcurrentCommit() {
        var g = new GraphMemIndexedSetTransactional();
        g.begin(TxnType.READ_PROMOTE);
        try {
            assertEquals(ReadWrite.READ, g.transactionMode());
            assertTrue(g.promote());
            assertEquals(ReadWrite.WRITE, g.transactionMode());
            g.add(T1);
            g.commit();
        } finally {
            g.end();
        }
        g.begin(TxnType.READ);
        try { assertTrue(g.contains(T1)); } finally { g.end(); }
    }

    @Test
    public void readPromote_failsAfterConcurrentCommit() throws Exception {
        var g = new GraphMemIndexedSetTransactional();
        g.begin(TxnType.READ_PROMOTE);
        try {
            // Concurrent writer commits between begin and promote.
            Thread writer = new Thread(() -> {
                g.begin(TxnType.WRITE);
                g.add(T2);
                g.commit();
                g.end();
            });
            writer.start();
            writer.join(5_000);

            assertFalse(g.promote());
            // Still in read mode; the lock was not retained.
            assertEquals(ReadWrite.READ, g.transactionMode());
            // Implicit promote on add() must also fail.
            try {
                g.add(T1);
                fail("expected AddDeniedException");
            } catch (AddDeniedException expected) { /* good */ }
        } finally {
            g.end();
        }
        // Verify the lock is free and the writer's commit landed.
        g.begin(TxnType.READ);
        try { assertTrue(g.contains(T2)); } finally { g.end(); }
    }

    @Test
    public void readCommittedPromote_alwaysSucceeds() throws Exception {
        var g = new GraphMemIndexedSetTransactional();
        g.begin(TxnType.READ_COMMITTED_PROMOTE);
        try {
            // Concurrent writer commits between begin and promote.
            Thread writer = new Thread(() -> {
                g.begin(TxnType.WRITE);
                g.add(T2);
                g.commit();
                g.end();
            });
            writer.start();
            writer.join(5_000);

            assertTrue(g.promote());
            assertEquals(ReadWrite.WRITE, g.transactionMode());
            // After read-committed promote we see committed changes from the other writer.
            assertTrue(g.contains(T2));
            g.add(T1);
            g.commit();
        } finally {
            g.end();
        }
        g.begin(TxnType.READ);
        try {
            assertTrue(g.contains(T1));
            assertTrue(g.contains(T2));
        } finally { g.end(); }
    }

    @Test
    public void promote_inPureReadThrows() {
        var g = new GraphMemIndexedSetTransactional();
        g.begin(TxnType.READ);
        try {
            assertThrows(JenaTransactionException.class, () -> g.promote(Transactional.Promote.ISOLATED));
        } finally {
            g.end();
        }
    }

    @Test
    public void promote_inWriteIsNoop() {
        var g = new GraphMemIndexedSetTransactional();
        g.begin(TxnType.WRITE);
        try {
            assertTrue(g.promote());
            assertEquals(ReadWrite.WRITE, g.transactionMode());
        } finally {
            g.commit();
            g.end();
        }
    }

    @Test
    public void copy_isIndependent() {
        var g = new GraphMemIndexedSetTransactional();
        g.begin(TxnType.WRITE);
        g.add(T1);
        g.commit();
        g.end();

        var g2 = g.copy();

        g.begin(TxnType.WRITE);
        g.add(T2);
        g.commit();
        g.end();

        g2.begin(TxnType.READ);
        try {
            assertTrue(g2.contains(T1));
            assertFalse(g2.contains(T2));
            assertEquals(1, g2.size());
        } finally {
            g2.end();
        }
    }

    @Test
    public void execHelpersWork() {
        // Smoke-test the Transactional default exec helpers.
        var g = new GraphMemIndexedSetTransactional();
        g.executeWrite(() -> g.add(T1));
        g.executeRead(() -> assertNotNull(g.find(T1).next()));
    }
}

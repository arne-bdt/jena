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

import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.Transactional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.jena.sparql.core.mem.CowTxnTestHelper.t;
import static org.junit.jupiter.api.Assertions.*;

public class GraphMemIndexedSetCowTxnTest {


    @Test
    public void writeCommitMakesChangesVisible() {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();

        g.begin(TxnType.WRITE);
        g.add(t("s", "p", "o"));
        assertEquals(1, g.size());
        g.commit();
        g.end();

        g.begin(TxnType.READ);
        assertTrue(g.contains(t("s", "p", "o")));
        assertEquals(1, g.size());
        g.end();
    }

    @Test
    public void abortDiscardsChanges() {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();

        g.begin(TxnType.WRITE);
        g.add(t("s", "p", "o"));
        g.abort();
        g.end();

        g.begin(TxnType.READ);
        assertEquals(0, g.size());
        assertFalse(g.contains(t("s", "p", "o")));
        g.end();
    }

    @Test
    public void readSnapshotIsolatedFromConcurrentWrite() throws Exception {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();
        g.begin(TxnType.WRITE);
        g.add(t("s", "p", "o1"));
        g.commit();
        g.end();

        // Reader thread captures a snapshot then waits.
        CountDownLatch readerStarted = new CountDownLatch(1);
        CountDownLatch writerDone = new CountDownLatch(1);
        AtomicInteger snapshotSize = new AtomicInteger(-1);
        AtomicReference<Throwable> readerErr = new AtomicReference<>();

        Thread reader = new Thread(() -> {
            try {
                g.begin(TxnType.READ);
                readerStarted.countDown();
                writerDone.await();
                snapshotSize.set(g.size());
                g.end();
            } catch (Throwable th) {
                readerErr.set(th);
            }
        });
        reader.start();
        readerStarted.await();

        // Writer adds another triple while reader holds snapshot.
        g.begin(TxnType.WRITE);
        g.add(t("s", "p", "o2"));
        g.commit();
        g.end();
        writerDone.countDown();

        reader.join(5_000);
        if (readerErr.get() != null) throw new AssertionError(readerErr.get());
        assertEquals(1, snapshotSize.get(),
                "Reader's snapshot must be isolated from concurrent commit");

        // After reader ends, a new read sees both triples.
        g.begin(TxnType.READ);
        assertEquals(2, g.size());
        g.end();
    }

    @Test
    public void writesSerializedByLock() throws Exception {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();

        CountDownLatch firstWriterIn = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        AtomicBoolean secondAcquired = new AtomicBoolean();
        AtomicReference<Throwable> err = new AtomicReference<>();

        Thread w1 = new Thread(() -> {
            try {
                g.begin(TxnType.WRITE);
                firstWriterIn.countDown();
                releaseFirst.await();
                g.add(t("a", "b", "c"));
                g.commit();
                g.end();
            } catch (Throwable th) { err.set(th); }
        });
        Thread w2 = new Thread(() -> {
            try {
                firstWriterIn.await();
                // give w1 some time to be inside the lock
                Thread.sleep(50);
                long t0 = System.nanoTime();
                g.begin(TxnType.WRITE);
                long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
                secondAcquired.set(true);
                g.add(t("d", "e", "f"));
                g.commit();
                g.end();
                assertTrue(elapsedMs >= 0,
                        "Second begin(WRITE) should have waited for the first"); // sanity
            } catch (Throwable th) { err.set(th); }
        });
        w1.start();
        w2.start();
        firstWriterIn.await();
        // w2 must not have acquired yet
        Thread.sleep(100);
        assertFalse(secondAcquired.get(),
                "Second writer must block until first commits");
        releaseFirst.countDown();
        w1.join(5_000);
        w2.join(5_000);
        if (err.get() != null) throw new AssertionError(err.get());
        assertTrue(secondAcquired.get());

        g.begin(TxnType.READ);
        assertEquals(2, g.size());
        g.end();
    }

    @Test
    public void readPromoteSucceedsWhenNoIntermediateCommit() {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();

        g.begin(TxnType.READ_PROMOTE);
        assertTrue(g.promote(Transactional.Promote.ISOLATED));
        assertEquals(ReadWrite.WRITE, g.transactionMode());
        g.add(t("s", "p", "o"));
        g.commit();
        g.end();

        g.begin(TxnType.READ);
        assertTrue(g.contains(t("s", "p", "o")));
        g.end();
    }

    @Test
    public void readPromoteFailsAfterIntermediateCommit() throws Exception {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();

        // T1 begins as READ_PROMOTE on the empty published view.
        g.begin(TxnType.READ_PROMOTE);

        // Another thread commits a write.
        Thread other = new Thread(() -> {
            g.begin(TxnType.WRITE);
            g.add(t("s", "p", "o"));
            g.commit();
            g.end();
        });
        other.start();
        other.join(5_000);

        // T1's promote should now fail (snapshot moved).
        assertFalse(g.promote(Transactional.Promote.ISOLATED));
        assertEquals(ReadWrite.READ, g.transactionMode());
        // T1 still sees the original (empty) snapshot.
        assertEquals(0, g.size());
        g.end();
    }

    @Test
    public void readCommittedPromoteAlwaysSucceeds() throws Exception {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();

        g.begin(TxnType.READ_COMMITTED_PROMOTE);

        Thread other = new Thread(() -> {
            g.begin(TxnType.WRITE);
            g.add(t("s", "p", "o"));
            g.commit();
            g.end();
        });
        other.start();
        other.join(5_000);

        assertTrue(g.promote(Transactional.Promote.READ_COMMITTED));
        assertEquals(ReadWrite.WRITE, g.transactionMode());
        g.add(t("s", "p", "o2"));
        g.commit();
        g.end();

        g.begin(TxnType.READ);
        assertEquals(2, g.size());
        g.end();
    }

    @Test
    public void writeOutsideTransactionFails() {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();
        assertThrows(JenaTransactionException.class, () -> g.add(t("s", "p", "o")));
    }

    @Test
    public void writeInReadTransactionFails() {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();
        g.begin(TxnType.READ);
        try {
            assertThrows(JenaTransactionException.class, () -> g.add(t("s", "p", "o")));
        } finally {
            g.end();
        }
    }

    @Test
    public void nestedBeginFails() {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();
        g.begin(TxnType.READ);
        try {
            assertThrows(JenaTransactionException.class, () -> g.begin(TxnType.READ));
        } finally {
            g.end();
        }
    }

    @Test
    public void writeLockReleasedAfterCommit() throws Exception {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();
        g.begin(TxnType.WRITE);
        g.add(t("s", "p", "o"));
        g.commit();
        g.end();

        AtomicReference<Throwable> err = new AtomicReference<>();
        Thread other = new Thread(() -> {
            try {
                g.begin(TxnType.WRITE);
                g.add(t("s2", "p2", "o2"));
                g.commit();
                g.end();
            } catch (Throwable th) { err.set(th); }
        });
        other.start();
        other.join(2_000);
        assertFalse(other.isAlive());
        if (err.get() != null) throw new AssertionError(err.get());
    }

    @Test
    public void clearWithinWriteTransaction() {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();
        g.begin(TxnType.WRITE);
        g.add(t("s", "p", "o"));
        g.add(t("s2", "p2", "o2"));
        g.clear();
        assertEquals(0, g.size());
        g.commit();
        g.end();

        g.begin(TxnType.READ);
        assertEquals(0, g.size());
        g.end();
    }

    @Test
    public void implicitPromoteOnAddForReadCommittedPromote() {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();
        g.begin(TxnType.READ_COMMITTED_PROMOTE);
        // Direct add should implicitly promote.
        g.add(t("s", "p", "o"));
        assertEquals(ReadWrite.WRITE, g.transactionMode());
        g.commit();
        g.end();

        g.begin(TxnType.READ);
        assertTrue(g.contains(t("s", "p", "o")));
        g.end();
    }

    @Test
    public void manyConcurrentReadersWithWriter() throws Exception {
        final GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();
        // Seed.
        g.begin(TxnType.WRITE);
        for (int i = 0; i < 100; i++)
            g.add(t("s" + i, "p", "o"));
        g.commit();
        g.end();

        AtomicBoolean stop = new AtomicBoolean();
        AtomicReference<Throwable> err = new AtomicReference<>();
        int numReaders = 8;
        Thread[] readers = new Thread[numReaders];
        for (int r = 0; r < numReaders; r++) {
            readers[r] = new Thread(() -> {
                try {
                    while (!stop.get()) {
                        g.begin(TxnType.READ);
                        int sz = g.size();
                        // Re-check inside snapshot — must not change.
                        for (int k = 0; k < 5; k++) {
                            if (g.size() != sz)
                                throw new AssertionError(
                                        "Snapshot size changed mid-transaction");
                        }
                        g.end();
                    }
                } catch (Throwable th) { err.set(th); }
            });
            readers[r].start();
        }

        Thread writer = new Thread(() -> {
            try {
                for (int i = 0; i < 200; i++) {
                    g.begin(TxnType.WRITE);
                    g.add(t("ws" + i, "p", "o"));
                    g.commit();
                    g.end();
                }
            } catch (Throwable th) { err.set(th); }
        });
        writer.start();
        writer.join(30_000);
        stop.set(true);
        for (Thread reader : readers) reader.join(5_000);

        if (err.get() != null) throw new AssertionError(err.get());

        g.begin(TxnType.READ);
        assertEquals(300, g.size());
        g.end();
    }

    @Test
    public void readsOutsideTransactionSeeLatestPublished() {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();
        // Read outside a txn returns the published view (empty).
        assertEquals(0, g.size());

        g.begin(TxnType.WRITE);
        g.add(t("s", "p", "o"));
        g.commit();
        g.end();

        assertEquals(1, g.size());
        assertTrue(g.contains(t("s", "p", "o")));
    }

    @Test
    @Timeout(5)
    public void endWithoutFinalisingDirtyWriteThrows() {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();
        g.begin(TxnType.WRITE);
        g.add(t("s", "p", "o"));
        try {
            g.end();
            fail("end() on dirty unfinished write should throw");
        } catch (JenaTransactionException expected) {
            // ok — and the lock must still be released
        }
        // Confirm lock released by starting another write.
        g.begin(TxnType.WRITE);
        assertEquals(0, g.size()); // previous write was discarded
        g.commit();
        g.end();
    }

    // ----- promote() contract --------------------------------------------

    /**
     * {@code promote(Promote)} on a plain READ transaction must return
     * {@code false} (matching the default no-arg {@code promote()} behaviour
     * and {@code DatasetGraphInMemory}). It must not throw.
     */
    @Test
    public void promoteOnPlainReadReturnsFalseForIsolated() {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();
        g.begin(TxnType.READ);
        try {
            assertFalse(g.promote(Transactional.Promote.ISOLATED));
            assertEquals(ReadWrite.READ, g.transactionMode());
        } finally {
            g.end();
        }
    }

    @Test
    public void promoteOnPlainReadReturnsFalseForReadCommitted() {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();
        g.begin(TxnType.READ);
        try {
            assertFalse(g.promote(Transactional.Promote.READ_COMMITTED));
            assertEquals(ReadWrite.READ, g.transactionMode());
        } finally {
            g.end();
        }
    }

    /**
     * ISOLATED promote must block on an active concurrent writer rather than
     * failing immediately. If that writer subsequently aborts, the reader's
     * snapshot is still the published one and the promote must succeed.
     */
    @Test
    @Timeout(5)
    public void isolatedPromoteBlocksAndSucceedsWhenActiveWriterAborts() throws Exception {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();

        // Active writer thread holds the write lock and ultimately aborts.
        CountDownLatch writerHoldsLock = new CountDownLatch(1);
        CountDownLatch writerMayProceed = new CountDownLatch(1);
        Thread activeWriter = new Thread(() -> {
            g.begin(TxnType.WRITE);
            writerHoldsLock.countDown();
            try {
                writerMayProceed.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            g.abort();
            g.end();
        });
        activeWriter.start();
        assertTrue(writerHoldsLock.await(2, TimeUnit.SECONDS));

        // Reader runs in its own thread so its READ_PROMOTE txn state is
        // separate from this thread's. The promoter blocks on the writer's
        // lock and should succeed when the writer aborts (snapshot unchanged).
        AtomicReference<Boolean> promoteResult = new AtomicReference<>();
        AtomicReference<Throwable> promoteError = new AtomicReference<>();
        Thread promoter = new Thread(() -> {
            try {
                g.begin(TxnType.READ_PROMOTE);
                promoteResult.set(g.promote(Transactional.Promote.ISOLATED));
                g.commit();
                g.end();
            } catch (Throwable th) { promoteError.set(th); }
        });
        promoter.start();
        // Give the promoter a moment to reach the blocking lock acquisition.
        Thread.sleep(100);
        assertNull(promoteResult.get(), "promote should still be blocking on the writer");

        // Release the writer; it aborts. The promoter should then succeed.
        writerMayProceed.countDown();
        promoter.join(5_000);
        if (promoteError.get() != null) throw new AssertionError(promoteError.get());
        assertEquals(Boolean.TRUE, promoteResult.get(),
                "promote(ISOLATED) must succeed once a no-op aborting writer releases");
        activeWriter.join(2_000);
    }

    /**
     * ISOLATED promote must block on an active concurrent writer. If that
     * writer commits a real change, the reader's snapshot has moved and
     * promote must return false.
     */
    @Test
    @Timeout(5)
    public void isolatedPromoteBlocksAndFailsWhenActiveWriterCommitsChanges() throws Exception {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();

        CountDownLatch writerHoldsLock = new CountDownLatch(1);
        CountDownLatch writerMayProceed = new CountDownLatch(1);
        Thread activeWriter = new Thread(() -> {
            g.begin(TxnType.WRITE);
            g.add(t("changed", "by", "writer"));
            writerHoldsLock.countDown();
            try {
                writerMayProceed.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            g.commit();
            g.end();
        });
        activeWriter.start();
        assertTrue(writerHoldsLock.await(2, TimeUnit.SECONDS));

        AtomicReference<Boolean> promoteResult = new AtomicReference<>();
        AtomicReference<Throwable> promoteError = new AtomicReference<>();
        Thread promoter = new Thread(() -> {
            try {
                g.begin(TxnType.READ_PROMOTE);
                promoteResult.set(g.promote(Transactional.Promote.ISOLATED));
                g.commit();
                g.end();
            } catch (Throwable th) { promoteError.set(th); }
        });
        promoter.start();
        Thread.sleep(100);
        assertNull(promoteResult.get(), "promote should still be blocking on the writer");

        writerMayProceed.countDown();
        promoter.join(5_000);
        if (promoteError.get() != null) throw new AssertionError(promoteError.get());
        assertEquals(Boolean.FALSE, promoteResult.get(),
                "promote(ISOLATED) must fail once a writer commits a real change");
        activeWriter.join(2_000);
    }

    /**
     * No-op concurrent commits (writer didn't change anything visible) must
     * not invalidate a reader's snapshot: ISOLATED promote may still succeed.
     * This is the per-graph "snapshot identity" semantics — distinct from the
     * stricter "any commit invalidates" rule that DatasetGraphInMemory and
     * its CowTxn sibling enforce at the dataset level via a generation
     * counter.
     */
    @Test
    @Timeout(5)
    public void isolatedPromoteSucceedsAfterConcurrentNoOpCommit() throws Exception {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();

        CountDownLatch writerHoldsLock = new CountDownLatch(1);
        CountDownLatch writerMayProceed = new CountDownLatch(1);
        Thread activeWriter = new Thread(() -> {
            g.begin(TxnType.WRITE);
            writerHoldsLock.countDown();
            try {
                writerMayProceed.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            g.commit();   // no-op commit: nothing was added
            g.end();
        });
        activeWriter.start();
        assertTrue(writerHoldsLock.await(2, TimeUnit.SECONDS));

        AtomicReference<Boolean> promoteResult = new AtomicReference<>();
        AtomicReference<Throwable> promoteError = new AtomicReference<>();
        Thread promoter = new Thread(() -> {
            try {
                g.begin(TxnType.READ_PROMOTE);
                promoteResult.set(g.promote(Transactional.Promote.ISOLATED));
                g.commit();
                g.end();
            } catch (Throwable th) { promoteError.set(th); }
        });
        promoter.start();
        Thread.sleep(100);
        assertNull(promoteResult.get(), "promote should still be blocking on the writer");

        writerMayProceed.countDown();
        promoter.join(5_000);
        if (promoteError.get() != null) throw new AssertionError(promoteError.get());
        assertEquals(Boolean.TRUE, promoteResult.get(),
                "promote(ISOLATED) must succeed after a no-op commit (snapshot identity unchanged)");
        activeWriter.join(2_000);
    }

    /**
     * Pre-acquisition fast-path: if the snapshot already moved before we
     * even try to take the writer lock, ISOLATED promote returns false
     * without blocking.
     */
    @Test
    @Timeout(5)
    public void isolatedPromoteFailsFastIfSnapshotAlreadyMoved() throws Exception {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();

        g.begin(TxnType.READ_PROMOTE);
        try {
            // Another thread commits a change end-to-end before we attempt promote.
            Thread writer = new Thread(() -> {
                g.begin(TxnType.WRITE);
                g.add(t("s", "p", "o"));
                g.commit();
                g.end();
            });
            writer.start();
            writer.join(2_000);

            assertFalse(g.promote(Transactional.Promote.ISOLATED));
            assertEquals(ReadWrite.READ, g.transactionMode());
        } finally {
            g.end();
        }
    }

    /**
     * Smoke test for {@link GraphMemIndexedSetCowTxn.ForkMode#PARALLEL}:
     * the parallel fork mode must exhibit identical visible behaviour to
     * the sequential default. This is the entry point benchmarks use to
     * compare the two paths.
     */
    @Test
    public void parallelForkModeBehavesLikeSequential() {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn(
                GraphMemIndexedSetCowTxn.ForkMode.PARALLEL);

        g.begin(TxnType.WRITE);
        for (int i = 0; i < 200; i++) g.add(t("s" + i, "p", "o"));
        g.commit();
        g.end();

        g.begin(TxnType.READ);
        assertEquals(200, g.size());
        for (int i = 0; i < 200; i++) assertTrue(g.contains(t("s" + i, "p", "o")));
        g.end();

        // A subsequent write transaction also goes through forkForWriteParallel.
        g.begin(TxnType.WRITE);
        for (int i = 0; i < 200; i += 2) g.delete(t("s" + i, "p", "o"));
        g.commit();
        g.end();

        g.begin(TxnType.READ);
        assertEquals(100, g.size());
        g.end();
    }
}

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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.Transactional;
import org.junit.jupiter.api.Test;

/**
 * {@code promote()} on a plain {@code READ} transaction must return
 * {@code false} (per the {@code Transactional} contract's default-method
 * dispatch), not throw. The two transactional graph implementations
 * ({@link GraphMemIndexedSetTxn} and {@link GraphMemIndexedSetCowTxn}) must
 * agree on this behaviour.
 * <p>
 * The {@code isolatedPromoteSucceedsAfterConcurrentWriterAborts_*} tests
 * pin down the second half of the dac7deee fix: an ISOLATED promote that
 * collides with a concurrent writer must <i>block</i> for that writer to
 * finish, then re-check the snapshot. If the writer aborts, the snapshot
 * hasn't moved and the promote must succeed. The earlier {@code tryLock}
 * implementation would fail-fast in this case (the writer holds the slot,
 * so {@code tryLock} returns false) — a spurious failure for what is in
 * fact a serialisable history.
 */
public class TestGraphMemIndexedSetTxnPromote {

    @Test
    public void plainReadPromoteReturnsFalse_nonCow() {
        GraphMemIndexedSetTxn g = new GraphMemIndexedSetTxn();
        g.begin(TxnType.READ);
        try {
            assertFalse(g.promote());
            assertFalse(g.promote(Transactional.Promote.ISOLATED));
            assertFalse(g.promote(Transactional.Promote.READ_COMMITTED));
        } finally {
            g.end();
        }
    }

    @Test
    public void plainReadPromoteReturnsFalse_cow() {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();
        g.begin(TxnType.READ);
        try {
            assertFalse(g.promote());
            assertFalse(g.promote(Transactional.Promote.ISOLATED));
            assertFalse(g.promote(Transactional.Promote.READ_COMMITTED));
        } finally {
            g.end();
        }
    }

    @Test
    public void readPromoteIsPromotable_nonCow() {
        GraphMemIndexedSetTxn g = new GraphMemIndexedSetTxn();
        g.begin(TxnType.READ_PROMOTE);
        try {
            assertTrue(g.promote());
            g.commit();
        } finally {
            g.end();
        }
    }

    @Test
    public void readPromoteIsPromotable_cow() {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();
        g.begin(TxnType.READ_PROMOTE);
        try {
            assertTrue(g.promote());
            g.commit();
        } finally {
            g.end();
        }
    }

    @Test
    public void isolatedPromoteSucceedsAfterConcurrentWriterAborts_nonCow() throws Exception {
        runIsolatedPromoteAfterAbort(new GraphMemIndexedSetTxn());
    }

    @Test
    public void isolatedPromoteSucceedsAfterConcurrentWriterAborts_cow() throws Exception {
        runIsolatedPromoteAfterAbort(new GraphMemIndexedSetCowTxn());
    }

    /**
     * Two threads against the same transactional graph:
     * <ol>
     *   <li>Reader thread begins {@code READ_PROMOTE}, pinning the
     *       current published store as its snapshot.</li>
     *   <li>Writer thread takes the write slot via {@code begin(WRITE)}.</li>
     *   <li>Reader thread calls {@code promote(ISOLATED)}. With the
     *       writer holding the lock, the reader must <i>block</i>
     *       (waiting on the writer slot) — not fail-fast.</li>
     *   <li>Writer aborts. The published snapshot has not moved, so the
     *       reader's post-acquire re-check passes and {@code promote}
     *       returns {@code true}.</li>
     * </ol>
     * Asserts that the reader's promote returned {@code true} and that
     * it actually blocked (waited at least a perceptible amount of time).
     */
    private static void runIsolatedPromoteAfterAbort(Transactional g) throws Exception {
        CountDownLatch writerHasLock = new CountDownLatch(1);
        CountDownLatch readerStartedPromote = new CountDownLatch(1);
        AtomicBoolean promoteResult = new AtomicBoolean(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Thread reader = new Thread(() -> {
            try {
                g.begin(TxnType.READ_PROMOTE);
                writerHasLock.await();
                readerStartedPromote.countDown();
                // Will block inside promote until the writer releases the slot.
                promoteResult.set(g.promote(Transactional.Promote.ISOLATED));
                if (promoteResult.get()) g.commit(); else g.end();
            } catch (Throwable th) {
                failure.set(th);
            }
        }, "isolated-promote-reader");

        Thread writer = new Thread(() -> {
            try {
                g.begin(TxnType.WRITE);
                writerHasLock.countDown();
                readerStartedPromote.await();
                // Spin until the reader is parked on the writer slot, then
                // abort. Abort leaves the published snapshot unchanged, so
                // the reader's post-acquire re-check should pass.
                waitUntilParked(reader, 2_000);
                g.abort();
            } catch (Throwable th) {
                failure.set(th);
            }
        }, "isolated-promote-writer");

        writer.start();
        reader.start();
        writer.join(5_000);
        reader.join(5_000);

        if (failure.get() != null) throw new AssertionError(failure.get());
        assertTrue(promoteResult.get(),
                "ISOLATED promote should succeed once the concurrent aborting writer releases the slot");
    }

    /**
     * Wait until {@code t} reaches a parked state (i.e. is blocked on the
     * writer lock inside {@code promote}). Polls instead of sleeping a
     * fixed amount so the test isn't fragile under load.
     */
    private static void waitUntilParked(Thread t, long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < deadline) {
            Thread.State st = t.getState();
            if (st == Thread.State.WAITING || st == Thread.State.TIMED_WAITING) return;
            Thread.sleep(1);
        }
        throw new AssertionError("thread " + t.getName() + " did not park within " + timeoutMs + "ms");
    }
}

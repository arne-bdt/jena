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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.TxnType;
import org.junit.jupiter.api.Test;

/**
 * Property-style concurrency test for {@link GraphMemIndexedSetTransactional}.
 * <p>
 * One writer commits batches of adds and removes; several readers
 * repeatedly capture a snapshot, observe a checksum (size + contains for
 * a fixed set of probe triples), and verify that what they see matches
 * one of the published states. No reader may ever observe a torn state.
 * <p>
 * Default runtime is short (a few seconds) so this is suitable for CI;
 * set the system property {@code jena.txn.concurrency.duration.ms} to a
 * larger value (the implementation plan asks for ≥ 60_000) for a
 * stress run.
 */
public class GraphMemIndexedSetTransactionalConcurrencyTest {

    private static final int  READER_THREADS    = 8;
    private static final int  PROBE_TRIPLES     = 32;
    private static final long DEFAULT_DURATION  = 3_000L;
    private static final long MAX_JOIN_WAIT_MS  = 60_000L;

    private static Node uri(String s) {
        return NodeFactory.createURI("http://example/" + s);
    }

    private static Triple probe(int i) {
        return Triple.create(uri("s" + i), uri("p"), uri("o" + i));
    }

    /**
     * A snapshot the writer just published: the size, and a long bitmap of
     * contains() answers for the fixed probe triples (bit i = probe(i)
     * present). Readers that see {@code published == this generation}
     * must produce exactly these answers.
     */
    private record Published(long generation, int size, long probeBitmap) {}

    @Test
    public void multipleReadersDoNotObserveTornStates() throws Exception {
        var g = new GraphMemIndexedSetTransactional();
        ConcurrentHashMap<Long, Published> history = new ConcurrentHashMap<>();
        AtomicLong generation = new AtomicLong(0);

        // Seed: empty published state.
        history.put(0L, new Published(0L, 0, 0L));

        long durationMs = Long.getLong("jena.txn.concurrency.duration.ms", DEFAULT_DURATION);
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(durationMs);

        AtomicReference<Throwable> firstError = new AtomicReference<>();
        CountDownLatch start = new CountDownLatch(1);

        Thread writer = new Thread(() -> {
            try {
                start.await();
                java.util.Random rnd = new java.util.Random(0xC0FFEEL);
                while (System.nanoTime() < deadline && firstError.get() == null) {
                    g.begin(TxnType.WRITE);
                    try {
                        // Random batch of adds and removes from the probe set
                        int batch = 1 + rnd.nextInt(8);
                        for (int b = 0; b < batch; b++) {
                            int i = rnd.nextInt(PROBE_TRIPLES);
                            Triple t = probe(i);
                            if (rnd.nextBoolean()) g.add(t);
                            else g.delete(t);
                        }
                        // Compute the checksum the writer is about to publish.
                        long bm = 0L;
                        int sz = g.size();
                        for (int i = 0; i < PROBE_TRIPLES; i++)
                            if (g.contains(probe(i))) bm |= (1L << i);
                        long gen = generation.incrementAndGet();
                        history.put(gen, new Published(gen, sz, bm));
                        g.commit();
                    } finally {
                        g.end();
                    }
                }
            } catch (Throwable t) {
                firstError.compareAndSet(null, t);
            }
        }, "txn-writer");

        List<Thread> readers = new ArrayList<>();
        for (int r = 0; r < READER_THREADS; r++) {
            final int seed = r;
            Thread reader = new Thread(() -> {
                try {
                    start.await();
                    while (System.nanoTime() < deadline && firstError.get() == null) {
                        g.begin(TxnType.READ);
                        try {
                            int sz = g.size();
                            long bm = 0L;
                            for (int i = 0; i < PROBE_TRIPLES; i++)
                                if (g.contains(probe(i))) bm |= (1L << i);
                            // The (size, bitmap) we observed must match
                            // some published version.
                            boolean found = false;
                            for (Published p : history.values()) {
                                if (p.size == sz && p.probeBitmap == bm) {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found)
                                throw new AssertionError(
                                        "Reader " + seed + " observed inconsistent state: size=" + sz
                                                + " bitmap=0x" + Long.toHexString(bm));
                            // Also check that find() agrees with contains() for one probe.
                            int i = (int) (Math.abs(System.nanoTime()) % PROBE_TRIPLES);
                            boolean cont = g.contains(probe(i));
                            boolean foundIt = g.find(probe(i)).hasNext();
                            if (cont != foundIt)
                                throw new AssertionError("contains/find mismatch for probe " + i);
                        } finally {
                            g.end();
                        }
                    }
                } catch (Throwable t) {
                    firstError.compareAndSet(null, t);
                }
            }, "txn-reader-" + r);
            readers.add(reader);
        }

        writer.start();
        readers.forEach(Thread::start);
        start.countDown();

        writer.join(MAX_JOIN_WAIT_MS);
        for (Thread r : readers) r.join(MAX_JOIN_WAIT_MS);

        if (firstError.get() != null)
            throw new AssertionError(firstError.get());

        // Sanity: writer made at least some progress.
        assertTrue(generation.get() > 0, "writer did not commit any transactions");
    }

    /**
     * Verify the write lock is correctly handed off: serialised writers
     * each see the prior writer's commits.
     */
    @Test
    public void writersSerialiseAndSeePriorCommits() throws Exception {
        var g = new GraphMemIndexedSetTransactional();
        int writers = 4;
        int perWriter = 50;

        AtomicReference<Throwable> err = new AtomicReference<>();
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (int w = 0; w < writers; w++) {
            final int wid = w;
            Thread t = new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perWriter; i++) {
                        g.begin(TxnType.WRITE);
                        try {
                            g.add(Triple.create(uri("w" + wid + "_" + i), uri("p"), uri("o")));
                            g.commit();
                        } finally {
                            g.end();
                        }
                    }
                } catch (Throwable e) {
                    err.compareAndSet(null, e);
                }
            }, "writer-" + w);
            threads.add(t);
        }
        threads.forEach(Thread::start);
        start.countDown();
        for (Thread t : threads) t.join(MAX_JOIN_WAIT_MS);

        if (err.get() != null) throw new AssertionError(err.get());

        g.begin(TxnType.READ);
        try {
            assertEquals(writers * perWriter, g.size());
            for (int w = 0; w < writers; w++)
                for (int i = 0; i < perWriter; i++)
                    assertNotNull(g.find(Triple.create(uri("w" + w + "_" + i), uri("p"), uri("o"))).next());
        } finally {
            g.end();
        }
    }
}

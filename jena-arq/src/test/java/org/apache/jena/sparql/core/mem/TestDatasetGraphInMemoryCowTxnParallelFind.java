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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.Test;

/**
 * Tests for the captured-view path and the parallel cross-graph
 * construction added to {@link DatasetGraphInMemoryCowTxn}.
 * <p>
 * Three concerns drive the cases:
 * <ol>
 *   <li><b>Functional equivalence.</b> Parallel and sequential paths must
 *       produce the same set of quads regardless of how the dataset is
 *       configured (ForkMode SEQUENTIAL vs PARALLEL).</li>
 *   <li><b>Snapshot stability across threads.</b> When a parallel cross-graph
 *       find runs on FJP workers, those workers must observe the
 *       per-graph view captured by the originating thread at {@code begin(READ)}
 *       — not the latest published snapshot (which a concurrent writer may
 *       have just moved past).</li>
 *   <li><b>Threshold and routing.</b> Below the parallel threshold, the
 *       parallel path must not be used (FJP submission overhead would
 *       dominate). Above, it must be used under PARALLEL mode.</li>
 * </ol>
 */
public class TestDatasetGraphInMemoryCowTxnParallelFind {

    private static Node uri(String s) { return NodeFactory.createURI("http://ex/" + s); }
    private static Quad nq(String g, String s, String p, String o) {
        return Quad.create(uri(g), uri(s), uri(p), uri(o));
    }
    private static Quad dq(String s, String p, String o) {
        return Quad.create(Quad.defaultGraphIRI, uri(s), uri(p), uri(o));
    }

    private static DatasetGraph populate(GraphMemIndexedSetCowTxn.ForkMode mode, int namedGraphs, int perGraph) {
        DatasetGraph ds = new DatasetGraphInMemoryCowTxn(mode);
        ds.begin(TxnType.WRITE);
        try {
            for (int g = 0; g < namedGraphs; g++) {
                for (int i = 0; i < perGraph; i++) {
                    ds.add(nq("g" + g, "s" + i, "p", "o" + i));
                }
            }
            ds.commit();
        } finally {
            ds.end();
        }
        return ds;
    }

    private static Set<Quad> collectAll(DatasetGraph ds) {
        ds.begin(TxnType.READ);
        try {
            return Iter.toSet(ds.find(Node.ANY, Node.ANY, Node.ANY, Node.ANY));
        } finally {
            ds.end();
        }
    }

    // ----- Functional equivalence -------------------------------------------

    /** Graph count chosen to exceed PARALLEL_CROSS_GRAPH_THRESHOLD (=16). */
    private static final int ABOVE_THRESHOLD = 20;

    @Test
    public void parallelFindMatchesSequentialFind_aboveThreshold() {
        DatasetGraph seq = populate(GraphMemIndexedSetCowTxn.ForkMode.SEQUENTIAL, ABOVE_THRESHOLD, 50);
        DatasetGraph par = populate(GraphMemIndexedSetCowTxn.ForkMode.PARALLEL,   ABOVE_THRESHOLD, 50);

        Set<Quad> fromSeq = collectAll(seq);
        Set<Quad> fromPar = collectAll(par);

        assertEquals(ABOVE_THRESHOLD * 50, fromSeq.size());
        assertEquals(fromSeq, fromPar);
    }

    @Test
    public void parallelFindMatchesSequentialFind_belowThreshold() {
        // 2 graphs is below threshold. Both ForkModes go through the
        // sequential-captured-view path; results must be identical.
        DatasetGraph seq = populate(GraphMemIndexedSetCowTxn.ForkMode.SEQUENTIAL, 2, 50);
        DatasetGraph par = populate(GraphMemIndexedSetCowTxn.ForkMode.PARALLEL,   2, 50);

        assertEquals(collectAll(seq), collectAll(par));
    }

    @Test
    public void parallelFindWithBoundPredicate_matchesSequential() {
        DatasetGraph par = populate(GraphMemIndexedSetCowTxn.ForkMode.PARALLEL, ABOVE_THRESHOLD, 30);
        par.begin(TxnType.READ);
        try {
            Set<Quad> got = Iter.toSet(par.find(Node.ANY, Node.ANY, uri("p"), Node.ANY));
            assertEquals(ABOVE_THRESHOLD * 30, got.size());
        } finally {
            par.end();
        }
    }

    @Test
    public void parallelStreamProducesAllQuads() {
        DatasetGraph par = populate(GraphMemIndexedSetCowTxn.ForkMode.PARALLEL, ABOVE_THRESHOLD, 30);
        par.begin(TxnType.READ);
        try {
            List<Quad> all = par.stream(Node.ANY, Node.ANY, Node.ANY, Node.ANY)
                    .collect(Collectors.toList());
            assertEquals(ABOVE_THRESHOLD * 30, all.size());
        } finally {
            par.end();
        }
    }

    @Test
    public void parallelStreamSupportsCallerParallelDownstream() {
        DatasetGraph par = populate(GraphMemIndexedSetCowTxn.ForkMode.PARALLEL, ABOVE_THRESHOLD, 30);
        par.begin(TxnType.READ);
        try {
            // Callers can ask for downstream parallelism on the returned
            // stream. The captured views are thread-safe so the parallel
            // terminal pipeline does not need any per-thread setup.
            long count = par.stream(Node.ANY, Node.ANY, Node.ANY, Node.ANY)
                    .parallel()
                    .filter(q -> q.getPredicate().equals(uri("p")))
                    .count();
            assertEquals(ABOVE_THRESHOLD * 30, count);
        } finally {
            par.end();
        }
    }

    // ----- Snapshot stability across threads --------------------------------

    /**
     * The key correctness test for the FJP-worker case: a READ transaction
     * begins, then a writer commits a change to every graph, then the
     * READ transaction does a parallel cross-graph find. The find must
     * return the pre-writer state — workers must use the captured view,
     * not the latest published.
     */
    @Test
    public void parallelFindSeesCapturedSnapshotAfterConcurrentCommit() throws Exception {
        DatasetGraph par = populate(GraphMemIndexedSetCowTxn.ForkMode.PARALLEL, ABOVE_THRESHOLD, 10);

        CountDownLatch readerBegun = new CountDownLatch(1);
        CountDownLatch writerDone  = new CountDownLatch(1);
        AtomicReference<Long> seenInReader = new AtomicReference<>();
        AtomicReference<Throwable> readerError = new AtomicReference<>();

        Thread reader = new Thread(() -> {
            try {
                par.begin(TxnType.READ);
                readerBegun.countDown();
                writerDone.await(5, TimeUnit.SECONDS);
                long n = par.stream(Node.ANY, Node.ANY, Node.ANY, Node.ANY).count();
                seenInReader.set(n);
                par.end();
            } catch (Throwable t) {
                readerError.set(t);
            }
        });
        reader.start();
        assertTrue(readerBegun.await(5, TimeUnit.SECONDS));

        // Writer adds one fresh quad to every named graph.
        par.begin(TxnType.WRITE);
        for (int g = 0; g < ABOVE_THRESHOLD; g++)
            par.add(nq("g" + g, "newSubject", "newPred", "newObj"));
        par.commit();
        par.end();
        writerDone.countDown();

        reader.join(10_000);
        if (readerError.get() != null) throw new AssertionError("reader threw", readerError.get());

        // Reader pinned its view before the writer ran. It must NOT see the writer's new quads.
        assertEquals((long) ABOVE_THRESHOLD * 10, seenInReader.get(),
                "Parallel reader saw post-commit data — captured view was not honoured by FJP workers");

        // A fresh reader after the writer must see the new state.
        par.begin(TxnType.READ);
        try {
            long n = par.stream(Node.ANY, Node.ANY, Node.ANY, Node.ANY).count();
            assertEquals((long) ABOVE_THRESHOLD * 10 + ABOVE_THRESHOLD, n);
        } finally {
            par.end();
        }
    }

    /**
     * Iterators returned by {@code findInAnyNamedGraphs} from an
     * auto-wrapped (no-surrounding-transaction) read must continue to
     * iterate the snapshot captured at the moment of the call, even
     * after the auto-wrapping READ transaction has ended and a writer
     * has committed new data.
     */
    @Test
    public void autoWrappedFindIteratorIsStableAcrossWriterCommit() {
        DatasetGraph par = populate(GraphMemIndexedSetCowTxn.ForkMode.SEQUENTIAL, 4, 5);

        // No transaction; auto-wrap. Returned iterator holds CowStore refs
        // captured during the wrapped READ.
        Iterator<Quad> it = par.find(Node.ANY, Node.ANY, Node.ANY, Node.ANY);

        // A concurrent writer commits new data.
        par.begin(TxnType.WRITE);
        par.add(nq("g0", "after", "p", "o"));
        par.commit();
        par.end();

        // Consume the iterator. It must reflect the pre-writer state
        // (20 quads), not include the writer's new quad.
        Set<Quad> seen = new HashSet<>();
        while (it.hasNext()) seen.add(it.next());
        assertEquals(20, seen.size());
        assertFalse(seen.contains(nq("g0", "after", "p", "o")),
                "auto-wrap-returned iterator must not observe writes that happened after the wrap returned");
    }

    // ----- Routing / threshold ---------------------------------------------

    @Test
    public void singleGraphFindUnaffectedByForkMode() {
        DatasetGraph par = populate(GraphMemIndexedSetCowTxn.ForkMode.PARALLEL, ABOVE_THRESHOLD, 10);
        par.begin(TxnType.READ);
        try {
            Set<Quad> got = Iter.toSet(par.find(uri("g3"), Node.ANY, Node.ANY, Node.ANY));
            assertEquals(10, got.size());
            for (Quad q : got) assertEquals(uri("g3"), q.getGraph());
        } finally {
            par.end();
        }
    }

    @Test
    public void writeTxnCrossGraphFindFallsBackToPerThread() {
        // Inside a WRITE txn there are no captured views; the path
        // we exercise is the per-thread fallback. Result must still be
        // consistent — and must see our own writes.
        DatasetGraph par = populate(GraphMemIndexedSetCowTxn.ForkMode.PARALLEL, ABOVE_THRESHOLD, 10);
        par.begin(TxnType.WRITE);
        try {
            par.add(nq("g0", "fresh", "p", "o"));
            long n = par.stream(Node.ANY, Node.ANY, Node.ANY, Node.ANY).count();
            assertEquals((long) ABOVE_THRESHOLD * 10 + 1, n);
            par.commit();
        } finally {
            par.end();
        }
    }

    @Test
    public void defaultGraphRoutingPreservedUnderPARALLEL() {
        // The default graph slot is in the captured-views map but must
        // be filtered out of the named-graph cross-graph result, then
        // re-added by the wildcard find/stream path.
        DatasetGraph par = new DatasetGraphInMemoryCowTxn(GraphMemIndexedSetCowTxn.ForkMode.PARALLEL);
        par.begin(TxnType.WRITE);
        try {
            par.add(dq("s", "p", "o"));
            for (int g = 0; g < ABOVE_THRESHOLD; g++)
                par.add(nq("g" + g, "s", "p", "o"));
            par.commit();
        } finally {
            par.end();
        }

        par.begin(TxnType.READ);
        try {
            // Wildcard = default + every named graph.
            List<Quad> wild = par.stream(Node.ANY, Node.ANY, Node.ANY, Node.ANY)
                    .collect(Collectors.toList());
            assertEquals(ABOVE_THRESHOLD + 1, wild.size());

            // Named-graphs-only must not include the default graph.
            List<Quad> named = Iter.toList(par.findNG(Node.ANY, Node.ANY, Node.ANY, Node.ANY));
            assertEquals(ABOVE_THRESHOLD, named.size());
            for (Quad q : named)
                assertNotEquals(Quad.defaultGraphIRI, q.getGraph());
                // (defaultGraphNodeGenerated also fails the equality above by URI.)

            // Default-graph-only.
            List<Quad> dft = Iter.toList(par.find(Quad.defaultGraphIRI, Node.ANY, Node.ANY, Node.ANY));
            assertEquals(1, dft.size());
        } finally {
            par.end();
        }
    }

    @Test
    public void parallelPathWithZeroNamedGraphsReturnsEmpty() {
        DatasetGraph par = new DatasetGraphInMemoryCowTxn(GraphMemIndexedSetCowTxn.ForkMode.PARALLEL);
        par.begin(TxnType.READ);
        try {
            assertEquals(0, par.stream(Node.ANY, Node.ANY, Node.ANY, Node.ANY).count());
            assertFalse(par.find(Node.ANY, Node.ANY, Node.ANY, Node.ANY).hasNext());
        } finally {
            par.end();
        }
    }

    /**
     * Many readers stream cross-graph in parallel while a writer commits
     * repeatedly. Each reader's stream count must remain consistent with
     * the snapshot it captured at begin — not change mid-iteration.
     */
    @Test
    public void parallelReadersStableUnderConcurrentWrites() throws Exception {
        DatasetGraph par = populate(GraphMemIndexedSetCowTxn.ForkMode.PARALLEL, 6, 20);
        int baseline = 6 * 20;

        int numReaders = 6;
        CountDownLatch allReadersBegun = new CountDownLatch(numReaders);
        CountDownLatch writerMayProceed = new CountDownLatch(1);
        AtomicReference<Throwable> firstError = new AtomicReference<>();
        List<Thread> readers = new ArrayList<>();
        for (int i = 0; i < numReaders; i++) {
            Thread r = new Thread(() -> {
                try {
                    par.begin(TxnType.READ);
                    allReadersBegun.countDown();
                    writerMayProceed.await(5, TimeUnit.SECONDS);
                    long c1 = par.stream(Node.ANY, Node.ANY, Node.ANY, Node.ANY).count();
                    long c2 = par.stream(Node.ANY, Node.ANY, Node.ANY, Node.ANY).count();
                    if (c1 != baseline || c2 != baseline)
                        firstError.compareAndSet(null,
                                new AssertionError("reader saw inconsistent count: " + c1 + " then " + c2));
                    par.end();
                } catch (Throwable t) {
                    firstError.compareAndSet(null, t);
                }
            });
            readers.add(r);
            r.start();
        }
        assertTrue(allReadersBegun.await(5, TimeUnit.SECONDS));

        Thread writer = new Thread(() -> {
            try {
                for (int i = 0; i < 20; i++) {
                    par.begin(TxnType.WRITE);
                    par.add(nq("g0", "w" + i, "p", "o"));
                    par.commit();
                    par.end();
                }
            } catch (Throwable t) { firstError.compareAndSet(null, t); }
        });
        writer.start();
        writerMayProceed.countDown();

        writer.join(10_000);
        for (Thread r : readers) r.join(10_000);
        if (firstError.get() != null) fail("error: " + firstError.get());
    }
}

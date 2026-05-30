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
import org.apache.jena.mem.IndexingStrategy;
import org.apache.jena.sparql.core.Transactional;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Lightweight (not rigorous JMH) sanity comparison of the copy-on-write and MVCC
 * transactional graphs, run from the jena-arq reactor so it can be executed
 * directly:
 * {@code mvn -pl jena-arq -am test -Dtest=MvccVsCowComparison -Dsurefire.useFile=false}.
 * <p>
 * It isolates the scenario where the two designs differ most: the per-transaction
 * begin cost on a large graph. The copy-on-write graph forks the writer-private
 * probe table at every {@code begin(WRITE)} (cost grows with graph size); the MVCC
 * graph never copies, so begin is O(1).
 */
public class MvccVsCowComparison {

    private static final int PRELOAD = 100_000;
    private static final int SMALL_TXNS = 2_000;
    private static final int READ_TXNS = 2_000;

    private static Triple gen(int i) {
        return Triple.create(
                NodeFactory.createURI("http://ex/s" + i),
                NodeFactory.createURI("http://ex/p" + (i & 7)),
                NodeFactory.createURI("http://ex/o" + i));
    }

    private static void preload(Graph g, int n) {
        ((Transactional) g).executeWrite(() -> {
            for (int i = 0; i < n; i++) {
                g.add(gen(i));
            }
        });
    }

    private static long smallWriteTxns(Graph g, int base, int count) {
        final Transactional t = (Transactional) g;
        final long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            final int id = base + i;
            t.executeWrite(() -> g.add(gen(id)));
        }
        return System.nanoTime() - start;
    }

    private static long readTxns(Graph g, int count) {
        final Transactional t = (Transactional) g;
        final AtomicLong sink = new AtomicLong();
        final long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            final Node s = NodeFactory.createURI("http://ex/s" + (i % PRELOAD));
            t.executeRead(() -> sink.addAndGet(g.find(s, Node.ANY, Node.ANY).toList().size()));
        }
        return (System.nanoTime() - start) + (sink.get() & 0);
    }

    private static double ms(long nanos) {
        return nanos / 1_000_000.0;
    }

    private void runVariant(String label, Supplier<Graph> factory) {
        Graph warm = factory.get();
        preload(warm, 10_000);
        smallWriteTxns(warm, 10_000, 500);
        readTxns(warm, 500);

        Graph g = factory.get();
        preload(g, PRELOAD);
        long writeNanos = smallWriteTxns(g, PRELOAD, SMALL_TXNS);
        long readNanos = readTxns(g, READ_TXNS);

        System.out.printf("  %-30s  write=%9.2f ms (%6.1f us/txn)   read=%9.2f ms (%5.1f us/txn)%n",
                label,
                ms(writeNanos), ms(writeNanos) / SMALL_TXNS * 1000.0,
                ms(readNanos), ms(readNanos) / READ_TXNS * 1000.0);
    }

    @Test
    public void compareCowVsMvcc() {
        System.out.printf("%nCoW vs MVCC — %d-triple base graph, %d tiny write txns, %d read txns%n",
                PRELOAD, SMALL_TXNS, READ_TXNS);
        System.out.println("(lower is better; the write column isolates per-transaction begin/commit cost)");
        runVariant("CoW EAGER (fork/begin)",
                () -> new GraphMemIndexedSetCowTxn(IndexingStrategy.EAGER,
                        GraphMemIndexedSetCowTxn.ForkMode.SEQUENTIAL));
        runVariant("MVCC EAGER (no copy)",
                () -> new GraphMemIndexedSetMvccTxn(IndexingStrategy.EAGER));
        runVariant("MVCC MINIMAL (no copy)",
                () -> new GraphMemIndexedSetMvccTxn(IndexingStrategy.MINIMAL));

        assertTrue(new GraphMemIndexedSetMvccTxn() instanceof Transactional);
    }
}

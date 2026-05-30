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

package org.apache.jena.sparql.core.mem.txn;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.sparql.core.mem.txn.helper.TxnGraphContext;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A fast, illustrative (not rigorous JMH) comparison of the copy-on-write and
 * MVCC transactional graphs, focused on the scenario where they differ most: the
 * per-transaction begin cost on a large graph. The copy-on-write graph forks the
 * writer-private probe table at every {@code begin(WRITE)} (cost proportional to
 * graph size); the MVCC graph never copies, so its begin is O(1).
 * <p>
 * Run explicitly: {@code mvn -pl jena-benchmarks/jena-benchmarks-jmh -am test
 * -Dtest=MvccVsCowQuickComparison}. For rigorous numbers use the JMH benchmarks
 * (e.g. {@code TestTxnGraphForkCost}) which now include the MVCC variants.
 */
public class MvccVsCowQuickComparison {

    private static final int PRELOAD = 100_000;   // base graph size
    private static final int SMALL_TXNS = 2_000;  // tiny write transactions
    private static final int READ_TXNS = 2_000;   // read transactions

    private static Triple gen(int i) {
        return Triple.create(
                NodeFactory.createURI("http://ex/s" + i),
                NodeFactory.createURI("http://ex/p" + (i & 7)),
                NodeFactory.createURI("http://ex/o" + i));
    }

    private static void preload(Graph g, int n) {
        TxnGraphContext.writeTxn(g, () -> {
            for (int i = 0; i < n; i++) {
                g.add(gen(i));
            }
        });
    }

    /** Many tiny write transactions on a large graph: isolates begin/commit cost. */
    private static long smallWriteTxns(Graph g, int base, int count) {
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            final int id = base + i;
            TxnGraphContext.writeTxn(g, () -> g.add(gen(id)));
        }
        return System.nanoTime() - start;
    }

    /** Many read transactions, each scanning one subject's triples. */
    private static long readTxns(Graph g, int count) {
        final AtomicLong sink = new AtomicLong();
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            final Node s = NodeFactory.createURI("http://ex/s" + (i % PRELOAD));
            TxnGraphContext.readTxn(g, () ->
                    sink.addAndGet(g.find(s, Node.ANY, Node.ANY).toList().size()));
        }
        return System.nanoTime() - start + (sink.get() & 0);
    }

    private static double ms(long nanos) {
        return nanos / 1_000_000.0;
    }

    private void runVariant(String variant) {
        // Warm up on a throwaway instance.
        Graph warm = TxnGraphContext.createGraph(variant);
        preload(warm, 10_000);
        smallWriteTxns(warm, 10_000, 500);
        readTxns(warm, 500);

        Graph g = TxnGraphContext.createGraph(variant);
        preload(g, PRELOAD);
        long writeNanos = smallWriteTxns(g, PRELOAD, SMALL_TXNS);
        long readNanos = readTxns(g, READ_TXNS);

        System.out.printf("  %-44s  write=%9.2f ms (%6.1f us/txn)   read=%9.2f ms (%5.1f us/txn)%n",
                variant,
                ms(writeNanos), ms(writeNanos) / SMALL_TXNS * 1000.0,
                ms(readNanos), ms(readNanos) / READ_TXNS * 1000.0);
    }

    @Test
    public void compareCowVsMvcc() {
        System.out.printf("%nCoW vs MVCC — %d-triple base graph, %d tiny write txns, %d read txns%n",
                PRELOAD, SMALL_TXNS, READ_TXNS);
        System.out.println("(lower is better; write column isolates per-transaction begin/commit cost)");
        for (String variant : new String[]{
                TxnGraphContext.GMIS_COW_TXN_EAGER_SEQ,
                TxnGraphContext.GMIS_COW_TXN_EAGER_PARALLEL,
                TxnGraphContext.GMIS_MVCC_TXN_EAGER,
                TxnGraphContext.GMIS_MVCC_TXN_MINIMAL,
        }) {
            runVariant(variant);
        }
        // Sanity: the variants must be Transactional (otherwise the harness lied).
        org.junit.jupiter.api.Assertions.assertTrue(
                TxnGraphContext.createGraph(TxnGraphContext.GMIS_MVCC_TXN_EAGER) instanceof Transactional);
    }
}

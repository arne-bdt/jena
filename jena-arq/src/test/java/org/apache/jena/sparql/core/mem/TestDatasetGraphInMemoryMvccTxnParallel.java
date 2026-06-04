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

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.mem.IndexingStrategy;
import org.apache.jena.mem.store.mvcc.MvccTripleStore;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.system.Txn;
import org.junit.jupiter.api.Test;

import static org.apache.jena.sparql.core.mem.CowTxnTestHelper.t;
import static org.junit.jupiter.api.Assertions.*;

/**
 * The {@link MvccTripleStore.ParallelMode} knob on {@link DatasetGraphInMemoryMvccTxn}
 * (the MVCC analogue of the copy-on-write dataset's fork mode): it must be reported
 * back, and {@link MvccTripleStore.ParallelMode#PARALLEL} — which collects per-graph
 * iterators in parallel for cross-graph reads over enough named graphs — must yield
 * exactly the same results as {@link MvccTripleStore.ParallelMode#SEQUENTIAL}.
 */
public class TestDatasetGraphInMemoryMvccTxnParallel {

    /** Comfortably above DatasetGraphInMemoryMvccTxn.PARALLEL_CROSS_GRAPH_THRESHOLD (16). */
    private static final int GRAPHS = 40;

    private static Node g(int i) {
        return NodeFactory.createURI("http://ex/g" + i);
    }

    private static DatasetGraphInMemoryMvccTxn populated(MvccTripleStore.ParallelMode mode) {
        DatasetGraphInMemoryMvccTxn dsg = new DatasetGraphInMemoryMvccTxn(IndexingStrategy.EAGER, mode);
        Txn.executeWrite(dsg, () -> {
            for (int i = 0; i < GRAPHS; i++) {
                dsg.add(Quad.create(g(i), t("s", "p", "o" + i)));
                dsg.add(Quad.create(g(i), t("s2", "p", "o")));
            }
        });
        return dsg;
    }

    @Test
    public void parallelModeReported() {
        assertEquals(MvccTripleStore.ParallelMode.SEQUENTIAL,
                new DatasetGraphInMemoryMvccTxn(IndexingStrategy.EAGER).getParallelMode());
        assertEquals(MvccTripleStore.ParallelMode.PARALLEL,
                new DatasetGraphInMemoryMvccTxn(IndexingStrategy.EAGER,
                        MvccTripleStore.ParallelMode.PARALLEL).getParallelMode());
    }

    @Test
    public void parallelCrossGraphFindAgreesWithSequential() {
        DatasetGraphInMemoryMvccTxn seq = populated(MvccTripleStore.ParallelMode.SEQUENTIAL);
        DatasetGraphInMemoryMvccTxn par = populated(MvccTripleStore.ParallelMode.PARALLEL);

        // Cross-graph reads over all named graphs (ANY graph term) take the parallel
        // collection path under PARALLEL with >= 16 graphs; results must match.
        assertEquals(2L * GRAPHS, countFindAny(seq));
        assertEquals(countFindAny(seq), countFindAny(par),
                "parallel cross-graph find must agree with sequential");

        // A partial pattern across all named graphs (s, ANY) -> one match per graph.
        assertEquals((long) GRAPHS, countFindPattern(seq, NodeFactory.createURI("http://ex/s")));
        assertEquals(countFindPattern(seq, NodeFactory.createURI("http://ex/s")),
                countFindPattern(par, NodeFactory.createURI("http://ex/s")));
    }

    @Test
    public void parallelCrossGraphStreamAgreesWithSequential() {
        DatasetGraphInMemoryMvccTxn seq = populated(MvccTripleStore.ParallelMode.SEQUENTIAL);
        DatasetGraphInMemoryMvccTxn par = populated(MvccTripleStore.ParallelMode.PARALLEL);

        // Wildcard-graph stream takes the dedicated parallel per-graph-stream path
        // under PARALLEL with >= 16 graphs; results must match sequential.
        assertEquals(2L * GRAPHS, countStreamAny(seq));
        assertEquals(countStreamAny(seq), countStreamAny(par),
                "parallel cross-graph stream must agree with sequential");

        final Node s = NodeFactory.createURI("http://ex/s");
        assertEquals((long) GRAPHS, countStreamPattern(seq, s));
        assertEquals(countStreamPattern(seq, s), countStreamPattern(par, s));
    }

    @Test
    public void unionGraphStreamAgrees() {
        DatasetGraphInMemoryMvccTxn seq = populated(MvccTripleStore.ParallelMode.SEQUENTIAL);
        DatasetGraphInMemoryMvccTxn par = populated(MvccTripleStore.ParallelMode.PARALLEL);
        // The union graph deduplicates triples: (s,p,o0..oN-1) are distinct (GRAPHS)
        // and (s2,p,o) repeats in every graph -> 1, so GRAPHS + 1 in total.
        assertEquals((long) GRAPHS + 1, countUnionGraphStream(seq));
        assertEquals(countUnionGraphStream(seq), countUnionGraphStream(par),
                "parallel union-graph stream must agree with sequential");
    }

    private static long countFindAny(DatasetGraphInMemoryMvccTxn dsg) {
        return Txn.calculateRead(dsg,
                () -> Iter.count(dsg.find(Node.ANY, Node.ANY, Node.ANY, Node.ANY)));
    }

    private static long countFindPattern(DatasetGraphInMemoryMvccTxn dsg, Node subject) {
        return Txn.calculateRead(dsg,
                () -> Iter.count(dsg.find(Node.ANY, subject, Node.ANY, Node.ANY)));
    }

    private static long countStreamAny(DatasetGraphInMemoryMvccTxn dsg) {
        return Txn.calculateRead(dsg,
                () -> dsg.stream(Node.ANY, Node.ANY, Node.ANY, Node.ANY).count());
    }

    private static long countStreamPattern(DatasetGraphInMemoryMvccTxn dsg, Node subject) {
        return Txn.calculateRead(dsg,
                () -> dsg.stream(Node.ANY, subject, Node.ANY, Node.ANY).count());
    }

    private static long countUnionGraphStream(DatasetGraphInMemoryMvccTxn dsg) {
        return Txn.calculateRead(dsg, () -> {
            final Graph union = dsg.getUnionGraph();
            return union.stream().count();
        });
    }
}

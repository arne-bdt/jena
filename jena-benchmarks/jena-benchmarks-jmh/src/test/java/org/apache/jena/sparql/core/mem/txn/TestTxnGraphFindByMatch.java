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
import org.apache.jena.graph.Triple;
import org.apache.jena.jmh.JmhDefaultOptions;
import org.apache.jena.mem.graph.helper.Releases;
import org.apache.jena.sparql.core.mem.txn.helper.TxnGraphContext;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pattern-match throughput inside a read transaction. Cycles through
 * the loaded triples and probes the graph with a {@code SUB_ANY_ANY}
 * pattern (i.e. the subject of the current triple, with predicate and
 * object as wildcards). This exercises the strategy's
 * {@code findMatch}/{@code streamMatch} path and lets the
 * non-transactional baseline, Phase A, and Phase B variants be
 * compared directly.
 */
@State(Scope.Benchmark)
public class TestTxnGraphFindByMatch {

    @Param({
            "../testing/cheeses-0.1.ttl",
            "../testing/pizza.owl.rdf",
            "../testing/data.nt.gz",
    })
    public String param0_GraphUri;

    @Param({
            TxnGraphContext.GMIS_BASELINE_EAGER,
            TxnGraphContext.GMIS_BASELINE_LAZY,
            TxnGraphContext.GMIS_TXN_EAGER,
            TxnGraphContext.GMIS_TXN_LAZY,
            TxnGraphContext.GMIS_COW_TXN_EAGER_SEQ,
            TxnGraphContext.GMIS_COW_TXN_LAZY,
            TxnGraphContext.GMIS_COW_TXN_LAZY_PARALLEL,
    })
    public String param1_GraphImplementation;

    private Graph graph;
    private List<Triple> triples;
    private final AtomicInteger cursor = new AtomicInteger();

    @Setup(Level.Trial)
    public void setupTrial() {
        graph = TxnGraphContext.createGraph(param1_GraphImplementation);
        triples = Releases.current.readTriples(param0_GraphUri);
        TxnGraphContext.writeTxn(graph, () -> triples.forEach(graph::add));
    }

    @Benchmark
    public long findBySubject() {
        int idx = cursor.getAndIncrement();
        Triple probe = triples.get(Math.floorMod(idx, triples.size()));
        Node subject = probe.getSubject();
        long[] count = { 0 };
        TxnGraphContext.readTxn(graph,
                () -> count[0] = graph.find(subject, Node.ANY, Node.ANY).toList().size());
        return count[0];
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JmhDefaultOptions.getDefaults(this.getClass()).build();
        Assert.assertNotNull(new Runner(opt).run());
    }
}

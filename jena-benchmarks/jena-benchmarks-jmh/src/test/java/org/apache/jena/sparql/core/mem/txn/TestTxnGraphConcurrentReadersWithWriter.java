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
import org.apache.jena.jmh.JmhDefaultOptions;
import org.apache.jena.mem.graph.helper.Releases;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.sparql.core.mem.txn.helper.TxnGraphContext;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mixed-thread benchmark: one writer thread doing tight add/commit
 * cycles, and {@code N} reader threads issuing partial-pattern lookups
 * inside read transactions. The two transactional graphs (Phase A
 * deep-copy, Phase B copy-on-write) should diverge most starkly here,
 * because Phase A's deep copy on every begin(WRITE) blocks readers from
 * progressing while Phase B's fork is allocation-light.
 * <p>
 * The {@link Group} JMH grouping puts readers and the writer on the
 * same workload so JMH's per-thread accounting separates their
 * throughput.
 */
@State(Scope.Benchmark)
public class TestTxnGraphConcurrentReadersWithWriter {

    @Param({
            "../testing/cheeses-0.1.ttl",
            "../testing/pizza.owl.rdf",
    })
    public String param0_GraphUri;

    @Param({
            TxnGraphContext.GMIS_TXN_EAGER,
            TxnGraphContext.GMIS_COW_TXN_EAGER_SEQ,
            TxnGraphContext.GMIS_COW_TXN_EAGER_PARALLEL,
    })
    public String param1_GraphImplementation;

    private Transactional graph;
    private Graph rawGraph;
    private List<Triple> triples;
    /** Triple to add at each writer iteration; cycles through the input. */
    private final AtomicInteger writeCursor = new AtomicInteger();
    /** Triple to look up at each reader iteration; cycles through the input. */
    private final AtomicInteger readCursor = new AtomicInteger();

    @Setup(Level.Trial)
    public void setupTrial() {
        Graph g = TxnGraphContext.createGraph(param1_GraphImplementation);
        triples = Releases.current.readTriples(param0_GraphUri);
        // Pre-load the graph with everything; the writer will then
        // re-add (idempotent) to keep the workload steady.
        TxnGraphContext.writeTxn(g, () -> triples.forEach(g::add));
        rawGraph = g;
        graph = (Transactional) g;
    }

    @Benchmark
    @Group("readWrite")
    @GroupThreads(4)
    public long reader() {
        int idx = readCursor.getAndIncrement();
        Triple probe = triples.get(Math.floorMod(idx, triples.size()));
        Node subject = probe.getSubject();
        graph.begin(TxnType.READ);
        try {
            // Partial-pattern lookup forces the strategy to do work.
            return rawGraph.find(subject, Node.ANY, Node.ANY).toList().size();
        } finally {
            graph.end();
        }
    }

    @Benchmark
    @Group("readWrite")
    @GroupThreads(1)
    public Object writer() {
        int idx = writeCursor.getAndIncrement();
        // Always-new triple so commits are visible churn, not no-ops.
        Triple t = Triple.create(
                NodeFactory.createURI("http://ex/writer/" + idx),
                NodeFactory.createURI("http://ex/p"),
                NodeFactory.createURI("http://ex/o"));
        graph.begin(TxnType.WRITE);
        try {
            rawGraph.add(t);
            graph.commit();
        } finally {
            graph.end();
        }
        return t;
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JmhDefaultOptions.getDefaults(this.getClass()).build();
        Assert.assertNotNull(new Runner(opt).run());
    }
}

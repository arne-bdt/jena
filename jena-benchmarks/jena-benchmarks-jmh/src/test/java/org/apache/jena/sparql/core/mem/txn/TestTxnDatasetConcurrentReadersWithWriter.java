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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.jmh.JmhDefaultOptions;
import org.apache.jena.mem.graph.helper.Releases;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.mem.txn.helper.TxnDatasetContext;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;

/**
 * Mixed-thread MRSW benchmark at the dataset level: one writer thread
 * doing tight add/commit cycles, and four reader threads doing partial-
 * pattern lookups inside read transactions. This is the dataset-level
 * analogue of {@code TestTxnGraphConcurrentReadersWithWriter}.
 * <p>
 * The COW dataset's lazy-fork begin(WRITE) should make writer iterations
 * cheaper than {@link org.apache.jena.sparql.core.mem.DatasetGraphInMemory}'s
 * lock-heavy begin, while readers should be lock-free in both
 * implementations.
 */
@State(Scope.Benchmark)
public class TestTxnDatasetConcurrentReadersWithWriter {

    @Param({
            "../testing/cheeses-0.1.ttl",
            "../testing/pizza.owl.rdf",
    })
    public String param0_GraphUri;

    @Param({
            TxnDatasetContext.DSG_IN_MEMORY,
            TxnDatasetContext.DSG_COW_TXN_SEQ,
            TxnDatasetContext.DSG_COW_TXN_PARALLEL,
    })
    public String param1_Implementation;

    private DatasetGraph dsg;
    private List<Triple> triples;
    private final AtomicInteger writeCursor = new AtomicInteger();
    private final AtomicInteger readCursor = new AtomicInteger();

    @Setup(Level.Trial)
    public void setupTrial() {
        dsg = TxnDatasetContext.createDataset(param1_Implementation);
        triples = Releases.current.readTriples(param0_GraphUri);
        dsg.begin(TxnType.WRITE);
        try {
            for (Triple t : triples)
                dsg.add(Quad.create(Quad.defaultGraphIRI, t));
            dsg.commit();
        } finally {
            dsg.end();
        }
    }

    @Benchmark
    @Group("readWrite")
    @GroupThreads(4)
    public long reader() {
        int idx = readCursor.getAndIncrement();
        Triple probe = triples.get(Math.floorMod(idx, triples.size()));
        Node subject = probe.getSubject();
        dsg.begin(TxnType.READ);
        try {
            return dsg.getDefaultGraph().find(subject, Node.ANY, Node.ANY).toList().size();
        } finally {
            dsg.end();
        }
    }

    @Benchmark
    @Group("readWrite")
    @GroupThreads(1)
    public Object writer() {
        int idx = writeCursor.getAndIncrement();
        // Always-fresh triple so commits represent real churn, not no-ops.
        Triple t = Triple.create(
                NodeFactory.createURI("http://ex/writer/" + idx),
                NodeFactory.createURI("http://ex/p"),
                NodeFactory.createURI("http://ex/o"));
        dsg.begin(TxnType.WRITE);
        try {
            dsg.add(Quad.create(Quad.defaultGraphIRI, t));
            dsg.commit();
        } finally {
            dsg.end();
        }
        return t;
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JmhDefaultOptions.getDefaults(this.getClass()).build();
        Assert.assertNotNull(new Runner(opt).run());
    }
}

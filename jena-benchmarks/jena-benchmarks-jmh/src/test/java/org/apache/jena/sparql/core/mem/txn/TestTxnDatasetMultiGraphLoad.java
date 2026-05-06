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
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;

/**
 * Multi-graph bulk-load benchmark: every triple is added under one of
 * {@link #NUM_GRAPHS} named graphs, round-robin by index. Highlights the
 * dataset-level cost of routing into multiple per-graph stores — where
 * {@link org.apache.jena.sparql.core.mem.DatasetGraphInMemoryCowTxn}'s
 * lazy fork-on-first-write should win against
 * {@link org.apache.jena.sparql.core.mem.DatasetGraphInMemory}'s single
 * HexTable.
 */
@State(Scope.Benchmark)
public class TestTxnDatasetMultiGraphLoad {

    private static final int NUM_GRAPHS = 8;

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

    private List<Triple> triples;
    private Node[] graphNodes;

    @Setup(Level.Trial)
    public void setupTrial() {
        triples = Releases.current.readTriples(param0_GraphUri);
        graphNodes = new Node[NUM_GRAPHS];
        for (int i = 0; i < NUM_GRAPHS; i++)
            graphNodes[i] = NodeFactory.createURI("http://ex/g" + i);
    }

    @Benchmark
    public DatasetGraph loadMultipleGraphs() {
        DatasetGraph dsg = TxnDatasetContext.createDataset(param1_Implementation);
        dsg.begin(TxnType.WRITE);
        try {
            int i = 0;
            for (Triple t : triples) {
                dsg.add(Quad.create(graphNodes[i++ % NUM_GRAPHS], t));
            }
            dsg.commit();
        } finally {
            dsg.end();
        }
        return dsg;
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JmhDefaultOptions.getDefaults(this.getClass()).build();
        Assert.assertNotNull(new Runner(opt).run());
    }
}

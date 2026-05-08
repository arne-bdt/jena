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

/**
 * Isolates the cost of a write transaction that performs no work: a tight
 * loop of {@code begin(WRITE) / commit / end} cycles on a pre-loaded
 * graph. This measures the {@code forkForWrite} machinery — the central
 * claim of Phase B is that this stays cheap even on large graphs because
 * the spine arrays are shared, not copied.
 * <p>
 * Only transactional variants are exercised here — the benchmark would
 * be a no-op against a non-transactional graph.
 */
@State(Scope.Benchmark)
public class TestTxnGraphForkCost {

    @Param({
            "../testing/cheeses-0.1.ttl",
            "../testing/pizza.owl.rdf",
            "../testing/data.nt.gz",
    })
    public String param0_GraphUri;

    @Param({
            TxnGraphContext.GMIS_TXN_EAGER,
            TxnGraphContext.GMIS_TXN_LAZY,
            TxnGraphContext.GMIS_COW_TXN_EAGER_SEQ,
            TxnGraphContext.GMIS_COW_TXN_EAGER_PARALLEL,
            TxnGraphContext.GMIS_COW_TXN_LAZY,
            TxnGraphContext.GMIS_COW_TXN_LAZY_PARALLEL,
    })
    public String param1_GraphImplementation;

    private Transactional graph;

    @Setup(Level.Trial)
    public void setupTrial() {
        Graph g = TxnGraphContext.createGraph(param1_GraphImplementation);
        List<Triple> triples = Releases.current.readTriples(param0_GraphUri);
        TxnGraphContext.writeTxn(g, () -> triples.forEach(g::add));
        // The benchmark path requires a Transactional; the @Param list
        // above only includes those.
        graph = (Transactional) g;
    }

    /**
     * One full begin-commit-end cycle without modifying the graph.
     * Measures the publish-side forkForWrite cost in isolation.
     */
    @Benchmark
    public void emptyWriteCycle() {
        graph.begin(TxnType.WRITE);
        graph.commit();
        graph.end();
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JmhDefaultOptions.getDefaults(this.getClass()).build();
        Assert.assertNotNull(new Runner(opt).run());
    }
}

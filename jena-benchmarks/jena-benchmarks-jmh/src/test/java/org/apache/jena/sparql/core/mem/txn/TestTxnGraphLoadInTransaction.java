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
import org.apache.jena.sparql.core.mem.txn.helper.TxnGraphContext;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.util.List;

/**
 * Bulk-load benchmark for the transactional graph implementations:
 * {@code begin(WRITE) → add* → commit → end}, measuring the dominant
 * cost of a one-shot load including the fork/copy machinery at begin
 * and the publication at commit.
 * <p>
 * The non-transactional {@link org.apache.jena.mem.GraphMemIndexedSet}
 * is included as a baseline so the transactional variants' overhead can
 * be quantified.
 */
@State(Scope.Benchmark)
public class TestTxnGraphLoadInTransaction {

    @Param({
            "../testing/cheeses-0.1.ttl",
            "../testing/pizza.owl.rdf",
    })
    public String param0_GraphUri;

    @Param({
            TxnGraphContext.GMIS_BASELINE_EAGER,
            TxnGraphContext.GMIS_BASELINE_LAZY,
            TxnGraphContext.GMIS_TXN_EAGER,
            TxnGraphContext.GMIS_TXN_LAZY,
            TxnGraphContext.GMIS_COW_TXN_EAGER_SEQ,
            TxnGraphContext.GMIS_COW_TXN_EAGER_PARALLEL,
            TxnGraphContext.GMIS_COW_TXN_LAZY,
            TxnGraphContext.GMIS_COW_TXN_LAZY_PARALLEL,
    })
    public String param1_GraphImplementation;

    private List<Triple> triples;

    @Setup(Level.Trial)
    public void setupTrial() {
        triples = Releases.current.readTriples(param0_GraphUri);
    }

    @Benchmark
    public Graph loadInTransaction() {
        Graph g = TxnGraphContext.createGraph(param1_GraphImplementation);
        TxnGraphContext.writeTxn(g, () -> triples.forEach(g::add));
        Assert.assertEquals(triples.size(), g.size());
        return g;
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JmhDefaultOptions.getDefaults(this.getClass()).build();
        Assert.assertNotNull(new Runner(opt).run());
    }
}

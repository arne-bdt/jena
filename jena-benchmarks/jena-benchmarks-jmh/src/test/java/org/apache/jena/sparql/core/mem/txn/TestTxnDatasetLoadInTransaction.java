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
 * Bulk-load benchmark for the dataset-level transactional implementations,
 * dataset-level analogue of {@code TestTxnGraphLoadInTransaction}.
 * <p>
 * One write transaction adds every triple of the input file into the
 * default graph, then commits. Measures the dominant cost of a one-shot
 * load including transaction setup, per-triple add overhead via the
 * Quad routing layer, and publication at commit.
 */
@State(Scope.Benchmark)
public class TestTxnDatasetLoadInTransaction {

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

    @Setup(Level.Trial)
    public void setupTrial() {
        triples = Releases.current.readTriples(param0_GraphUri);
    }

    @Benchmark
    public DatasetGraph loadInTransaction() {
        DatasetGraph dsg = TxnDatasetContext.createDataset(param1_Implementation);
        dsg.begin(TxnType.WRITE);
        try {
            for (Triple t : triples)
                dsg.add(Quad.create(Quad.defaultGraphIRI, t));
            dsg.commit();
        } finally {
            dsg.end();
        }
        Assert.assertEquals(triples.size(), dsg.getDefaultGraph().size());
        return dsg;
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JmhDefaultOptions.getDefaults(this.getClass()).build();
        Assert.assertNotNull(new Runner(opt).run());
    }
}

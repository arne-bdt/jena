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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

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
 * Mixed delete-then-add workload run as repeated write transactions at
 * the dataset level. Models a typical update workload: many small write
 * transactions each touching a small fraction of the data.
 * <p>
 * Each invocation runs {@link #ITERATION_LIMIT} write transactions; each
 * transaction touches roughly {@code triples.size() * UPDATE_RATIO}
 * triples. The triple list is reshuffled between iterations (deterministic
 * seed) so the per-iteration touch set varies.
 */
@State(Scope.Benchmark)
public class TestTxnDatasetTransactions {

    /** Fraction of the loaded triples deleted and re-added per iteration. */
    private static final double UPDATE_RATIO = 0.003;
    /** Iterations per benchmark invocation. */
    private static final int ITERATION_LIMIT = 60;
    /** Seed used by the per-iteration shuffle. */
    private static final int SHUFFLE_SEED = 4721;

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
    private int updateCount;

    @Setup(Level.Trial)
    public void setupTrial() {
        triples = Releases.current.readTriples(param0_GraphUri);
        updateCount = Math.max(1, (int) (triples.size() * UPDATE_RATIO));
    }

    @Benchmark
    public DatasetGraph repeatedSmallTransactions() {
        DatasetGraph dsg = TxnDatasetContext.createDataset(param1_Implementation);

        // Pre-load.
        dsg.begin(TxnType.WRITE);
        try {
            for (Triple t : triples)
                dsg.add(Quad.create(Quad.defaultGraphIRI, t));
            dsg.commit();
        } finally {
            dsg.end();
        }

        List<Triple> shuffled = new ArrayList<>(triples);
        Random rng = new Random(SHUFFLE_SEED);

        for (int iter = 0; iter < ITERATION_LIMIT; iter++) {
            Collections.shuffle(shuffled, rng);
            // Always split into two transactions every other iteration —
            // mixes "single big txn" and "two small txns" patterns.
            boolean splitTxn = (iter & 1) == 0;
            int half = splitTxn ? updateCount / 2 : updateCount;

            dsg.begin(TxnType.WRITE);
            try {
                for (int i = 0; i < half; i++) {
                    Triple t = shuffled.get(i);
                    dsg.delete(Quad.create(Quad.defaultGraphIRI, t));
                    dsg.add(Quad.create(Quad.defaultGraphIRI, t));
                }
                dsg.commit();
            } finally {
                dsg.end();
            }

            if (splitTxn) {
                dsg.begin(TxnType.WRITE);
                try {
                    for (int i = half; i < updateCount; i++) {
                        Triple t = shuffled.get(i);
                        dsg.delete(Quad.create(Quad.defaultGraphIRI, t));
                        dsg.add(Quad.create(Quad.defaultGraphIRI, t));
                    }
                    dsg.commit();
                } finally {
                    dsg.end();
                }
            }
        }
        return dsg;
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JmhDefaultOptions.getDefaults(this.getClass()).build();
        Assert.assertNotNull(new Runner(opt).run());
    }
}

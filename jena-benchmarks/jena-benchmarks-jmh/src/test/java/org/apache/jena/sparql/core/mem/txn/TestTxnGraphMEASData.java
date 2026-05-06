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
import java.util.List;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.jmh.JmhDefaultOptions;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.sparql.core.mem.txn.helper.MEASData;
import org.apache.jena.sparql.core.mem.txn.helper.TxnGraphContext;
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
 * MEAS-style update workload: each iteration commits {@code numberOfTransactions}
 * write transactions, each replacing a slice of the timestamp/status/value
 * triples on the pre-loaded measurement resources. The slice size is
 * {@code totalValues / numberOfTransactions}, so the total triple churn per
 * iteration is roughly constant — the parameter varies how it is split.
 * <p>
 * Useful for comparing the fork/publish cost of the transactional variants
 * under realistic SCADA-style refresh traffic, where a small but non-trivial
 * fraction of the triples is rewritten each cycle.
 */
@State(Scope.Benchmark)
public class TestTxnGraphMEASData {

    /** Number of AnalogValue resources to pre-load; each contributes 5 triples. */
    private static final int NUMBER_OF_ANALOG_VALUES = 1_600;
    /** Number of DiscreteValue resources to pre-load; each contributes 5 triples. */
    private static final int NUMBER_OF_DISCRETE_VALUES = 400;

    private List<MEASData.AnalogValue> analogValues;
    private List<MEASData.DiscreteValue> discreteValues;

    @Param({
            TxnGraphContext.GMIS_DEFAULT_IN_MEMORY,
            TxnGraphContext.GMIS_COW_TXN_EAGER_SEQ,
            TxnGraphContext.GMIS_COW_TXN_EAGER_PARALLEL,
    })
    public String param1_GraphImplementation;

    @Param({"10", "100", "250", "500", "1000"})
    public int numberOfTransactions;

    private Graph sut;

    @Benchmark
    public long update() {
        long changes = 0;
        for (int i = 0; i < numberOfTransactions; i++) {
            final var updates = nextUpdateBatch();
            final Transactional txn = (sut instanceof Transactional t) ? t : null;
            if (txn != null) {
                txn.begin(TxnType.WRITE);
            }
            for (var t : updates) {
                final var existing = sut.find(t.getSubject(), t.getPredicate(), Node.ANY).nextOptional();
                // Skip if the existing triple already has the new object — no churn needed.
                if (existing.isPresent() && t.getObject().equals(existing.get().getObject())) {
                    continue;
                }
                // Add first, then delete: this lets indexes update without transiently emptying a bucket.
                sut.add(t);
                changes++;
                if (existing.isPresent()) {
                    sut.delete(existing.get());
                    changes++;
                }
            }
            if (txn != null) {
                txn.commit();
                txn.end();
            }
        }
        return changes;
    }

    private List<Triple> nextUpdateBatch() {
        final int analogSlice = analogValues.size() / numberOfTransactions;
        final int discreteSlice = discreteValues.size() / numberOfTransactions;
        final var analogUpdates = MEASData.getRandomlyUpdatedAnalogValues(analogValues, analogSlice);
        final var discreteUpdates = MEASData.getRandomlyUpdatedDiscreteValues(discreteValues, discreteSlice);

        final var triples = new ArrayList<Triple>((analogUpdates.size() + discreteUpdates.size()) * 3);
        triples.addAll(MEASData.analogValuesToTriplesForUpdate(analogUpdates));
        triples.addAll(MEASData.discreteValuesToTriplesForUpdate(discreteUpdates));
        return triples;
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        this.analogValues = MEASData.generateRandomAnalogValues(NUMBER_OF_ANALOG_VALUES);
        this.discreteValues = MEASData.generateRandomDiscreteValues(NUMBER_OF_DISCRETE_VALUES);

        this.sut = TxnGraphContext.createGraph(param1_GraphImplementation);
        MEASData.addAnalogValuesToGraph(sut, analogValues);
        MEASData.addDiscreteValuesToGraph(sut, discreteValues);
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JmhDefaultOptions.getDefaults(this.getClass()).build();
        Assert.assertNotNull(new Runner(opt).run());
    }
}

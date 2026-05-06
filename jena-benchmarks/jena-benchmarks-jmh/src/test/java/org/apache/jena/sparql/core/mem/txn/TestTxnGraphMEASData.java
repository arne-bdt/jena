/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.sparql.core.mem.txn;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.jmh.JmhDefaultOptions;
import org.apache.jena.mem.graph.helper.Context;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.sparql.core.mem.txn.helper.MEASData;
import org.apache.jena.sparql.core.mem.txn.helper.TxnGraphContext;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.util.ArrayList;
import java.util.List;

@State(Scope.Benchmark)
public class TestTxnGraphMEASData {

    // recommended: 160_000 * 5 triples / value --> 800_000 triples
    private static final int NUMBER_OF_ANALOG_VALUES = 1_600;
    // 40_000 * 5 triples / value --> 200_000 triples
    private static final int NUMBER_OF_DIGITS = 400;

    private List<MEASData.AnalogValue> analogValues;
    private List<MEASData.DiscreteValue> discreteValues;

    @Param({
            TxnGraphContext.GMIS_DEFAULT_IN_MEMORY,
            TxnGraphContext.GMIS_COW_TXN_EAGER_SEQ,
            TxnGraphContext.GMIS_COW_TXN_EAGER_PARALLEL,
    })
    public String param1_GraphImplementation;


    @Param({
            "10",
            "100",
            "250",
            "500",
            "1000"
    })
    public int numberOfTransactions;

    private Context trialContext;
    private Graph sut;

    @Benchmark
    public long update() {
        long changes = 0;
        Transactional transactional = null;
        for(var i = 0; i < numberOfTransactions; i++) {
            final var updates = getUpdate();
            if (sut instanceof Transactional t) {
                transactional = t;
                transactional.begin(TxnType.WRITE);
            }
            for (var t : updates) {
                final var tripleToDelete = sut.find(t.getSubject(), t.getPredicate(), Node.ANY).nextOptional();
                if (tripleToDelete.isPresent()) {
                    //if the triple to delete is the same as the one to add, we don't need to do anything
                    if (t.getObject().equals(tripleToDelete.get().getObject())) {
                        continue;
                    }
                }
                //add first, to avoid removing index during deletion
                sut.add(t);
                changes++;
                if (tripleToDelete.isPresent()) {
                    sut.delete(tripleToDelete.get());
                    changes++;
                }
            }
            if (transactional != null) {
                transactional.commit();
                transactional.end();
            }
        }
        return changes;
    }

    private List<Triple> getUpdate() {
        var analogValuesToUpdate = MEASData.getRandomlyUpdatedAnalogValues(analogValues, analogValues.size() /  numberOfTransactions);
        var discreteValuesToUpdate = MEASData.getRandomlyUpdatedDiscreteValues(discreteValues, discreteValues.size() /  numberOfTransactions);


        final var updatedTriples = new ArrayList<Triple>((analogValuesToUpdate.size() + discreteValuesToUpdate.size())*3);
        MEASData.analogValuesToTriplesForUpdate(analogValuesToUpdate).forEach(updatedTriples::add);
        MEASData.disccreteValuesToTriplesForUpdate(discreteValuesToUpdate).forEach(updatedTriples::add);
        return  updatedTriples;
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        this.analogValues = MEASData.generateRandomAnalogValues(NUMBER_OF_ANALOG_VALUES);
        this.discreteValues = MEASData.generateRandomDiscreteValues(NUMBER_OF_DIGITS);

        this.sut = TxnGraphContext.createGraph(param1_GraphImplementation);
        MEASData.addAnalogValuesToGraph(sut, analogValues);
        MEASData.addDiscreteValuesToGraph(sut, discreteValues);
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JmhDefaultOptions.getDefaults(this.getClass())
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }
}

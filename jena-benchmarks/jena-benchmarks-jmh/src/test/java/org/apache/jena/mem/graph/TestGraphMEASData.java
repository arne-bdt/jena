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

package org.apache.jena.mem.graph;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.graph.helper.Context;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.mem.graph.helper.MEASData;
import org.apache.jena.mem.graph.helper.Releases;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.sparql.core.mem.DatasetGraphInMemory;
import org.apache.jena.sparql.core.mem.GraphMemTransactional;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

@State(Scope.Benchmark)
public class TestGraphMEASData {

    private final double ITERATION_LIMIT = 100;

    private List<MEASData.AnalogValue> analogValues = MEASData.generateRandomAnalogValues(100000);
    private List<MEASData.DiscreteValue> discreteValues = MEASData.generateRandomDiscreteValues(25000);

    @Param({
            "GraphMemFast (current)",
            "DatasetGraphInMemory (current)",
            "GraphMemTransactional (current)",
//            "GraphMemTxn (current)",
//            "GraphMemValue (current)",
//            "GraphMemRoaring EAGER (current)",
//            "GraphMemRoaring LAZY (current)",
//            "GraphMemRoaring LAZY_PARALLEL (current)",
//            "GraphMemRoaring MINIMAL (current)",
//            "GraphMemValue (Jena 5.6.0)",
//            "GraphMemFast (Jena 5.6.0)",
//            "GraphMemValue (Jena 5.6.0)",
    })
    public String param1_GraphImplementation;
    private Context trialContext;
    private Graph sut;
    private GraphMemTransactional graphMemTransactional;
    private DatasetGraphInMemory datasetGraphInMemory;
    private List<Triple> allTriplesCurrent;
    private List<Triple> triplesToDeleteFromSutCurrent;
    private Supplier<Long> graphTransactionsWithDeleteAndAdd;

    @Benchmark
    public long update() {
        long changes = 0;
        Transactional transactional = null;
        for(var i= 0 ; i < ITERATION_LIMIT; i++) {
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
        var analogValuesToUpdate = MEASData.getRandomlyUpdatedAnalogValues(analogValues, 500);
        var discreteValuesToUpdate = MEASData.getRandomlyUpdatedDiscreteValues(discreteValues, 100);


        final var updatedTriples = new ArrayList<Triple>((analogValuesToUpdate.size() + discreteValuesToUpdate.size())*3);
        MEASData.analogValuesToTriplesForUpdate(analogValuesToUpdate).forEach(updatedTriples::add);
        MEASData.disccreteValuesToTriplesForUpdate(discreteValuesToUpdate).forEach(updatedTriples::add);
        return  updatedTriples;
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        this.trialContext = new Context(param1_GraphImplementation);
        this.analogValues = MEASData.generateRandomAnalogValues(100000);
        this.discreteValues = MEASData.generateRandomDiscreteValues(25000);

        this.sut = Releases.current.createGraph(this.trialContext.getGraphClass());
        MEASData.addAnalogValuesToGraph(sut, analogValues);
        MEASData.addDiscreteValuesToGraph(sut, discreteValues);
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JMHDefaultOptions.getDefaults(this.getClass())
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }
}

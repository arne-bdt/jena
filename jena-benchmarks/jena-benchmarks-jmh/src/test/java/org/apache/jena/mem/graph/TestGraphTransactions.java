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
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.graph.helper.Context;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.mem.graph.helper.Releases;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.mem.DatasetGraphInMemory;
import org.apache.jena.sparql.core.mem.GraphMemTransactional;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

@State(Scope.Benchmark)
public class TestGraphTransactions {

    private final double UPDATE_RATIO = 0.03;
    private final double ITERATION_LIMIT = 30;
    private final boolean MEASURE_MEMORY = false;

    @Param({
            "C:/temp/CGMES_ConformityAssessmentScheme_r3-0-2/CGMES_ConformityAssessmentScheme_TestConfigurations_v3-0-3/v3.0/RealGrid/RealGrid-Merged/RealGrid_EQ.xml",
            "C:/temp/CGMES_ConformityAssessmentScheme_r3-0-2/CGMES_ConformityAssessmentScheme_TestConfigurations_v3-0-3/v3.0/RealGrid/RealGrid-Merged/RealGrid_SSH.xml",
//            "../testing/cheeses-0.1.ttl",
//            "../testing/pizza.owl.rdf",
    })
    public String param0_GraphUri;

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
    private Graph sutCurrent;
    private GraphMemTransactional graphMemTransactional;
    private DatasetGraphInMemory datasetGraphInMemory;
    private List<Triple> allTriplesCurrent;
    private List<Triple> triplesToDeleteFromSutCurrent;
    private Supplier<Long> graphTransactionsWithDeleteAndAdd;

    @Benchmark
    public long transactionsWithDeleteAndAdd() {
        return graphTransactionsWithDeleteAndAdd.get();
    }

    public long transactionsWithDeleteAndAddGraph() {

        int maxUpdateIndex = (int)(triplesToDeleteFromSutCurrent.size()* UPDATE_RATIO);
        long updates = 0;
        var iterations = 0;
        for(int iteration = 0; iteration < ITERATION_LIMIT; iteration++) {
            double memoryBefore;
            if(MEASURE_MEMORY) {
                memoryBefore = runGcAndGetUsedMemoryInMB();
            }
            for (int i = 0; i < maxUpdateIndex; i++) {
                sutCurrent.delete(triplesToDeleteFromSutCurrent.get(i));
            }
            for (int i = 0; i < maxUpdateIndex; i++) {
                sutCurrent.add(triplesToDeleteFromSutCurrent.get(i));
            }
            updates += maxUpdateIndex;
            if (MEASURE_MEMORY) {
                var memoryAfter = runGcAndGetUsedMemoryInMB();
                System.out.printf("Iteration %d - Graph - Memory before: %5.3f MB, Memory after: %5.3f MB, Difference: %5.3f MB%n",
                        iteration,
                        memoryBefore,
                        memoryAfter,
                        (memoryAfter - memoryBefore));
            }
            java.util.Collections.shuffle(this.triplesToDeleteFromSutCurrent, new Random(iteration));
            iterations++;
        }
        // print transactions and updates for information
        System.out.println("Graph - Iterations: " + iterations + ", Updates: " + updates);
        return updates;
    }

    public long transactionsWithDeleteAndAddGraphMemTransactional() {
        int maxUpdateIndex = (int)(triplesToDeleteFromSutCurrent.size()*UPDATE_RATIO);
        var transactions = 0;
        long changes = 0;
        for(int iteration = 0; iteration < ITERATION_LIMIT; iteration++) {
            double memoryBefore;
            if(MEASURE_MEMORY) {
                memoryBefore = runGcAndGetUsedMemoryInMB();
            }
            transactions++;
            graphMemTransactional.begin(TxnType.WRITE);
            for( int i = 0; i < maxUpdateIndex; i++ ) {
                graphMemTransactional.delete(triplesToDeleteFromSutCurrent.get(i));
            }
            if(iteration % 2 ==0) {
                graphMemTransactional.commit();
                graphMemTransactional.end();
                graphMemTransactional.begin(TxnType.WRITE);
                transactions++;
            }
            for( int i = 0; i < maxUpdateIndex; i++ ) {
                graphMemTransactional.add(triplesToDeleteFromSutCurrent.get(i));
            }
            if(false && iteration % 10 == 0) {
                graphMemTransactional.abort();
            } else {
                changes += maxUpdateIndex;
                graphMemTransactional.commit();
                graphMemTransactional.end();
            }
            if (MEASURE_MEMORY) {
                var memoryAfter = runGcAndGetUsedMemoryInMB();
                System.out.printf("Iteration %d - GraphMemTransactional - Memory before: %5.3f MB, Memory after: %5.3f MB, Difference: %5.3f MB%n",
                        iteration,
                        memoryBefore,
                        memoryAfter,
                        (memoryAfter - memoryBefore));
            }
            java.util.Collections.shuffle(this.triplesToDeleteFromSutCurrent, new Random(iteration));
        }
        // print transactions and updates for information
        System.out.println("GraphMemTransactional - Transactions: " + transactions + ", Updates: " + changes);
        return changes;
    }

    public long transactionsWithDeleteAndDatasetGraphInMemory() {
        int maxUpdateIndex = (int)(triplesToDeleteFromSutCurrent.size()*UPDATE_RATIO);
        var transactions = 0;
        long updates = 0;
        for(int iteration = 0; iteration < ITERATION_LIMIT; iteration++) {
            double memoryBefore;
            if(MEASURE_MEMORY) {
                memoryBefore = runGcAndGetUsedMemoryInMB();
            }
            transactions++;
            datasetGraphInMemory.begin(TxnType.WRITE);

            for( int i = 0; i < maxUpdateIndex; i++ ) {
                datasetGraphInMemory.delete(Quad.create(Quad.defaultGraphIRI, triplesToDeleteFromSutCurrent.get(i)));
            }
            if(iteration % 2 ==0) {
                datasetGraphInMemory.commit();
                datasetGraphInMemory.end();
                datasetGraphInMemory.begin(TxnType.WRITE);
                transactions++;
            }
            for( int i = 0; i < maxUpdateIndex; i++ ) {
                datasetGraphInMemory.add(Quad.create(Quad.defaultGraphIRI, triplesToDeleteFromSutCurrent.get(i)));
            }
            if(false && iteration % 10 == 0) {
                datasetGraphInMemory.abort();
            } else {
                updates+= maxUpdateIndex;
                datasetGraphInMemory.commit();
                datasetGraphInMemory.end();
            }
            if (MEASURE_MEMORY) {
                var memoryAfter = runGcAndGetUsedMemoryInMB();
                System.out.printf("Iteration %d - DatasetGraphInMemory - Memory before: %5.3f MB, Memory after: %5.3f MB, Difference: %5.3f MB%n",
                        iteration,
                        memoryBefore,
                        memoryAfter,
                        (memoryAfter - memoryBefore));
            }
            java.util.Collections.shuffle(this.triplesToDeleteFromSutCurrent, new Random(iteration));
        }
        // print transactions and updates for information
        System.out.println("DatasetGraphInMemory - Transactions: " + transactions + ", Updates: " + updates);
        return updates;
    }

    @Test
    public void testTransactions() {
        this.trialContext = new Context("GraphMemTransactional (current)");
        this.allTriplesCurrent = Releases.current.readTriples("C:/temp/CGMES_ConformityAssessmentScheme_r3-0-2/CGMES_ConformityAssessmentScheme_TestConfigurations_v3-0-3/v3.0/RealGrid/RealGrid-Merged/RealGrid_EQ.xml");

        this.graphMemTransactional = new GraphMemTransactional();
        this.graphTransactionsWithDeleteAndAdd = this::transactionsWithDeleteAndAddGraphMemTransactional;
        this.graphMemTransactional.begin(TxnType.WRITE);
        this.allTriplesCurrent.forEach(this.graphMemTransactional::add);
        this.graphMemTransactional.commit();
        this.graphMemTransactional.end();

        /*cloning is important so that the triples are not reference equal */
        this.triplesToDeleteFromSutCurrent = Releases.current.cloneTriples(this.allTriplesCurrent);
                /* Shuffle is import because the order might play a role. We want to test the performance of the
                       contains method regardless of the order */
        java.util.Collections.shuffle(this.triplesToDeleteFromSutCurrent, new Random(4721));

        transactionsWithDeleteAndAddGraphMemTransactional();
    }

    /**
     * This method is used to get the memory consumption of the current JVM.
     *
     * @return the memory consumption in MB
     */
    private static double runGcAndGetUsedMemoryInMB() {
        System.gc();
        Runtime.getRuntime().gc();
        return BigDecimal.valueOf(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()).divide(BigDecimal.valueOf(1024L), 3, RoundingMode.HALF_UP).divide(BigDecimal.valueOf(1024L), 3, RoundingMode.HALF_UP).doubleValue();
    }

    @Setup(Level.Invocation)
    public void setupInvocation() {
        switch (this.trialContext.getJenaVersion()) {
            case CURRENT:
                switch (this.trialContext.getGraphClass()) {
                    case DatasetGraphInMemory:
                        this.datasetGraphInMemory = new DatasetGraphInMemory();
                        this.graphTransactionsWithDeleteAndAdd = this::transactionsWithDeleteAndDatasetGraphInMemory;
                        this.datasetGraphInMemory.begin(TxnType.WRITE);
                        this.allTriplesCurrent.forEach(t -> datasetGraphInMemory.add(Quad.create(Quad.defaultGraphIRI, t)));
                        this.datasetGraphInMemory.commit();
                        this.datasetGraphInMemory.end();
                        break;
                    case GraphMemTransactional:
                        this.graphMemTransactional = new GraphMemTransactional();
                        this.graphTransactionsWithDeleteAndAdd = this::transactionsWithDeleteAndAddGraphMemTransactional;
                        this.graphMemTransactional.begin(TxnType.WRITE);
                        this.allTriplesCurrent.forEach(this.graphMemTransactional::add);
                        this.graphMemTransactional.commit();
                        this.graphMemTransactional.end();
                        break;
                    default:
                        this.sutCurrent = Releases.current.createGraph(this.trialContext.getGraphClass());
                        this.graphTransactionsWithDeleteAndAdd = this::transactionsWithDeleteAndAddGraph;
                        this.allTriplesCurrent.forEach(this.sutCurrent::add);
                        break;
                }

                /*cloning is important so that the triples are not reference equal */
                this.triplesToDeleteFromSutCurrent = Releases.current.cloneTriples(this.allTriplesCurrent);
                /* Shuffle is import because the order might play a role. We want to test the performance of the
                       contains method regardless of the order */
                java.util.Collections.shuffle(this.triplesToDeleteFromSutCurrent, new Random(4721));
                break;

            default:
                throw new IllegalArgumentException("Unknown Jena version: " + this.trialContext.getJenaVersion());
        }
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        this.trialContext = new Context(param1_GraphImplementation);
        switch (this.trialContext.getJenaVersion()) {
            case CURRENT:
                this.allTriplesCurrent = Releases.current.readTriples(param0_GraphUri);
                break;
            default:
                throw new IllegalArgumentException("Unknown Jena version: " + this.trialContext.getJenaVersion());
        }
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JMHDefaultOptions.getDefaults(this.getClass())
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }
}

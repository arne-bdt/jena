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
import org.apache.jena.graph.Triple;
import org.apache.jena.jmh.JmhDefaultOptions;
import org.apache.jena.mem.graph.helper.Releases;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.sparql.core.mem.DatasetGraphInMemory;
import org.apache.jena.sparql.core.mem.GraphMemIndexedSetCowTxn;
import org.apache.jena.sparql.core.mem.txn.helper.TxnGraphContext;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

@State(Scope.Benchmark)
public class TestTxnGraphTransactions {

    private final double UPDATE_RATIO = 0.003;
    private final double ITERATION_LIMIT = 60;
    private final boolean MEASURE_MEMORY = false;

    @Param({
            "../testing/cheeses-0.1.ttl",
            "../testing/pizza.owl.rdf",
    })
    public String param0_GraphUri;

    @Param({
            TxnGraphContext.GMIS_TXN_EAGER,
            TxnGraphContext.GMIS_DEFAULT_IN_MEMORY,
            TxnGraphContext.GMIS_COW_TXN_EAGER_SEQ,
            TxnGraphContext.GMIS_COW_TXN_EAGER_PARALLEL,
    })

    public String param1_GraphImplementation;
    private Graph sutCurrent;
    private DatasetGraphInMemory datasetGraphInMemory;
    private Transactional transactional;
    private List<Triple> allTriplesCurrent;
    private List<Triple> triplesToDeleteFromSutCurrent;
    private Supplier<Long> randomGraphModifications;

    @Benchmark
    public long transactionsWithDeleteAndAdd() {
        return randomGraphModifications.get();
    }

    public long randomGraphModificationsNonTransactional() {

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

    public long randomGraphModificationsTransactional() {
        int maxUpdateIndex = (int)(triplesToDeleteFromSutCurrent.size()*UPDATE_RATIO);
        var transactions = 0;
        long changes = 0;
        for(int iteration = 0; iteration < ITERATION_LIMIT; iteration++) {
            double memoryBefore;
            if(MEASURE_MEMORY) {
                memoryBefore = runGcAndGetUsedMemoryInMB();
            }
            transactions++;
            transactional.begin(TxnType.WRITE);
            for( int i = 0; i < maxUpdateIndex; i++ ) {
                sutCurrent.delete(triplesToDeleteFromSutCurrent.get(i));
            }
            if(iteration % 2 ==0) {
                transactional.commit();
                transactional.end();
                transactional.begin(TxnType.WRITE);
                transactions++;
            }
            for( int i = 0; i < maxUpdateIndex; i++ ) {
                sutCurrent.add(triplesToDeleteFromSutCurrent.get(i));
            }
            if(false && iteration % 10 == 0) {
                transactional.abort();
            } else {
                changes += maxUpdateIndex;
                transactional.commit();
                transactional.end();
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
        System.out.println("GraphTxn - Transactions: " + transactions + ", Updates: " + changes);
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

    @Ignore
    @Test
    public void testTransactions() {
        this.allTriplesCurrent = Releases.current.readTriples("C:/temp/AMP_H26_S71_K_96_a593ce445264eed8150b78d8e240afa20fe1ad71.rdf");

        for( int i = 0; i < 5; i++ ) {
            var g = new GraphMemIndexedSetCowTxn(GraphMemIndexedSetCowTxn.ForkMode.PARALLEL);
            g.begin(TxnType.WRITE);
            this.allTriplesCurrent.forEach(g::add);
            g.commit();
            g.end();

            sutCurrent = g;
            transactional = g;


            /*cloning is important so that the triples are not reference equal */
            this.triplesToDeleteFromSutCurrent = Releases.current.cloneTriples(this.allTriplesCurrent);
                /* Shuffle is import because the order might play a role. We want to test the performance of the
                       contains method regardless of the order */
            java.util.Collections.shuffle(this.triplesToDeleteFromSutCurrent, new Random(4721));

            randomGraphModificationsTransactional();
        }

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
        final double memoryBefore;
        if(MEASURE_MEMORY) {
            memoryBefore = runGcAndGetUsedMemoryInMB();
        }
        if(TxnGraphContext.GMIS_DEFAULT_IN_MEMORY.equals(param1_GraphImplementation)) {
            datasetGraphInMemory = new DatasetGraphInMemory();
            datasetGraphInMemory.begin(TxnType.WRITE);
            allTriplesCurrent.forEach(t -> datasetGraphInMemory.add(Quad.create(Quad.defaultGraphIRI, t)));
            datasetGraphInMemory.commit();
            datasetGraphInMemory.end();
            this.randomGraphModifications = this::transactionsWithDeleteAndDatasetGraphInMemory;
        } else {
            sutCurrent = TxnGraphContext.createGraph(param1_GraphImplementation);
            if(sutCurrent instanceof Transactional) {
                transactional = (Transactional) sutCurrent;
                this.randomGraphModifications = this::randomGraphModificationsTransactional;
            } else {
                this.randomGraphModifications = this::randomGraphModificationsNonTransactional;
            }
            TxnGraphContext.writeTxn(sutCurrent, () -> allTriplesCurrent.forEach(sutCurrent::add));
        }
        if(MEASURE_MEMORY) {
            var memoryAfter = runGcAndGetUsedMemoryInMB();
            System.out.printf("Memory allocated: %5.3f MB%n",  (memoryAfter - memoryBefore));
        }

        /*cloning is important so that the triples are not reference equal */
        this.triplesToDeleteFromSutCurrent = Releases.current.cloneTriples(this.allTriplesCurrent);
                /* Shuffle is import because the order might play a role. We want to test the performance of the
                       contains method regardless of the order */
        java.util.Collections.shuffle(this.triplesToDeleteFromSutCurrent, new Random(4721));
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        allTriplesCurrent = Releases.current.readTriples(param0_GraphUri);
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JmhDefaultOptions.getDefaults(this.getClass())
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }
}

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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.LongSupplier;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.jmh.JmhDefaultOptions;
import org.apache.jena.mem.graph.helper.Releases;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.sparql.core.mem.DatasetGraphInMemory;
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
 * Mixed delete-then-add workload run as repeated write transactions, with
 * the triple list reshuffled between iterations so the strategy cannot
 * cache hot paths. Per benchmark invocation we run {@link #ITERATION_LIMIT}
 * iterations; each iteration touches roughly {@code triples.size() *
 * UPDATE_RATIO} triples.
 * <p>
 * Three workload variants are dispatched based on the graph implementation
 * chosen by {@code param1_GraphImplementation}:
 * <ul>
 *   <li>{@link DatasetGraphInMemory} via its dataset-level transaction API
 *       (delete/add of {@link Quad}s in the default graph);</li>
 *   <li>A {@link Transactional} graph (the deep-copy and copy-on-write
 *       variants) via {@code begin/commit/end}.</li>
 * </ul>
 * Within an iteration the work is intentionally split into two transactions
 * on every other iteration, so the benchmark mixes "single big txn" and
 * "two smaller txns" cases.
 * <p>
 * Setting {@link #MEASURE_MEMORY} to {@code true} adds a {@code System.gc()}
 * before and after each iteration and prints the delta. This perturbs timings
 * substantially and is intended as an opt-in development aid, not for the
 * recorded benchmark results.
 */
@State(Scope.Benchmark)
public class TestTxnGraphTransactions {

    /** Fraction of the loaded triples deleted and re-added per iteration. */
    private static final double UPDATE_RATIO = 0.003;
    /** Iterations per benchmark invocation. */
    private static final int ITERATION_LIMIT = 60;
    /** Seed used by the per-iteration shuffle; constant so runs are comparable. */
    private static final int SHUFFLE_SEED = 4721;
    /** When true, run System.gc() and print used-memory deltas around each iteration. */
    private static final boolean MEASURE_MEMORY = false;

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

    private List<Triple> allTriples;
    private List<Triple> triplesToUpdate;

    private Graph graph;
    private Transactional transactional;
    private DatasetGraphInMemory datasetGraphInMemory;

    /** Dispatcher set up in {@link #setupInvocation} based on the graph variant. */
    private LongSupplier workload;

    @Benchmark
    public long transactionsWithDeleteAndAdd() {
        return workload.getAsLong();
    }

    /** Transactional variant: same workload wrapped in begin/commit/end. */
    private long runTransactionalWorkload() {
        final int batchSize = (int) (triplesToUpdate.size() * UPDATE_RATIO);
        long changes = 0;
        for (int iteration = 0; iteration < ITERATION_LIMIT; iteration++) {
            final double memoryBefore = MEASURE_MEMORY ? runGcAndGetUsedMemoryInMB() : 0;
            transactional.begin(TxnType.WRITE);
            for (int i = 0; i < batchSize; i++) {
                graph.delete(triplesToUpdate.get(i));
            }
            // Split the iteration across two write transactions half the time:
            // forces the fork/publish path to run mid-iteration.
            if (iteration % 2 == 0) {
                transactional.commit();
                transactional.end();
                transactional.begin(TxnType.WRITE);
            }
            for (int i = 0; i < batchSize; i++) {
                graph.add(triplesToUpdate.get(i));
            }
            changes += batchSize;
            transactional.commit();
            transactional.end();
            if (MEASURE_MEMORY) {
                printMemoryDelta("GraphMemTransactional", iteration, memoryBefore);
            }
            Collections.shuffle(triplesToUpdate, new Random(iteration));
        }
        return changes;
    }

    /** {@link DatasetGraphInMemory} variant: same workload at dataset level. */
    private long runDatasetGraphWorkload() {
        final int batchSize = (int) (triplesToUpdate.size() * UPDATE_RATIO);
        long changes = 0;
        for (int iteration = 0; iteration < ITERATION_LIMIT; iteration++) {
            final double memoryBefore = MEASURE_MEMORY ? runGcAndGetUsedMemoryInMB() : 0;
            datasetGraphInMemory.begin(TxnType.WRITE);
            for (int i = 0; i < batchSize; i++) {
                datasetGraphInMemory.delete(Quad.create(Quad.defaultGraphIRI, triplesToUpdate.get(i)));
            }
            if (iteration % 2 == 0) {
                datasetGraphInMemory.commit();
                datasetGraphInMemory.end();
                datasetGraphInMemory.begin(TxnType.WRITE);
            }
            for (int i = 0; i < batchSize; i++) {
                datasetGraphInMemory.add(Quad.create(Quad.defaultGraphIRI, triplesToUpdate.get(i)));
            }
            changes += batchSize;
            datasetGraphInMemory.commit();
            datasetGraphInMemory.end();
            if (MEASURE_MEMORY) {
                printMemoryDelta("DatasetGraphInMemory", iteration, memoryBefore);
            }
            Collections.shuffle(triplesToUpdate, new Random(iteration));
        }
        return changes;
    }

    @Setup(Level.Invocation)
    public void setupInvocation() {
        final double memoryBefore = MEASURE_MEMORY ? runGcAndGetUsedMemoryInMB() : 0;
        if (TxnGraphContext.GMIS_DEFAULT_IN_MEMORY.equals(param1_GraphImplementation)) {
            datasetGraphInMemory = new DatasetGraphInMemory();
            datasetGraphInMemory.begin(TxnType.WRITE);
            allTriples.forEach(t -> datasetGraphInMemory.add(Quad.create(Quad.defaultGraphIRI, t)));
            datasetGraphInMemory.commit();
            datasetGraphInMemory.end();
            workload = this::runDatasetGraphWorkload;
        } else {
            graph = TxnGraphContext.createGraph(param1_GraphImplementation);
            // All configured graph variants are Transactional.
            transactional = (Transactional) graph;
            workload = this::runTransactionalWorkload;
            TxnGraphContext.writeTxn(graph, () -> allTriples.forEach(graph::add));
        }
        if (MEASURE_MEMORY) {
            final double memoryAfter = runGcAndGetUsedMemoryInMB();
            System.out.printf("Memory allocated: %5.3f MB%n", memoryAfter - memoryBefore);
        }

        // Cloning is important so triples are not reference-equal to the loaded set;
        // shuffling so the strategy cannot rely on input order.
        triplesToUpdate = Releases.current.cloneTriples(allTriples);
        Collections.shuffle(triplesToUpdate, new Random(SHUFFLE_SEED));
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        allTriples = Releases.current.readTriples(param0_GraphUri);
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JmhDefaultOptions.getDefaults(this.getClass()).build();
        Assert.assertNotNull(new Runner(opt).run());
    }

    /** Used-memory in MB after a synchronous GC pass. Best-effort — relies on System.gc(). */
    private static double runGcAndGetUsedMemoryInMB() {
        System.gc();
        Runtime.getRuntime().gc();
        final long usedBytes = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        return BigDecimal.valueOf(usedBytes)
                .divide(BigDecimal.valueOf(1024L), 3, RoundingMode.HALF_UP)
                .divide(BigDecimal.valueOf(1024L), 3, RoundingMode.HALF_UP)
                .doubleValue();
    }

    private static void printMemoryDelta(String label, int iteration, double memoryBefore) {
        final double memoryAfter = runGcAndGetUsedMemoryInMB();
        System.out.printf("Iteration %d - %s - Memory before: %5.3f MB, Memory after: %5.3f MB, Difference: %5.3f MB%n",
                iteration, label, memoryBefore, memoryAfter, memoryAfter - memoryBefore);
    }
}

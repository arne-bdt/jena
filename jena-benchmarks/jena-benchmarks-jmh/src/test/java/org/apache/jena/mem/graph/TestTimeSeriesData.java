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

import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.graph.Graph;
import org.apache.jena.mem.GraphMem;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.mem.graph.helper.Serialization;
import org.apache.jena.mem2.GraphMem2Fast;
import org.apache.jena.mem2.GraphMem2Legacy;
import org.apache.jena.mem2.GraphMem2Roaring;
import org.apache.jena.riot.RDFFormat;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.math.BigDecimal;


@State(Scope.Benchmark)
public class TestTimeSeriesData {

    @Param({
            //"GraphMem",
            "GraphMem2Fast",
            //"GraphMem2Legacy",
//            "GraphMem2Roaring"
    })
    public String p0_GraphImplementation;
    @Param({
//             "100000",
//             "250000",
             "500000",
//             "750000",
//            "1000000",
//            "2500000"
    })
    public String p1_NumberOfTimeSeries;

    private int numberOfTimeSeries;

    @Ignore
    @Test
    public void test_generateTimeSeriesDataGraphAndMeasureMemoryConsumption() {
        var memoryBefore = runGcAndGetUsedMemoryInMB();
        var stopwatch = StopWatch.createStarted();
        final var g = new GraphMem2Fast();
        TimeSeriesData.fillGraphWithTimeSeries(g, 4000000);
        stopwatch.stop();
        var memoryAfter = runGcAndGetUsedMemoryInMB();
        //printf: Generating graph with X timeseries took Y seconds, generated Z triples and increased memory consumption by X,XXX.XX MB
        System.out.printf("Generating graph with %d timeseries took %s, generated %d triples and increased memory consumption by %,.2f MB%n",
                numberOfTimeSeries,
                stopwatch,
                g.size(),
                memoryAfter - memoryBefore);
    }

    @Benchmark
    public Graph generateTimeSeriesDataGraphAndMeasureMemoryConsumption() {
        final var memoryBefore = runGcAndGetUsedMemoryInMB();
        final var stopwatch = StopWatch.createStarted();
        final var g = createGraph();
        TimeSeriesData.fillGraphWithTimeSeries(g, numberOfTimeSeries);
        stopwatch.stop();
        final var memoryAfter = runGcAndGetUsedMemoryInMB();
        //printf: Generating graph with X timeseries took Y seconds, generated Z triples and increased memory consumption by X,XXX.XX MB
        System.out.printf("Generating graph with %d timeseries took %s, generated %d triples and increased memory consumption by %,.2f MB%n",
                numberOfTimeSeries,
                stopwatch,
                g.size(),
                memoryAfter - memoryBefore);
        stopwatch.reset();
        stopwatch.start();
        final var serializedGraph = Serialization.serialize(g, RDFFormat.RDF_THRIFT, Serialization.LZ4_FASTEST);
        stopwatch.stop();
        //printf: Serializing graph with %d timeseries took %s, compressed size: %,.2f MB%n, uncompressed size: %,.2f MB%n
        System.out.printf("Serializing graph with %d timeseries took %s, compressed size: %,.2f MB, uncompressed size: %,.2f MB%n",
                numberOfTimeSeries,
                stopwatch,
                serializedGraph.bytes().length / 1024.0 / 1024.0,
                serializedGraph.uncompressedSize() / 1024.0 / 1024.0);
        stopwatch.reset();
        stopwatch.start();
        final var g1 = Serialization.deserialize(serializedGraph, false);
        stopwatch.stop();
        System.out.printf("Deserializing graph with %d timeseries took %s%n",
                numberOfTimeSeries,
                stopwatch);
        return g;
    }

    public Graph createGraph() {
        switch (p0_GraphImplementation) {
            case "GraphMem":
                return new GraphMem();
            case "GraphMem2Fast":
                return new GraphMem2Fast();
            case "GraphMem2Legacy":
                return new GraphMem2Legacy();
            case "GraphMem2Roaring":
                return new GraphMem2Roaring();
            default:
                throw new IllegalArgumentException("Unknown graph implementation: " + p0_GraphImplementation);
        }
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        this.numberOfTimeSeries = Integer.parseInt(p1_NumberOfTimeSeries);
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JMHDefaultOptions.getDefaults(this.getClass())
                .warmupIterations(5)
                .measurementIterations(10)
                .jvmArgs("-Xmx16G")
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }

    /**
     * This method is used to get the memory consumption of the current JVM.
     *
     * @return the memory consumption in MB
     */
    private static double runGcAndGetUsedMemoryInMB() {
        System.runFinalization();
        System.gc();
        Runtime.getRuntime().runFinalization();
        Runtime.getRuntime().gc();
        return BigDecimal.valueOf(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()).divide(BigDecimal.valueOf(1024L)).divide(BigDecimal.valueOf(1024L)).doubleValue();
    }

}

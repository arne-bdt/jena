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

package org.apache.jena.mem.jmh.bigBDSM;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.GraphMem;
import org.apache.jena.mem.GraphMemWithArrayListOnly;
import org.apache.jena.mem2.GraphMem2;
import org.apache.jena.mem2.GraphMem2NoEqualsOkOpt;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@State(Scope.Benchmark)
public class TestGraphMemBigBDSMContainsFindAndStreamAll {

    @Param({"./../jena-examples/src/main/resources/data/BSBM_50000.ttl.gz"})
    public String param0_GraphUri;

    @Param({"GraphMem", "GraphMem2", "GraphMem2NoEqualsOkOpt"})
    public String param1_GraphImplementation;

    private Graph createGraph() {
        switch (this.param1_GraphImplementation) {
            case "GraphMem":
                return new GraphMem();

            case "GraphMem2":
                return new GraphMem2();

            case "GraphMem2NoEqualsOkOpt":
                return new GraphMem2NoEqualsOkOpt();

            default:
                throw new IllegalArgumentException();
        }
    }

    private List<Triple> triples;
    private List<Triple> samples;
    private Graph sut;

    @Setup(Level.Invocation)
    public void setupInvokation() throws Exception {
        // Invocation level: to be executed for each benchmark method execution.
    }

    @Setup(Level.Iteration)
    public void setupIteration() throws Exception {
        // Iteration level: to be executed before/after each iteration of the benchmark.
    }

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        // Trial level: to be executed before/after each run of the benchmark.
        var loadingGraph = new GraphMemWithArrayListOnly();
        RDFDataMgr.read(loadingGraph, param0_GraphUri);
        this.triples = loadingGraph.triples;
        this.sut = createGraph();
        this.triples.forEach(t -> sut.add(Triple.create(t.getSubject(), t.getPredicate(), t.getObject())));
    }


    @Benchmark
    public void graphContains() {
        triples.forEach(t -> Assert.assertTrue(sut.contains(t)));
    }

    @Benchmark
    public void graphFind() {
        var found = new ArrayList<Triple>(sut.size());
        var it = sut.find();
        while(it.hasNext()) {
            found.add(it.next());
        }
        it.close();
        assertEquals(sut.size(), found.size());
    }

    @Benchmark
    public void graphStream() {
        var found = sut.stream().collect(Collectors.toList());
        assertEquals(sut.size(), found.size());
    }

    @Benchmark
    public void graphStreamParallel() {
        var found = sut.stream().parallel().collect(Collectors.toList());
        assertEquals(sut.size(), found.size());
    }

    @Test
    public void benchmark() throws Exception {
        var opt = new OptionsBuilder()
                // Specify which benchmarks to run.
                // You can be more specific if you'd like to run only one benchmark per test.
                .include(this.getClass().getName())
                // Set the following options as needed
                .mode (Mode.AverageTime)
                .timeUnit(TimeUnit.MILLISECONDS)
                .warmupTime(TimeValue.NONE)
                .warmupIterations(3)
                .measurementTime(TimeValue.NONE)
                .measurementIterations(15)
                .threads(1)
                .forks(1)
                .shouldFailOnError(true)
                .shouldDoGC(true)
                //.jvmArgs("-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintInlining")
                .jvmArgs("-Xmx12G")
                .jvmArgsAppend("-Djava.util.concurrent.ForkJoinPool.common.parallelism=8")
                //.addProfiler(WinPerfAsmProfiler.class)
                .resultFormat(ResultFormatType.JSON)
                .result(this.getClass().getSimpleName() + "_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")) + ".json")
                .build();

        new Runner(opt).run();
    }
}

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

package org.apache.jena.mem.jmh.second;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.GraphMem;
import org.apache.jena.mem.TypedTripleReader;
import org.apache.jena.mem2.*;
import org.junit.Ignore;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

@Ignore
@State(Scope.Benchmark)
public class TestGraphMemBigBDSMFindAnything {

    @Param({"./../jena-examples/src/main/resources/data/BSBM_50000.ttl.gz"})
    public String param0_GraphUri;

    @Param({
            "GraphMem",
            "GraphMem2",
            "GraphMem3",
            "GraphMem4Fast",
//            "GraphMem2Fast",
//            "GraphMem3Fast"
    })
    public String param1_GraphImplementation;

    private Graph createGraph() {
        switch (this.param1_GraphImplementation) {
            case "GraphMem":
                return new GraphMem();

            case "GraphMem2":
                return new GraphMem2();

            case "GraphMem2EqualsOk":
                return new GraphMem2EqualsOk();

            case "GraphMem2Fast":
                return new GraphMem2Fast();

            case "GraphMem3":
                return new GraphMem3();

            case "GraphMem3Fast":
                return new GraphMem3Fast();

            case "GraphMem4Fast":
                return new GraphMem4Fast();

            default:
                throw new IllegalArgumentException();
        }
    }

    private List<Triple> triples;
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
        this.sut = createGraph();
        this.triples = TypedTripleReader.read(param0_GraphUri);
        triples.forEach(t -> sut.add(Triple.create(t.getSubject(), t.getPredicate(), t.getObject())));
    }

    @Benchmark
    public void graphFindBySamples_Subject_ANY_ANY() {
        for (Triple sample : triples) {
            var it = sut.find(sample.getSubject(), Node.ANY, Node.ANY);
            assertTrue(it.hasNext());
        }
    }

    @Benchmark
    public void graphFindBySamples_ANY_Predicate_ANY() {
        for (Triple sample : triples) {
            var it = sut.find(Node.ANY, sample.getPredicate(), Node.ANY);
            assertTrue(it.hasNext());
        }
    }

    @Benchmark
    public void graphFindBySamples_ANY_ANY_Object() {
        for (Triple sample : triples) {
            var it = sut.find(Node.ANY, Node.ANY, sample.getObject());
            assertTrue(it.hasNext());
        }
    }

    @Benchmark
    public void graphFindBySamples_Subject_Predicate_ANY() {
        for (Triple sample : triples) {
            var it = sut.find(sample.getSubject(), sample.getPredicate(), Node.ANY);
            assertTrue(it.hasNext());
        }
    }

    @Benchmark
    public void graphFindBySamples_Subject_ANY_Object() {
        for (Triple sample : triples) {
            var it = sut.find(sample.getSubject(), Node.ANY, sample.getObject());
            assertTrue(it.hasNext());
        }
    }

    @Benchmark
    public void graphFindBySamples_ANY_Predicate_Object() {
        for (Triple sample : triples) {
            var it = sut.find(Node.ANY, sample.getPredicate(), sample.getObject());
            assertTrue(it.hasNext());
        }
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
                .warmupIterations(5)
                .measurementTime(TimeValue.NONE)
                .measurementIterations(15)
                .threads(1)
                .forks(1)
                .shouldFailOnError(true)
                .shouldDoGC(true)
                //.jvmArgs("-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintInlining")
                .jvmArgs("-Xmx12G")
                //.addProfiler(WinPerfAsmProfiler.class)
                .resultFormat(ResultFormatType.JSON)
                .result(this.getClass().getSimpleName() + "_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")) + ".json")
                .build();

        new Runner(opt).run();
    }
}

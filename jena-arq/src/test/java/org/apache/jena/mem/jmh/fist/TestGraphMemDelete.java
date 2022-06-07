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

package org.apache.jena.mem.jmh.fist;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.GraphMem;
import org.apache.jena.mem.GraphMemWithArrayListOnly;
import org.apache.jena.mem.TypedTripleReader;
import org.apache.jena.mem2.*;
import org.apache.jena.riot.RDFDataMgr;
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

@State(Scope.Benchmark)
public class TestGraphMemDelete {

    public String getParam0_GraphUri() {
        return param0_GraphUri;
    }

    @Param({
            "./../jena-examples/src/main/resources/data/cheeses-0.1.ttl",
            "./../jena-examples/src/main/resources/data/pizza.owl.rdf",
            "C:/temp/res_test/xxx_CGMES_EQ.xml",
            "C:/temp/res_test/xxx_CGMES_SSH.xml",
            "C:/temp/res_test/xxx_CGMES_TP.xml",
            "./../jena-examples/src/main/resources/data/BSBM_2500.ttl",
            "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml",
            "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml",
            "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml",
            "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml",
    })
    public String param0_GraphUri;

    @Param({
            "GraphMem",
            "GraphMem2",
            "GraphMem3",
            "GraphMem4",
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

            case "GraphMem4":
                return new GraphMem4();

            default:
                throw new IllegalArgumentException();
        }
    }

    private List<Triple> triplesToAdd;
    private List<Triple> triplesToDelete;
    private Graph sut;

    @Setup(Level.Invocation)
    public void setupInvokation() {
        this.sut = createGraph();
        triplesToAdd.forEach(sut::add);
        // Invocation level: to be executed for each benchmark method execution.
    }

    @Setup(Level.Iteration)
    public void setupIteration() {
        // Iteration level: to be executed before/after each iteration of the benchmark.
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        // Trial level: to be executed before/after each run of the benchmark.
        this.triplesToAdd = TypedTripleReader.read(param0_GraphUri);
        this.triplesToDelete = new ArrayList<>(triplesToAdd.size());
        this.triplesToAdd.forEach(t -> triplesToDelete.add(Triple.create(t.getSubject(), t.getPredicate(), t.getObject())));
    }


    @Benchmark
    public void graphDelete() {
        triplesToDelete.forEach(t -> this.sut.delete(t));
        Assert.assertTrue(this.sut.isEmpty());
    }

    @Test
    public void benchmark() throws Exception {
        var opt = new OptionsBuilder()
                // Specify which benchmarks to run.
                // You can be more specific if you'd like to run only one benchmark per test.
                .include(this.getClass().getName())
                // Set the following options as needed
                .mode (Mode.AverageTime)
                .timeUnit(TimeUnit.SECONDS)
                .warmupTime(TimeValue.NONE)
                .warmupIterations(3)
                .measurementTime(TimeValue.NONE)
                .measurementIterations(10)
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

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

package org.apache.jena.mem.jmh;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.GraphMem;
import org.apache.jena.mem.TripleReaderReadingCGMES_2_4_15_WithTypedLiterals;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public abstract class AbstractJmhTestGraphBase {

    public String getParam0_GraphUri() {
        return param0_GraphUri;
    }

    @Param({
            "./testing/cheeses-0.1.ttl",
            "./testing/pizza.owl.rdf",
            //"C:/temp/res_test/xxx_CGMES_EQ.xml",
            //"C:/temp/res_test/xxx_CGMES_SSH.xml",
            //"C:/temp/res_test/xxx_CGMES_TP.xml",
            //"C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml",
            //"C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml",
            //"C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml",
            //"C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml",
            //"./testing/BSBM/bsbm-1m.nt.gz",
            //"./testing/BSBM/bsbm-5m.nt.gz",
            //"./testing/BSBM/bsbm-25m.nt.gz"
    })
    public String param0_GraphUri;

    @Param({
            "GraphMem",
    })
    public String param1_GraphImplementation;

    /**
     * Creates a new graph instance of the type specified by {@link #param1_GraphImplementation}.
     * @return
     */
    protected Graph createGraph() {
        switch (this.param1_GraphImplementation) {
            case "GraphMem":
                return new GraphMem();
            default:
                throw new IllegalArgumentException();
        }
    }

    /**
     * During the setup of each trial, this list ist filled with the triples from the file specified by {@link #param0_GraphUri}.
     */
    protected List<Triple> triples;

    protected void fillTriplesList() throws Exception {
        // Trial level: to be executed before/after each run of the benchmark.
        this.triples = TripleReaderReadingCGMES_2_4_15_WithTypedLiterals.read(getParam0_GraphUri());
    }

    /**
     * This method is used to set up the JMH options.
     * The ChainedOptionsBuilder can be used to set or override the default options.
     * @param c the class to be benchmarked
     * @return the options builder
     */
    protected ChainedOptionsBuilder setupOptionsBuilderWithDefaultOptions(Class<?> c) {
        return new OptionsBuilder()
                // Specify which benchmarks to run.
                // You can be more specific if you'd like to run only one benchmark per test.
                .include(c.getName())
                // Set the following options as needed
                .mode (Mode.AverageTime)
                .timeUnit(TimeUnit.SECONDS)
                .warmupTime(TimeValue.NONE)
                .warmupIterations(3)
                .measurementIterations(10)
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
                .result(c.getSimpleName() + "_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")) + ".json");
    }
}

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
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.graph.helper.Context;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.mem.graph.helper.Releases;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.core.Transactional;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.math.BigDecimal;
import java.util.List;

@State(Scope.Benchmark)
public class TestGraphMemoryConsumption {

    @Param({
            "C:/temp/AMP_Export.rdf",
//            "../testing/cheeses-0.1.ttl",
//            "../testing/pizza.owl.rdf",
//            "C:/temp/res_test/xxx_CGMES_EQ.xml",
//            "C:/temp/res_test/xxx_CGMES_SSH.xml",
//            "C:/temp/res_test/xxx_CGMES_TP.xml",
//            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml",
//            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml",
//            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml",
//            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml",
//            "../testing/BSBM/bsbm-1m.nt.gz",
//            "../testing/BSBM/bsbm-5m.nt.gz",
//            "../testing/BSBM/bsbm-25m.nt.gz",
    })
    public String param0_GraphUri;

    @Param({
//            "GraphMem (current)",
//            "GraphWrapperTransactional (current)",
//            "GraphTxn (current)",
            "GraphMem2Fast (current)",
            "GraphMem2Legacy (current)",
            "GraphMem2Roaring (current)",
//            "GraphMem (Jena 5.3.0)",
    })
    public String param1_GraphImplementation;
    java.util.function.Supplier<Object> graphFill;
    private Context trialContext;
    private List<Triple> allTriplesCurrent;
    private List<org.apache.shadedJena530.graph.Triple> allTriples530;

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

    @Benchmark
    public Object graphFill() {
        return graphFill.get();
    }

    private Object graphFillCurrent() {
        var memoryBefore = runGcAndGetUsedMemoryInMB();
        var stopwatch = StopWatch.createStarted();
        Graph sut = null;
        sut = Releases.current.createGraph(trialContext.getGraphClass());
        if (sut instanceof Transactional transactional) {
            transactional.begin(ReadWrite.WRITE);
            allTriplesCurrent.forEach(sut::add);
            transactional.commit();
        } else {
            allTriplesCurrent.forEach(sut::add);
        }
        stopwatch.stop();
        var memoryAfter = runGcAndGetUsedMemoryInMB();
        if (sut instanceof Transactional transactional) {
            transactional.begin(ReadWrite.READ);
        }
        System.out.printf("graphs: %d time to fill graphs: %s additional memory: %5.3f MB%n",
                sut.size(),
                stopwatch.formatTime(),
                (memoryAfter - memoryBefore));
        if (sut instanceof Transactional transactional) {
            transactional.end();
        }
        return sut;
    }

    private Object graphFill530() {
        var memoryBefore = runGcAndGetUsedMemoryInMB();
        var stopwatch = StopWatch.createStarted();
        var sut = Releases.v530.createGraph(trialContext.getGraphClass());
        allTriples530.forEach(sut::add);
        stopwatch.stop();
        var memoryAfter = runGcAndGetUsedMemoryInMB();
        System.out.printf("graphs: %d time to fill graphs: %s additional memory: %5.3f MB%n",
                sut.size(),
                stopwatch.formatTime(),
                (memoryAfter - memoryBefore));
        return sut;
    }

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        this.trialContext = new Context(param1_GraphImplementation);
        switch (this.trialContext.getJenaVersion()) {
            case CURRENT:
                this.allTriplesCurrent = Releases.current.readTriples(param0_GraphUri);
                this.graphFill = this::graphFillCurrent;
                break;
            case JENA_5_3_0:
                this.allTriples530 = Releases.v530.readTriples(param0_GraphUri);
                this.graphFill = this::graphFill530;
                break;
            default:
                throw new IllegalArgumentException("Unknown Jena version: " + this.trialContext.getJenaVersion());
        }
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JMHDefaultOptions.getDefaults(this.getClass())
                .warmupIterations(3)
                .measurementIterations(3)
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }
}
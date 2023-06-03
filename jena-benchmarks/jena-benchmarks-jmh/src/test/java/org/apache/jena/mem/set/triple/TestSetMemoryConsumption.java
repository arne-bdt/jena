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

package org.apache.jena.mem.set.triple;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.graph.helper.Context;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.mem.graph.helper.Releases;
import org.apache.jena.mem2.collection.FastTripleHashSet;
import org.apache.jena.mem2.collection.FastTripleSet;
import org.apache.jena.mem2.collection.TripleSet;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;

@State(Scope.Benchmark)
public class TestSetMemoryConsumption {

    @Param({
            "../testing/cheeses-0.1.ttl",
            "../testing/pizza.owl.rdf",
            "C:/temp/res_test/xxx_CGMES_EQ.xml",
            "C:/temp/res_test/xxx_CGMES_SSH.xml",
            "C:/temp/res_test/xxx_CGMES_TP.xml",
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
            "HashSet",
            "TripleSet",
            "FastTripleSet",
            "FastTripleHashSet"
    })
    public String param1_SetImplementation;

    private List<Triple> triples;

    java.util.function.Supplier<Object> fillSet;

    @Benchmark
    public Object fillSet() {
        var memoryBefore = runGcAndGetUsedMemoryInMB();
        var stopwatch = StopWatch.createStarted();
        var sut = fillSet.get();
        stopwatch.stop();
        var memoryAfter = runGcAndGetUsedMemoryInMB();
        System.out.println(String.format("graphs: %d time to fill graphs: %s additional memory: %5.3f MB",
                triples.size(),
                stopwatch.formatTime(),
                (memoryAfter - memoryBefore)));
        return sut;
    }

    private Object fillHashSet() {
        var sut = new HashSet<Triple>();
        triples.forEach(sut::add);
        Assert.assertEquals(triples.size(), sut.size());
        return sut;
    }
    private Object fillTripleSet() {
        var sut = new TripleSet();
        triples.forEach(sut::addKey);
        Assert.assertEquals(triples.size(), sut.size());
        return sut;
    }

    private Object fillFastTripleSet() {
        var sut = new FastTripleSet();
        triples.forEach(sut::addKey);
        Assert.assertEquals(triples.size(), sut.size());
        return sut;
    }

    private Object fillFastTripleHashSet() {
        var sut = new FastTripleHashSet();
        triples.forEach(sut::add);
        Assert.assertEquals(triples.size(), sut.size());
        return sut;
    }

    /**
     * This method is used to get the memory consumption of the current JVM.
     * @return the memory consumption in MB
     */
    private static double runGcAndGetUsedMemoryInMB() {
        System.runFinalization();
        System.gc();
        Runtime.getRuntime().runFinalization();
        Runtime.getRuntime().gc();
        return BigDecimal.valueOf(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory()).divide(BigDecimal.valueOf(1024l)).divide(BigDecimal.valueOf(1024l)).doubleValue();
    }

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        triples = Releases.current.readTriples(param0_GraphUri);
        switch (param1_SetImplementation) {
            case "HashSet":
                this.fillSet = this::fillHashSet;
                break;
            case "TripleSet":
                this.fillSet = this::fillTripleSet;
                break;
            case "FastTripleSet":
                this.fillSet = this::fillFastTripleSet;
                break;
            case "FastTripleHashSet":
                this.fillSet = this::fillFastTripleHashSet;
                break;
            default:
                throw new IllegalArgumentException("Unknown set implementation: " + param1_SetImplementation);
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
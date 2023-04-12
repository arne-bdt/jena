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

import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.graph.Graph;
import org.apache.jena.mem.TripleReaderReadingCGMES_2_4_15_WithTypedLiterals;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;

import java.math.BigDecimal;


@State(Scope.Benchmark)
public class TestGraphMemoryConsumption extends AbstractJmhTestGraphBase {

    @Benchmark
    public Graph graphFill() {
        var memoryBefore = runGcAndGetUsedMemoryInMB();
        var stopwatch = StopWatch.createStarted();
        var sut = createGraph();
        triples.forEach(sut::add);
        stopwatch.stop();
        var memoryAfter = runGcAndGetUsedMemoryInMB();
        System.out.println(String.format("graphs: %d time to fill graphs: %s additional memory: %5.3f MB",
                sut.size(),
                stopwatch.formatTime(),
                (memoryAfter - memoryBefore)));
        return sut;
    }

    @Test
    public void benchmark() throws Exception {
        var opt = setupOptionsBuilderWithDefaultOptions(this.getClass())
                .warmupIterations(3)
                .measurementIterations(3)
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
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

}

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

package org.apache.jena.mem.graph.iterator;

import org.apache.jena.atlas.iterator.ActionCount;
import org.apache.jena.mem2.store.adaptive.base.iterator.ArrayIterator;
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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;


@State(Scope.Benchmark)
public class TestArrayIteratorsForeachRemaining {


    List<Object[]> arrays = new ArrayList<>(100000);

    List<Integer> sizes = new ArrayList<>(100000);


    @Param({"4", "20", "60", "80"})
    public int param0_arraySize;

    @Param({
            "ArrayIterator",
    })
    public String param1_iteratorImplementation;

    @Benchmark
    public long testSpliteratorForeachRemaining() {
        long total = 0;
        for(int i = 0; i < arrays.size(); i++) {
            var arrayWithNulls = arrays.get(i);
            var elementsCount = sizes.get(i);
            var actionCounter = new ActionCount<>();

            var sut = createSut(arrayWithNulls, elementsCount);

            sut.forEachRemaining(actionCounter::accept);

            total += actionCounter.getCount();
            Assert.assertEquals(elementsCount.longValue(), actionCounter.getCount());
        }
        return total;
    }

    public Iterator<Object> createSut(Object[] arrayWithNulls, int elementsCount) {
        var count = elementsCount;
        Runnable checkForConcurrentModification = () -> {
            if (count != elementsCount) {
                throw new RuntimeException("Concurrent modification detected");
            }
        };
        switch (param1_iteratorImplementation) {
            case "ArrayIterator":
                return new ArrayIterator<>(arrayWithNulls, 0, checkForConcurrentModification);

            default:
                throw new IllegalArgumentException("Unknown spliterator implementation: " + param1_iteratorImplementation);
        }
    }

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        for(int i = 0; i < arrays.size(); i++) {
            var arrayWithNulls = new Object[param0_arraySize];
            for (int k = 0; k < arrayWithNulls.length; k++) {
                arrayWithNulls[k] = new Object();
            }
            this.arrays.add(i, arrayWithNulls);
            this.sizes.add(i, param0_arraySize);
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
                .timeUnit(TimeUnit.SECONDS)
                .warmupTime(TimeValue.NONE)
                .warmupIterations(5)
                .measurementIterations(25)
                .measurementTime(TimeValue.NONE)
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
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }

}

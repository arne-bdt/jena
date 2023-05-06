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

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.mem2.iterator.*;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;


@State(Scope.Benchmark)
@Ignore
public class TestArrayWithNullsIteratorsHasNextNext {


    Object[] arrayWithNulls;

    int elementsCount;

    @Param({"1000000", "2000000", "3000000", "5000000"})
    public int param0_arraySize;

    @Param({
            "ArrayWithNullsIterator"
    })
    public String param1_iteratorImplementation;

    @Param({"5", "4", "3", "2", "1"})
    public float param2_stepsWithNull;


    @Benchmark
    public int testIteratorHasNextNext() {
        var sut = createSut();
        var count = 0;
        while (sut.hasNext()) {
            sut.next();
            count++;
        }
        Assert.assertEquals(elementsCount, count);
        return count;
    }

    public Iterator<Object> createSut() {
        switch (param1_iteratorImplementation) {
            case "ArrayWithNullsIterator":
                return new ArrayWithNullsIterator<>(arrayWithNulls);

            default:
                throw new IllegalArgumentException("Unknown iterator implementation: " + param1_iteratorImplementation);
        }
    }

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        arrayWithNulls = new Object[param0_arraySize];
        elementsCount = 0;
        for (int i = 0; i < arrayWithNulls.length; i+=1+ param2_stepsWithNull) {
            arrayWithNulls[i] = new Object();
            elementsCount++;
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
                .warmupIterations(4)
                .measurementIterations(15)
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

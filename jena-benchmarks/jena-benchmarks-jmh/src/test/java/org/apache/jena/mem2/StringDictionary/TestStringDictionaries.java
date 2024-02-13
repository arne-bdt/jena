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

package org.apache.jena.mem2.StringDictionary;

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
public class TestStringDictionaries {


    public static List<List<String>> splittedReferenceList = splitReferenceList();

    private static List<List<String>> splitReferenceList() {
        var splittedReferenceList = new ArrayList<List<String>>();
        var subList = new ArrayList<String>();
        for (int i = 0; i < 1000000; i++) {
            if (i % 5 == 0) {
                splittedReferenceList.add(subList);
                subList = new ArrayList<>();
            }
            subList.add("string" + i);
        }
        splittedReferenceList.add(subList);
        return splittedReferenceList;
    }

    @Benchmark
    public long testThrift2StringDictionary() {
        long total = 0;
        var sut = new org.apache.jena.riot.thrift2.StringDictionaryReader();
        for (int i = 0; i < splittedReferenceList.size(); i++) {
            var list = splittedReferenceList.get(i);
            sut.addAll(list);
            for (int j = 0; j < list.size(); j++) {
                var string = sut.get(j);
                total += string.length();
            }
        }
        return total;
    }

    @Benchmark
    public long testProto2StringDictionary() {
        long total = 0;
        var sut = new org.apache.jena.riot.protobuf2.StringDictionaryReader();
        for (int i = 0; i < splittedReferenceList.size(); i++) {
            var list = splittedReferenceList.get(i);
            sut.addAll(list);
            for (int j = 0; j < list.size(); j++) {
                var string = sut.get(j);
                total += string.length();
            }
        }
        return total;
    }


//    @Benchmark
//    public long testThrift2StringDictionary2() {
//        long total = 0;
//        var sut = new org.apache.jena.riot.thrift2.StringDictionaryReader();
//        var maxIndex = 0;
//        for (int i = 0; i < splittedReferenceList.size(); i++) {
//            var list = splittedReferenceList.get(i);
//            sut.addAll(list);
//            maxIndex += list.size();
//            for (int j = 0; j < maxIndex; j++) {
//                var string = sut.get(j);
//                total += string.length();
//            }
//        }
//        return total;
//    }
//
//    @Benchmark
//    public long testProto2StringDictionary2() {
//        long total = 0;
//        var sut = new org.apache.jena.riot.protobuf2.StringDictionaryReader();
//        var maxIndex = 0;
//        for (int i = 0; i < splittedReferenceList.size(); i++) {
//            var list = splittedReferenceList.get(i);
//            sut.addAll(list);
//            maxIndex += list.size();
//            for (int j = 0; j < maxIndex; j++) {
//                var string = sut.get(j);
//                total += string.length();
//            }
//        }
//        return total;
//    }


    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
    }

    @Test
    public void benchmark() throws Exception {
        var opt = new OptionsBuilder()
                // Specify which benchmarks to run.
                // You can be more specific if you'd like to run only one benchmark per test.
                .include(this.getClass().getName())
                // Set the following options as needed
                .mode(Mode.AverageTime)
                .timeUnit(TimeUnit.SECONDS)
                .warmupTime(TimeValue.NONE)
                .warmupIterations(10)
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

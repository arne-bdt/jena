/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *   SPDX-License-Identifier: Apache-2.0
 */

package org.apache.jena.sparql.core.mem.txn;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * One-shot helper to run the four {@code TestTxnDataset*} benchmarks with
 * reduced iteration counts so developers can sanity-check the perf claim
 * without waiting for a full JMH run. Not a JUnit test: invoke
 * {@link #main(String[])} explicitly.
 */
public class QuickRunDatasetBenchmarks {

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(TestTxnDatasetLoadInTransaction.class.getSimpleName())
                .include(TestTxnDatasetMultiGraphLoad.class.getSimpleName())
                .include(TestTxnDatasetTransactions.class.getSimpleName())
                .include(TestTxnDatasetConcurrentReadersWithWriter.class.getSimpleName())
                .mode(Mode.AverageTime)
                .timeUnit(TimeUnit.MILLISECONDS)
                .warmupIterations(3)
                .warmupTime(TimeValue.seconds(2))
                .measurementIterations(8)
                .measurementTime(TimeValue.seconds(3))
                .forks(1)
                .threads(1)
                // Skip the larger pizza file for the quick run.
                .param("param0_GraphUri", "../testing/cheeses-0.1.ttl")
                .shouldFailOnError(true)
                .build();
        new Runner(opt).run();
    }
}

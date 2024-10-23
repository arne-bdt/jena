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
package org.apache.jena.sys;

import org.apache.jena.rdf.model.ModelFactory;
import org.junit.Test;


import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class TestJenaSystem {

    /**
     * Test that the first initialization of JenaSystem is successful.
     * The name is chosen so that it is executed first.
     */
    @Test
    public void testFirstInitParallel() {

        var pool = Executors.newFixedThreadPool(8);

        var futures = IntStream.range(0, 16)
                .mapToObj(i -> pool.submit(() -> {
                    if (i % 2 == 0) {
                        ModelFactory.createDefaultModel();
                    }
                    else {
                        JenaSystem.init();
                    }

                    return i;
                }))
                .toList();

        var intSet = new HashSet<Integer>();
        assertTimeoutPreemptively(
                Duration.of(5, ChronoUnit.SECONDS),
                () -> {
                    for (var future : futures) {
                        intSet.add(future.get());
                    }
                });

        assertEquals(16, intSet.size());
    }

    /**
     * Test that the first initialization of JenaSystem is successful.
     * This test is placed in the integration tests module to ensure that the initialization
     * is successful when multiple modules are loaded.
     */
    @Test
    public void testLaterInit() {
        assertDoesNotThrow(() -> JenaSystem.init());
    }


}

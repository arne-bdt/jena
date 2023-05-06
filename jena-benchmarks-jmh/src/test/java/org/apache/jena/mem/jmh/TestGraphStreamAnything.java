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

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;

import java.util.Optional;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@State(Scope.Benchmark)
public class TestGraphStreamAnything extends AbstractTestGraphBaseWithFilledGraph {

    @Benchmark
    public Triple graphStreamBySamples_Subject_ANY_ANY() {
        Optional<Triple> it = null;
        Triple t = null;
        for (Triple sample : triples) {
            it = sut.stream(sample.getSubject(), Node.ANY, Node.ANY).findAny();
            assertTrue(it.isPresent());
            t = it.get();
            assertNotNull(t);
        }
        return t;
    }

    @Benchmark
    public Triple graphStreamBySamples_ANY_Predicate_ANY() {
        Optional<Triple> it = null;
        Triple t = null;
        for (Triple sample : triples) {
            it = sut.stream(Node.ANY, sample.getPredicate(), Node.ANY).findAny();
            assertTrue(it.isPresent());
            t = it.get();
            assertNotNull(t);
        }
        return t;
    }

    @Benchmark
    public Triple graphStreamBySamples_ANY_ANY_Object() {
        Optional<Triple> it = null;
        Triple t = null;
        for (Triple sample : triples) {
            it = sut.stream(Node.ANY, Node.ANY, sample.getObject()).findAny();
            assertTrue(it.isPresent());
            t = it.get();
            assertNotNull(t);
        }
        return t;
    }

    @Benchmark
    public Triple graphStreamBySamples_Subject_Predicate_ANY() {
        Optional<Triple> it = null;
        Triple t = null;
        for (Triple sample : triples) {
            it = sut.stream(sample.getSubject(), sample.getPredicate(), Node.ANY).findAny();
            assertTrue(it.isPresent());
            t = it.get();
            assertNotNull(t);
        }
        return t;
    }

    @Benchmark
    public Triple graphStreamBySamples_Subject_ANY_Object() {
        Optional<Triple> it = null;
        Triple t = null;
        for (Triple sample : triples) {
            it = sut.stream(sample.getSubject(), Node.ANY, sample.getObject()).findAny();
            assertTrue(it.isPresent());
            t = it.get();
            assertNotNull(t);
        }
        return t;
    }

    @Benchmark
    public Triple graphStreamBySamples_ANY_Predicate_Object() {
        Optional<Triple> it = null;
        Triple t = null;
        for (Triple sample : triples) {
            it = sut.stream(Node.ANY, sample.getPredicate(), sample.getObject()).findAny();
            assertTrue(it.isPresent());
            t = it.get();
            assertNotNull(t);
        }
        return t;
    }

    @Test
    public void benchmark() throws Exception {
        var opt = setupOptionsBuilderWithDefaultOptions(this.getClass())
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }
}

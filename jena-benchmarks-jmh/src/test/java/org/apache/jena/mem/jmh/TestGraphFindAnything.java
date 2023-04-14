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
import org.junit.Ignore;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;

import static org.junit.Assert.assertTrue;

@Ignore
@State(Scope.Benchmark)
public class TestGraphFindAnything extends AbstractTestGraphBaseWithFilledGraph {

    @Benchmark
    public ExtendedIterator<Triple> graphFindBySamples_Subject_ANY_ANY() {
        ExtendedIterator<Triple> it = null;
        for (Triple sample : triples) {
            it = sut.find(sample.getSubject(), Node.ANY, Node.ANY);
            assertTrue(it.hasNext());
        }
        return it;
    }

    @Benchmark
    public ExtendedIterator<Triple> graphFindBySamples_ANY_Predicate_ANY() {
        ExtendedIterator<Triple> it = null;
        for (Triple sample : triples) {
            it = sut.find(Node.ANY, sample.getPredicate(), Node.ANY);
            assertTrue(it.hasNext());
        }
        return it;
    }

    @Benchmark
    public ExtendedIterator<Triple> graphFindBySamples_ANY_ANY_Object() {
        ExtendedIterator<Triple> it = null;
        for (Triple sample : triples) {
            it = sut.find(Node.ANY, Node.ANY, sample.getObject());
            assertTrue(it.hasNext());
        }
        return it;
    }

    @Benchmark
    public ExtendedIterator<Triple> graphFindBySamples_Subject_Predicate_ANY() {
        ExtendedIterator<Triple> it = null;
        for (Triple sample : triples) {
            it = sut.find(sample.getSubject(), sample.getPredicate(), Node.ANY);
            assertTrue(it.hasNext());
        }
        return it;
    }

    @Benchmark
    public ExtendedIterator<Triple> graphFindBySamples_Subject_ANY_Object() {
        ExtendedIterator<Triple> it = null;
        for (Triple sample : triples) {
            it = sut.find(sample.getSubject(), Node.ANY, sample.getObject());
            assertTrue(it.hasNext());
        }
        return it;
    }

    @Benchmark
    public ExtendedIterator<Triple> graphFindBySamples_ANY_Predicate_Object() {
        ExtendedIterator<Triple> it = null;
        for (Triple sample : triples) {
            it = sut.find(Node.ANY, sample.getPredicate(), sample.getObject());
            assertTrue(it.hasNext());
        }
        return it;
    }

    @Test
    public void benchmark() throws Exception {
        var opt = setupOptionsBuilderWithDefaultOptions(this.getClass())
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }
}

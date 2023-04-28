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

import org.apache.jena.atlas.iterator.ActionCount;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.jena.mem.jmh.AbstractTestGraphBaseWithFilledGraph.cloneNode;
import static org.junit.Assert.assertTrue;

@State(Scope.Benchmark)
public class TestGraphFindBySamples extends AbstractJmhTestGraphBase {

    /**
     * The graph under test. This is initialized in {@link #fillTriplesList()}.
     */
    protected Graph sut;

    @Param({"500"})
    public int param2_sampleSize;

    private List<Triple> samples;

    @Setup(Level.Trial)
    public void fillTriplesList() throws Exception {
        super.fillTriplesList();
        this.sut = createGraph();
        // Add the same triples to the graph under test as new instances so that they are not reference equals.
        // This is important because the graph under test must not use reference equality as shortcut during the
        // benchmark.
        this.triples.forEach(t -> sut.add(Triple.create(cloneNode(t.getSubject()), cloneNode(t.getPredicate()), cloneNode(t.getObject()))));

        this.samples = new ArrayList<>(param2_sampleSize);
        var sampleIncrement = triples.size() / param2_sampleSize;
        for(var i=0; i< triples.size(); i+=sampleIncrement) {
            this.samples.add(triples.get(i));
        }
    }

    @Benchmark
    public int graphFindBySamples_Subject_ANY_ANY() {
        var total = 0;
        for (Triple sample : samples) {
            total += count(sut.find(sample.getSubject(), Node.ANY, Node.ANY));
        }
        assertTrue(total > 0);
        return total;
    }

    @Benchmark
    public int graphFindBySamples_ANY_Predicate_ANY() {
        var total = 0;
        for (Triple sample : samples) {
            total += count(sut.find(Node.ANY, sample.getPredicate(), Node.ANY));
        }
        assertTrue(total > 0);
        return total;
    }

    @Benchmark
    public int graphFindBySamples_ANY_ANY_Object() {
        var total = 0;
        for (Triple sample : samples) {
            total += count(sut.find(Node.ANY, Node.ANY, sample.getObject()));
        }
        assertTrue(total > 0);
        return total;
    }

    @Benchmark
    public int graphFindBySamples_Subject_Predicate_ANY() {
        var total = 0;
        for (Triple sample : samples) {
            total += count(sut.find(sample.getSubject(), sample.getPredicate(), Node.ANY));
        }
        assertTrue(total > 0);
        return total;
    }

    @Benchmark
    public int graphFindBySamples_Subject_ANY_Object() {
        var total = 0;
        for (Triple sample : samples) {
            total += count(sut.find(sample.getSubject(), Node.ANY, sample.getObject()));
        }
        assertTrue(total > 0);
        return total;
    }

    @Benchmark
    public int graphFindBySamples_ANY_Predicate_Object() {
        var total = 0;
        for (Triple sample : samples) {
            total += count(sut.find(Node.ANY, sample.getPredicate(), sample.getObject()));
        }
        assertTrue(total > 0);
        return total;
    }

    private static int count(final ExtendedIterator extendedIterator) {
        var actionCounter = new ActionCount<>();
        extendedIterator.forEachRemaining(actionCounter::accept);
        return (int)actionCounter.getCount();
    }

    @Test
    public void benchmark() throws Exception {
        var opt = setupOptionsBuilderWithDefaultOptions(this.getClass())
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }
}

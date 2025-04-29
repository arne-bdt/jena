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

package org.apache.jena.mem.graph;

import org.apache.jena.atlas.iterator.ActionCount;
import org.apache.jena.graph.Graph;
import org.apache.jena.mem.graph.helper.Context;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.mem.graph.helper.Releases;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import static org.junit.Assert.assertEquals;


@State(Scope.Benchmark)
public class TestGraphFindAllWithForEachRemaining {

    @Param({
            "../testing/cheeses-0.1.ttl",
            "../testing/pizza.owl.rdf",
            "../testing/BSBM/bsbm-1m.nt.gz",
            "../testing/BSBM/bsbm-5m.nt.gz",
//            "../testing/BSBM/bsbm-25m.nt.gz",
    })
    public String param0_GraphUri;

    @Param({
            "GraphMem (current)",
            "GraphMem2Fast (current)",
            "GraphMem2Legacy (current)",
            "GraphMem2Roaring (current)",
            "GraphMem (Jena 5.3.0)",
    })
    public String param1_GraphImplementation;
    java.util.function.Supplier<Long> graphFindAll;
    private Graph sutCurrent;
    private org.apache.shadedJena530.graph.Graph sut530;

    @Benchmark
    public Long graphFindAll() {
        return graphFindAll.get();
    }

    private Long graphFindAllCurrent() {
        var actionCounter = new ActionCount<>();
        var iter = sutCurrent.find();
        iter.forEachRemaining(actionCounter::accept);
        iter.close();
        assertEquals(sutCurrent.size(), actionCounter.getCount());
        return actionCounter.getCount();
    }

    private Long graphFindAll530() {
        var actionCounter = new ActionCount<>();
        var iter = sut530.find();
        iter.forEachRemaining(actionCounter::accept);
        iter.close();
        assertEquals(sut530.size(), actionCounter.getCount());
        return actionCounter.getCount();
    }

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        Context trialContext = new Context(param1_GraphImplementation);
        switch (trialContext.getJenaVersion()) {
            case CURRENT: {
                this.sutCurrent = Releases.current.createGraph(trialContext.getGraphClass());
                this.graphFindAll = this::graphFindAllCurrent;

                var triples = Releases.current.readTriples(param0_GraphUri);
                triples.forEach(this.sutCurrent::add);
            }
            break;
            case JENA_5_3_0: {
                this.sut530 = Releases.v530.createGraph(trialContext.getGraphClass());
                this.graphFindAll = this::graphFindAll530;

                var triples = Releases.v530.readTriples(param0_GraphUri);
                triples.forEach(this.sut530::add);
            }
            break;
            default:
                throw new IllegalArgumentException("Unknown Jena version: " + trialContext.getJenaVersion());
        }
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JMHDefaultOptions.getDefaults(this.getClass())
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }
}
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

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.graph.helper.Context;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.mem.graph.helper.Releases;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import static org.junit.Assert.assertNotNull;


@State(Scope.Benchmark)
public class TestGraphFindByMatchAndCount {

    @Param({
//            "../testing/cheeses-0.1.ttl",
//            "../testing/pizza.owl.rdf",
            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml",
            "../testing/BSBM/bsbm-1m.nt.gz",
    })
    public String param0_GraphUri;

    @Param({
//            "GraphMem (current)",
            "GraphMem2Fast (current)",
//            "GraphMem2Legacy (current)",
            "GraphMem2Roaring (current)",
//            "GraphMem (Jena 5.3.0)",
    })
    public String param1_GraphImplementation;

    @Param({"800"})
    public int param2_sampleSize;
    Function<String, Object> graphFindByMatchesAndCount;
    private Graph sutCurrent;
    private org.apache.shadedJena530.graph.Graph sut530;
    private List<Triple> triplesToFindCurrent;
    private List<org.apache.shadedJena530.graph.Triple> triplesToFind530;

    @Benchmark
    public Object graphFindS__() {
        return graphFindByMatchesAndCount.apply("S__");
    }

    @Benchmark
    public Object graphFind_P_() {
        return graphFindByMatchesAndCount.apply("_P_");
    }

    @Benchmark
    public Object graphFind__O() {
        return graphFindByMatchesAndCount.apply("__O");
    }

    @Benchmark
    public Object graphFindSP_() {
        return graphFindByMatchesAndCount.apply("SP_");
    }

    @Benchmark
    public Object graphFindS_O() {
        return graphFindByMatchesAndCount.apply("S_O");
    }

    @Benchmark
    public Object graphFind_PO() {
        return graphFindByMatchesAndCount.apply("_PO");
    }


    private int graphFindByMatchesAndCount(String pattern) {
        var findFunction = getFindFunctionByPatternCurrent(pattern);
        var total = 0;
        for (Triple sample : this.triplesToFindCurrent) {
            total += Iter.count(findFunction.apply(sample));
        }
        return total;
    }

    private Object graphFindByMatchesAndCount530(String pattern) {
        var findFunction = getFindFunctionByPattern530(pattern);
        var total = 0;
        for (org.apache.shadedJena530.graph.Triple sample : this.triplesToFind530) {
            total += Iter.count(findFunction.apply(sample));
        }
        return total;
    }

    Function<Triple, Iterator<Triple>> getFindFunctionByPatternCurrent(String pattern) {
        switch (pattern) {
            case "S__":
                return t -> sutCurrent.find(t.getSubject(), null, null);
            case "_P_":
                return t -> sutCurrent.find(null, t.getPredicate(), null);
            case "__O":
                return t -> sutCurrent.find(null, null, t.getObject());
            case "SP_":
                return t -> sutCurrent.find(t.getSubject(), t.getPredicate(), null);
            case "S_O":
                return t -> sutCurrent.find(t.getSubject(), null, t.getObject());
            case "_PO":
                return t -> sutCurrent.find(null, t.getPredicate(), t.getObject());
            default:
                throw new IllegalArgumentException("Unknown pattern: " + pattern);
        }
    }

    Function<org.apache.shadedJena530.graph.Triple, Iterator<org.apache.shadedJena530.graph.Triple>> getFindFunctionByPattern530(String pattern) {
        switch (pattern) {
            case "S__":
                return t -> sut530.find(t.getSubject(), null, null);
            case "_P_":
                return t -> sut530.find(null, t.getPredicate(), null);
            case "__O":
                return t -> sut530.find(null, null, t.getObject());
            case "SP_":
                return t -> sut530.find(t.getSubject(), t.getPredicate(), null);
            case "S_O":
                return t -> sut530.find(t.getSubject(), null, t.getObject());
            case "_PO":
                return t -> sut530.find(null, t.getPredicate(), t.getObject());
            default:
                throw new IllegalArgumentException("Unknown pattern: " + pattern);
        }
    }

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        Context trialContext = new Context(param1_GraphImplementation);
        switch (trialContext.getJenaVersion()) {
            case CURRENT: {
                this.sutCurrent = Releases.current.createGraph(trialContext.getGraphClass());
                this.graphFindByMatchesAndCount = this::graphFindByMatchesAndCount;

                var triples = Releases.current.readTriples(param0_GraphUri);
                triples.forEach(this.sutCurrent::add);

                /*clone the triples because they should not be the same objects*/
                this.triplesToFindCurrent = new ArrayList<>(param2_sampleSize);
                var sampleIncrement = triples.size() / param2_sampleSize;
                for (var i = 0; i < triples.size(); i += sampleIncrement) {
                    this.triplesToFindCurrent.add(Releases.current.cloneTriple(triples.get(i)));
                }
                    /* Shuffle is import because the order might play a role. We want to test the performance of the
                       contains method regardless of the order */
                java.util.Collections.shuffle(this.triplesToFindCurrent, new Random(4721));
            }
            break;
            case JENA_5_3_0: {
                this.sut530 = Releases.v530.createGraph(trialContext.getGraphClass());
                this.graphFindByMatchesAndCount = this::graphFindByMatchesAndCount530;

                var triples = Releases.v530.readTriples(param0_GraphUri);
                triples.forEach(this.sut530::add);

                /*clone the triples because they should not be the same objects*/
                this.triplesToFind530 = new ArrayList<>(param2_sampleSize);
                var sampleIncrement = triples.size() / param2_sampleSize;
                for (var i = 0; i < triples.size(); i += sampleIncrement) {
                    this.triplesToFind530.add(Releases.v530.cloneTriple(triples.get(i)));
                }
                    /* Shuffle is import because the order might play a role. We want to test the performance of the
                       contains method regardless of the order */
                java.util.Collections.shuffle(this.triplesToFind530, new Random(4721));
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
        assertNotNull(results);
    }
}
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

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.graph.helper.Context;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.mem.graph.helper.Releases;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.util.List;
import java.util.Random;


@State(Scope.Benchmark)
public class TestGraphContainsTriple {

    @Param({
//            "../testing/cheeses-0.1.ttl",
//            "../testing/pizza.owl.rdf",
            "C:/temp/res_test/xxx_CGMES_EQ.xml",
            "C:/temp/res_test/xxx_CGMES_SSH.xml",
            "C:/temp/res_test/xxx_CGMES_TP.xml",
            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml",
            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml",
            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml",
            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml",
            "../testing/BSBM/bsbm-1m.nt.gz",
            "../testing/BSBM/bsbm-5m.nt.gz",
//            "../testing/BSBM/bsbm-25m.nt.gz",
    })
    public String param0_GraphUri;

    @Param({
//            "GraphMem (current)",
            "GraphMem2Fast (current)",
//            "GraphMem2Legacy (current)",
//            "GraphMem2Roaring (current)",
//            "GraphMem (Jena 5.3.0)",
            "GraphTxn (current)",
            "GraphWrapperTransactional (current)",
    })
    public String param1_GraphImplementation;
    java.util.function.Supplier<Boolean> graphContains;
    private Graph sutCurrent;
    private org.apache.shadedJena530.graph.Graph sut530;
    private List<Triple> triplesToFindCurrent;
    private List<org.apache.shadedJena530.graph.Triple> triplesToFind530;

    @Benchmark
    public boolean graphContains() {
        return graphContains.get();
    }

    private boolean graphContainsCurrent() {
        var found = false;
        for (var t : triplesToFindCurrent) {
            found = sutCurrent.contains(t);
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean graphContains530() {
        var found = false;
        for (var t : triplesToFind530) {
            found = sut530.contains(t);
            Assert.assertTrue(found);
        }
        return found;
    }


    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        var trialContext = new Context(param1_GraphImplementation);
        switch (trialContext.getJenaVersion()) {
            case CURRENT: {
                this.sutCurrent = Releases.current.createGraph(trialContext.getGraphClass());
                this.graphContains = this::graphContainsCurrent;

                var triples = Releases.current.readTriples(param0_GraphUri);
                triples.forEach(this.sutCurrent::add);

                /*clone the triples because they should not be the same objects*/
                this.triplesToFindCurrent = Releases.current.cloneTriples(triples);
                    /* Shuffle is import because the order might play a role. We want to test the performance of the
                       contains method regardless of the order */
                java.util.Collections.shuffle(this.triplesToFindCurrent, new Random(4721));
            }
            break;
            case JENA_5_3_0: {
                this.sut530 = Releases.v530.createGraph(trialContext.getGraphClass());
                this.graphContains = this::graphContains530;

                var triples = Releases.v530.readTriples(param0_GraphUri);
                triples.forEach(this.sut530::add);

                /*clone the triples because they should not be the same objects*/
                this.triplesToFind530 = Releases.v530.cloneTriples(triples);
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
        Assert.assertNotNull(results);
    }
}

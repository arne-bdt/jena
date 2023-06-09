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

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.TripleReaderReadingCGMES_2_4_15_WithTypedLiterals;
import org.apache.jena.mem.graph.helper.Context;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.mem.graph.helper.Releases;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@State(Scope.Benchmark)
public class TestGraphBulkUpdates {

    @Param({
//            "../testing/cheeses-0.1.ttl",
//            "../testing/pizza.owl.rdf",
//            "C:/temp/res_test/xxx_CGMES_EQ.xml",
//            "C:/temp/res_test/xxx_CGMES_SSH.xml",
//            "C:/temp/res_test/xxx_CGMES_TP.xml",
            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml",
            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml",
            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml",
            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml",
//            "../testing/BSBM/bsbm-1m.nt.gz",
            "../testing/BSBM/bsbm-5m.nt.gz",
            "../testing/BSBM/bsbm-25m.nt.gz",
    })
    public String param0_GraphUri;

    @Param({
            "GraphMem (current)",
//            "GraphMemB (current)",
            "GraphMem2Fast (current)",
//            "GraphMem2Huge (current)",
            "GraphMem2Legacy (current)",
            "GraphMem2Roaring (current)",
//              "GraphMem (Jena 4.8.0)",
    })
    public String param1_GraphImplementation;

    private Graph sutCurrent;
    private org.apache.shadedJena480.graph.Graph sut480;
    private List<org.apache.shadedJena480.graph.Triple> triples480;

    private List<Triple> triples;


//    @Test
//    public void testBulkLoad() {
//        var trialContext = new Context("GraphMem2Roaring (current)");
//        this.sutCurrent = Releases.current.createGraph(trialContext.getGraphClass());
//
//        var triples = TripleReaderReadingCGMES_2_4_15_WithTypedLiterals
//                .read("C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml");
//
//        triples.forEach(this.sutCurrent::add);
//
//        var doubleTriples = new ArrayList<Triple>();
//        for(var t: triples) {
//            if(t.getObject().isLiteral() && t.getObject().getLiteralDatatype() == XSDDatatype.XSDfloat) {
//                doubleTriples.add(t);
//            }
//        }
//        System.out.println("Found " + doubleTriples.size() + " triples with double literals");
//
//        for(int i=0; i<2; i++) {
//            for (var t : doubleTriples) {
//                var oldTriple = this.sutCurrent.find(t.getSubject(), t.getPredicate(), Node.ANY).next();
//                var oldValue = (float) oldTriple.getObject().getIndexingValue();
//                this.sutCurrent.delete(oldTriple);
//                this.sutCurrent.add(Triple.create(t.getSubject(), t.getPredicate(), NodeFactory.createLiteralByValue((oldValue + 1.0f), oldTriple.getObject().getLiteralDatatype())));
//            }
//        }
//    }


    @Benchmark
    public Graph bulkUpdate() {
        var doubleTriples = new ArrayList<Triple>();
        for(var t: triples) {
            if(t.getObject().isLiteral() && t.getObject().getLiteralDatatype() == XSDDatatype.XSDfloat) {
                doubleTriples.add(t);
            }
        }
        /* Shuffle is import because the order might play a role. We want to test the performance of the
           contains method regardless of the order */
        Collections.shuffle(doubleTriples);
        //System.out.println("Found " + doubleTriples.size() + " triples with double literals");

        for(int i=0; i<2; i++) {
            for (var t : doubleTriples) {
                var oldTriple = this.sutCurrent.find(t.getSubject(), t.getPredicate(), Node.ANY).next();
                var oldValue = (float) oldTriple.getObject().getIndexingValue();
                this.sutCurrent.delete(oldTriple);
                this.sutCurrent.add(Triple.create(t.getSubject(), t.getPredicate(), NodeFactory.createLiteralByValue((oldValue + 1.0f), oldTriple.getObject().getLiteralDatatype())));
            }
        }
        return sutCurrent;
    }


    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        var trialContext = new Context(param1_GraphImplementation);
        switch (trialContext.getJenaVersion()) {
            case CURRENT:
                {
                    this.sutCurrent = Releases.current.createGraph(trialContext.getGraphClass());

                    this.triples = TripleReaderReadingCGMES_2_4_15_WithTypedLiterals
                            .read(param0_GraphUri);

                    triples.forEach(this.sutCurrent::add);
                }
                break;
            case JENA_4_8_0:
                {
                    this.sut480 = Releases.v480.createGraph(trialContext.getGraphClass());

                    var triples = Releases.v480.readTriples(param0_GraphUri);
                    triples.forEach(this.sut480::add);

                    /*clone the triples because they should not be the same objects*/
                    this.triples480 = Releases.v480.cloneTriples(triples);
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

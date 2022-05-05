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

package org.apache.jena.mem;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.function.Supplier;

public class TestGraphMemVariants extends TestGraphMemVariantsBase {

    @Test
    public void cheeses_ttl() {
        loadGraphsMeasureTimeAndMemory(graphImplementationsToTest,
                50, 1000,
                "./../jena-examples/src/main/resources/data/cheeses-0.1.ttl");
    }

    @Test
    public void pizza_owl_rdf() {
        loadGraphsMeasureTimeAndMemory(graphImplementationsToTest,
                50, 1500,
                "./../jena-examples/src/main/resources/data/pizza.owl.rdf");
    }

    /**
     * Generated large dataset.
     * Tool:
     * http://wbsg.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/spec/BenchmarkRules/index.html#datagenerator
     * Generated with: java -cp lib/* benchmark.generator.Generator -pc 50000 -s ttl -ud
     */
    @Test
    public void BSBM_50000() {
        loadGraphsMeasureTimeAndMemory(graphImplementationsToTest,
                1, 100,
                "./../jena-examples/src/main/resources/data/BSBM_50000.ttl.gz");
    }

    /**
     * Due to copyright, data cannot be added to the repository.
     * Download https://www.entsoe.eu/Documents/CIM_documents/Grid_Model_CIM/ENTSO-E_Conformity_Assessment_Scheme_v3.0.zip
     * from https://www.entsoe.eu/digital/cim/cim-conformity-and-interoperability/     *
     */
    @Test
    public void ENTSO_E_Test_Configurations_v3_0_RealGrid_EQ_SSH_SV_and_TP() {
        loadGraphsMeasureTimeAndMemory(graphImplementationsToTest, 1, 100,
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml",
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml",
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml",
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml");
    }

    /**
     * Due to copyright, data cannot be added to the repository.
     * Download https://www.entsoe.eu/Documents/CIM_documents/Grid_Model_CIM/ENTSO-E_Conformity_Assessment_Scheme_v3.0.zip
     * from https://www.entsoe.eu/digital/cim/cim-conformity-and-interoperability/     *
     */
    @Test
    public void ENTSO_E_Test_Configurations_v3_0_RealGrid_EQ() {
        loadGraphsMeasureTimeAndMemory(graphImplementationsToTest, 1, 500,
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml");
    }

    /**
     * Due to copyright, data cannot be added to the repository.
     * Download https://www.entsoe.eu/Documents/CIM_documents/Grid_Model_CIM/ENTSO-E_Conformity_Assessment_Scheme_v3.0.zip
     * from https://www.entsoe.eu/digital/cim/cim-conformity-and-interoperability/     *
     */
    @Test
    public void ENTSO_E_Test_Configurations_v3_0_RealGrid_SSH() {
        loadGraphsMeasureTimeAndMemory(graphImplementationsToTest, 1, 500,
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml");
    }

    /**
     * Due to copyright, data cannot be added to the repository.
     * Download https://www.entsoe.eu/Documents/CIM_documents/Grid_Model_CIM/ENTSO-E_Conformity_Assessment_Scheme_v3.0.zip
     * from https://www.entsoe.eu/digital/cim/cim-conformity-and-interoperability/     *
     */
    @Test
    public void ENTSO_E_Test_Configurations_v3_0_RealGrid_SV() {
        loadGraphsMeasureTimeAndMemory(graphImplementationsToTest, 1, 500,
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml");
    }

    /**
     * Due to copyright, data cannot be added to the repository.
     * Download https://www.entsoe.eu/Documents/CIM_documents/Grid_Model_CIM/ENTSO-E_Conformity_Assessment_Scheme_v3.0.zip
     * from https://www.entsoe.eu/digital/cim/cim-conformity-and-interoperability/     *
     */
    @Test
    public void ENTSO_E_Test_Configurations_v3_0_RealGrid_TP() {
        loadGraphsMeasureTimeAndMemory(graphImplementationsToTest, 1, 500,
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml");
    }

    private void loadGraphsMeasureTimeAndMemory(List<Pair<String, Supplier<Graph>>> graphVariantSuppliersWithNames, int graphMultiplier, int numberOfRandomTriplesToSearchFor, String... graphUris) {
        final var triplesPerGraph = loadTriples(graphMultiplier, graphUris);
        final var existingTriplesToSearchFor = selectRandomTriples(triplesPerGraph, numberOfRandomTriplesToSearchFor);
        final var nonExistingTriplesToSearchFor = generateRandomTriples(1000);
        for (Pair<String, Supplier<Graph>> graphSuppliersWithName : graphVariantSuppliersWithNames) {
            System.out.println("graph variant: '" + graphSuppliersWithName.getKey() + "'");
            var graphs = fillGraphs(graphSuppliersWithName.getValue(), triplesPerGraph);
            if(0 < numberOfRandomTriplesToSearchFor && !(graphSuppliersWithName.getValue().get() instanceof GraphMemWithArrayListOnly)) {
                var stopwatchWholeSearch = StopWatch.createStarted();
                {
                    var count___ = 0;
                    var countS__ = 0;
                    var count_P_ = 0;
                    var count__O = 0;
                    var countSP_ = 0;
                    var countSPO = 0;
                    var countS_O = 0;
                    var count_PO = 0;
                    var stopwatchContains = StopWatch.createStarted();
                    stopwatchContains.suspend();
                    var stopwatchNonExisting = StopWatch.createStarted();
                    stopwatchNonExisting.suspend();
                    var stopwatch___ = StopWatch.createStarted();
                    stopwatch___.suspend();
                    var stopwatchS__ = StopWatch.createStarted();
                    stopwatchS__.suspend();
                    var stopwatch_P_ = StopWatch.createStarted();
                    stopwatch_P_.suspend();
                    var stopwatch__O = StopWatch.createStarted();
                    stopwatch__O.suspend();
                    var stopwatchSP_ = StopWatch.createStarted();
                    stopwatchSP_.suspend();
                    var stopwatchSPO = StopWatch.createStarted();
                    stopwatchSPO.suspend();
                    var stopwatchS_O = StopWatch.createStarted();
                    stopwatchS_O.suspend();
                    var stopwatch_PO = StopWatch.createStarted();
                    stopwatch_PO.suspend();
                    for (int i = 0; i < graphs.size(); i++) {
                        var graph = graphs.get(i);
                        var triples = existingTriplesToSearchFor.get(i);

                        stopwatchContains.resume();
                        for (Triple t : triples) {
                            if(!graph.contains(t)) {
                                Assert.fail();
                            }
                        }
                        stopwatchContains.suspend();

                        stopwatchNonExisting.resume();
                        var counter=0;
                        while(counter < triples.size()) {
                            for (Triple t : nonExistingTriplesToSearchFor) {
                                if(graph.contains(t)) {
                                    Assert.fail();
                                }
                                counter++;
                            }
                        }
                        stopwatchNonExisting.suspend();

                        stopwatch___.resume();
                        count___ += count(graph.find());
                        stopwatch___.suspend();

                        stopwatchS__.resume();
                        for (Triple t : triples) {
                            countS__ += count(graph.find(t.getSubject(), Node.ANY, Node.ANY));
                        }
                        stopwatchS__.suspend();

                        stopwatch_P_.resume();
                        for (Triple t : triples) {
                            count_P_ += count(graph.find(Node.ANY, t.getPredicate(), Node.ANY));
                        }
                        stopwatch_P_.suspend();

                        stopwatch__O.resume();
                        for (Triple t : triples) {
                            count__O += count(graph.find(Node.ANY, Node.ANY, t.getObject()));
                        }
                        stopwatch__O.suspend();

                        stopwatchSP_.resume();
                        for (Triple t : triples) {
                            countSP_ += count(graph.find(t.getSubject(), t.getPredicate(), Node.ANY));
                        }
                        stopwatchSP_.suspend();

                        stopwatchSPO.resume();
                        for (Triple t : triples) {
                            countSPO += count(graph.find(t.getSubject(), t.getPredicate(), t.getObject()));
                        }
                        stopwatchSPO.suspend();

                        stopwatchS_O.resume();
                        for (Triple t : triples) {
                            countS_O += count(graph.find(t.getSubject(), Node.ANY, t.getObject()));
                        }
                        stopwatchS_O.suspend();

                        stopwatch_PO.resume();
                        for (Triple t : triples) {
                            count_PO += count(graph.find(Node.ANY, t.getPredicate(), t.getObject()));
                        }
                        stopwatch_PO.suspend();
                    }
                    stopwatchContains.stop();
                    stopwatchNonExisting.stop();
                    stopwatch___.stop();
                    stopwatchS__.stop();
                    stopwatch_P_.stop();
                    stopwatch__O.stop();
                    stopwatchSP_.stop();
                    stopwatchSPO.stop();
                    stopwatchS_O.stop();
                    stopwatch_PO.stop();
                    System.out.println(String.format("Graph.find: ___: %d/%s S__: %d/%s _P_: %d/%s __O: %d/%s SP_: %d/%s SPO: %d/%s S_O: %d/%s _PO: %d/%s contains: %s non_existing: %s",
                            count___,
                            stopwatch___.formatTime(),
                            countS__,
                            stopwatchS__.formatTime(),
                            count_P_,
                            stopwatch_P_.formatTime(),
                            count__O,
                            stopwatch__O.formatTime(),
                            countSP_,
                            stopwatchSP_.formatTime(),
                            countSPO,
                            stopwatchSPO.formatTime(),
                            countS_O,
                            stopwatchS_O.formatTime(),
                            count_PO,
                            stopwatch_PO.formatTime(),
                            stopwatchContains.formatTime(),
                            stopwatchNonExisting.formatTime()));
                }
                stopwatchWholeSearch.stop();
                System.out.println("number of random triples to search for: " + numberOfRandomTriplesToSearchFor + " total time of all Graph.find and Graph.contains operations: " + stopwatchWholeSearch.formatTime());
            }
            if(0 < numberOfRandomTriplesToSearchFor && !(graphSuppliersWithName.getValue().get() instanceof GraphMemWithArrayListOnly)) {
                var stopwatchWholeSearch = StopWatch.createStarted();
                {
                    var count___ = 0;
                    var countS__ = 0;
                    var count_P_ = 0;
                    var count__O = 0;
                    var countSP_ = 0;
                    var countSPO = 0;
                    var countS_O = 0;
                    var count_PO = 0;
                    var stopwatch___ = StopWatch.createStarted();
                    stopwatch___.suspend();
                    var stopwatchS__ = StopWatch.createStarted();
                    stopwatchS__.suspend();
                    var stopwatch_P_ = StopWatch.createStarted();
                    stopwatch_P_.suspend();
                    var stopwatch__O = StopWatch.createStarted();
                    stopwatch__O.suspend();
                    var stopwatchSP_ = StopWatch.createStarted();
                    stopwatchSP_.suspend();
                    var stopwatchSPO = StopWatch.createStarted();
                    stopwatchSPO.suspend();
                    var stopwatchS_O = StopWatch.createStarted();
                    stopwatchS_O.suspend();
                    var stopwatch_PO = StopWatch.createStarted();
                    stopwatch_PO.suspend();
                    for (int i = 0; i < graphs.size(); i++) {
                        var graph = graphs.get(i);
                        var triples = existingTriplesToSearchFor.get(i);
                        var nonExistingTriples = nonExistingTriplesToSearchFor.get(i);

                        stopwatch___.resume();
                        count___ += graph.stream().count();
                        stopwatch___.suspend();

                        stopwatchS__.resume();
                        for (Triple t : triples) {
                            countS__ += graph.stream(t.getSubject(), Node.ANY, Node.ANY).count();
                        }
                        stopwatchS__.suspend();

                        stopwatch_P_.resume();
                        for (Triple t : triples) {
                            count_P_ += graph.stream(Node.ANY, t.getPredicate(), Node.ANY).count();
                        }
                        stopwatch_P_.suspend();

                        stopwatch__O.resume();
                        for (Triple t : triples) {
                            count__O += graph.stream(Node.ANY, Node.ANY, t.getObject()).count();
                        }
                        stopwatch__O.suspend();

                        stopwatchSP_.resume();
                        for (Triple t : triples) {
                            countSP_ += graph.stream(t.getSubject(), t.getPredicate(), Node.ANY).count();
                        }
                        stopwatchSP_.suspend();

                        stopwatchSPO.resume();
                        for (Triple t : triples) {
                            countSPO += graph.stream(t.getSubject(), t.getPredicate(), t.getObject()).count();
                        }
                        stopwatchSPO.suspend();

                        stopwatchS_O.resume();
                        for (Triple t : triples) {
                            countS_O += graph.stream(t.getSubject(), Node.ANY, t.getObject()).count();
                        }
                        stopwatchS_O.suspend();

                        stopwatch_PO.resume();
                        for (Triple t : triples) {
                            count_PO += graph.stream(Node.ANY, t.getPredicate(), t.getObject()).count();
                        }
                        stopwatch_PO.suspend();
                    }
                    stopwatch___.stop();
                    stopwatchS__.stop();
                    stopwatch_P_.stop();
                    stopwatch__O.stop();
                    stopwatchSP_.stop();
                    stopwatchSPO.stop();
                    stopwatchS_O.stop();
                    stopwatch_PO.stop();
                    System.out.println(String.format("Graph.stream().count(): ___: %d/%s S__: %d/%s _P_: %d/%s __O: %d/%s SP_: %d/%s SPO: %d/%s S_O: %d/%s _PO: %d/%s",
                            count___,
                            stopwatch___.formatTime(),
                            countS__,
                            stopwatchS__.formatTime(),
                            count_P_,
                            stopwatch_P_.formatTime(),
                            count__O,
                            stopwatch__O.formatTime(),
                            countSP_,
                            stopwatchSP_.formatTime(),
                            countSPO,
                            stopwatchSPO.formatTime(),
                            countS_O,
                            stopwatchS_O.formatTime(),
                            count_PO,
                            stopwatch_PO.formatTime()));
                }
                stopwatchWholeSearch.stop();
                System.out.println("number of random triples to search for: " + numberOfRandomTriplesToSearchFor + " total time of all Graph.stream.count operations: " + stopwatchWholeSearch.formatTime());
            }
        }
    }
}

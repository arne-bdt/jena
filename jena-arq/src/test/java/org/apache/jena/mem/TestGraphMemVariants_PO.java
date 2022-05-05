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
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.function.Supplier;

@Ignore
public class TestGraphMemVariants_PO extends TestGraphMemVariantsBase {

    @Test
    public void cheeses_ttl() {
        loadGraphsMeasureTimeAndMemory_PO(graphImplementationsToTest,
                50, 5000,
                "./../jena-examples/src/main/resources/data/cheeses-0.1.ttl");
    }

    @Test
    public void pizza_owl_rdf() {
        loadGraphsMeasureTimeAndMemory_PO(graphImplementationsToTest,
                50, 5000,
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
        loadGraphsMeasureTimeAndMemory_PO(graphImplementationsToTest,
                1, 1000,
                "./../jena-examples/src/main/resources/data/BSBM_50000.ttl.gz");
    }

    /**
     * Due to copyright, data cannot be added to the repository.
     * Download https://www.entsoe.eu/Documents/CIM_documents/Grid_Model_CIM/ENTSO-E_Conformity_Assessment_Scheme_v3.0.zip
     * from https://www.entsoe.eu/digital/cim/cim-conformity-and-interoperability/     *
     */
    @Test
    public void ENTSO_E_Test_Configurations_v3_0_RealGrid() {
        loadGraphsMeasureTimeAndMemory_PO(graphImplementationsToTest, 1, 1000,
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml",
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml",
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml",
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml");
    }

    private void loadGraphsMeasureTimeAndMemory_PO(List<Pair<String, Supplier<Graph>>> graphVariantSuppliersWithNames, int graphMultiplier, int numberOfRandomTriplesToSearchFor, String... graphUris) {
        final var triplesPerGraph = loadTriples(graphMultiplier, graphUris);
        final var existingTriplesToSearchFor = selectRandomTriples(triplesPerGraph, numberOfRandomTriplesToSearchFor);
        final var nonExistingTriplesToSearchFor = generateRandomTriples(1000);
        for (Pair<String, Supplier<Graph>> graphSuppliersWithName : graphVariantSuppliersWithNames) {
            System.out.println("graph variant: '" + graphSuppliersWithName.getKey() + "'");
            var graphs = fillGraphs(graphSuppliersWithName.getValue(), triplesPerGraph);
            if(0 < numberOfRandomTriplesToSearchFor) {
                var stopwatchWholeSearch = StopWatch.createStarted();
                {
                    var count_PO = 0;
                    var stopwatch_PO = StopWatch.createStarted();
                    stopwatch_PO.suspend();
                    for (int i = 0; i < graphs.size(); i++) {
                        var graph = graphs.get(i);
                        var triples = existingTriplesToSearchFor.get(i);

                        stopwatch_PO.resume();
                        for (Triple t : triples) {
                            count_PO += count(graph.find(Node.ANY, t.getPredicate(), t.getObject()));
                        }
                        stopwatch_PO.suspend();
                    }
                    System.out.println(String.format("Graph.find: _PO: %d/%s",
                            count_PO,
                            stopwatch_PO.formatTime()));
                }
                stopwatchWholeSearch.stop();
                System.out.println("graph-variant: '" + graphSuppliersWithName.getKey() + "' graphs: " + graphs.size() + " number of random triples to search for: " + numberOfRandomTriplesToSearchFor + " total time of all Graph.find and Graph.contains operations: " + stopwatchWholeSearch.formatTime());
            }
        }
    }

}

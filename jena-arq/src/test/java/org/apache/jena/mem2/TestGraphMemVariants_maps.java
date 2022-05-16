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

package org.apache.jena.mem2;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.generic.LeanHashMap;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class TestGraphMemVariants_maps extends TestGraphMemVariantsBase {

    @Test
    public void pizza_owl_rdf() {
        loadGraphsMeasureTimeAndMemory_load(graphImplementationsToTest, Triple::getSubject,
                "./../jena-examples/src/main/resources/data/pizza.owl.rdf");
    }

    @Test
    public void cheeses_ttl() {
        loadGraphsMeasureTimeAndMemory_load(graphImplementationsToTest, Triple::getSubject,
                "./../jena-examples/src/main/resources/data/cheeses-0.1.ttl");
    }

    /**
     * Generated large dataset.
     * Tool:
     * http://wbsg.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/spec/BenchmarkRules/index.html#datagenerator
     * Generated with: java -cp lib/* benchmark.generator.Generator -pc 50000 -s ttl -ud
     */
    @Test
    @Ignore
    public void BSBM_50000() {
        loadGraphsMeasureTimeAndMemory_load(graphImplementationsToTest, Triple::getSubject,
                "./../jena-examples/src/main/resources/data/BSBM_50000.ttl.gz");
    }

    /**
     * Due to copyright, data cannot be added to the repository.
     * Download https://www.entsoe.eu/Documents/CIM_documents/Grid_Model_CIM/ENTSO-E_Conformity_Assessment_Scheme_v3.0.zip
     * from https://www.entsoe.eu/digital/cim/cim-conformity-and-interoperability/     *
     */
    @Test
    public void ENTSO_E_Test_Configurations_v3_0_RealGrid_EQ_SSH_SV_and_TP() {
        loadGraphsMeasureTimeAndMemory_load(graphImplementationsToTest, Triple::getSubject,
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
        loadGraphsMeasureTimeAndMemory_load(graphImplementationsToTest, Triple::getSubject,
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml");
    }

    /**
     * Due to copyright, data cannot be added to the repository.
     * Download https://www.entsoe.eu/Documents/CIM_documents/Grid_Model_CIM/ENTSO-E_Conformity_Assessment_Scheme_v3.0.zip
     * from https://www.entsoe.eu/digital/cim/cim-conformity-and-interoperability/     *
     */
    @Test
    public void ENTSO_E_Test_Configurations_v3_0_RealGrid_SSH() {
        loadGraphsMeasureTimeAndMemory_load(graphImplementationsToTest, Triple::getSubject,
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml");
    }

    /**
     * Due to copyright, data cannot be added to the repository.
     * Download https://www.entsoe.eu/Documents/CIM_documents/Grid_Model_CIM/ENTSO-E_Conformity_Assessment_Scheme_v3.0.zip
     * from https://www.entsoe.eu/digital/cim/cim-conformity-and-interoperability/     *
     */
    @Test
    public void ENTSO_E_Test_Configurations_v3_0_RealGrid_SV() {
        loadGraphsMeasureTimeAndMemory_load(graphImplementationsToTest, Triple::getSubject,
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml");
    }

    /**
     * Due to copyright, data cannot be added to the repository.
     * Download https://www.entsoe.eu/Documents/CIM_documents/Grid_Model_CIM/ENTSO-E_Conformity_Assessment_Scheme_v3.0.zip
     * from https://www.entsoe.eu/digital/cim/cim-conformity-and-interoperability/     *
     */
    @Test
    public void ENTSO_E_Test_Configurations_v3_0_RealGrid_TP() {
        loadGraphsMeasureTimeAndMemory_load(graphImplementationsToTest, Triple::getSubject,
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml");
    }

    private void loadGraphsMeasureTimeAndMemory_load(List<Pair<String, Supplier<Graph>>> graphVariantSuppliersWithNames, Function<Triple, Node> keyNodeResolver, String... graphUris) {
        final var triplesPerGraph = loadTriples(1, graphUris);
        //for(var numberOfTriples : Arrays.asList(50, 100, 250, 500, 1000, 2000, 5000, 10000, 25000, 50000, 75000, 100000, 150000)) {
        for(var numberOfTriples : Arrays.asList(1000, 2000, 5000, 10000, 25000, 50000, 75000, 100000, 110000, 120000, 130000, 140000, 150000, 160000, 170000, 180000, 190000, 200000)) {
        //for(var numberOfTriples : Arrays.asList(50, 100, 125, 150, 175, 200, 225, 250, 275, 300, 400, 500, 600, 700, 800, 900, 1000, 1200)) {
        //for(var numberOfTriples : Arrays.asList(6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 22, 23, 24, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100, 125, 150, 175, 200, 225, 250, 275, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000)) {
        //for(var numberOfTriples : Arrays.asList(290, 300, 310, 320, 330, 340, 350, 360, 370, 380, 390, 400, 410)) {
        //for(var numberOfTriples : Arrays.asList(6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)) {
            var randomTriple = selectRandomTriples(triplesPerGraph, numberOfTriples);

            var maps = new ArrayList<Supplier<Map<Integer, ArrayList<Triple>>>>();
            //maps.add(() -> new LeanHashMap<Integer, ArrayList<Triple>>(16, 2.5f));
            maps.add(() -> new HashMap<Integer, ArrayList<Triple>>());


            for (var supplier  : maps) {
                var stopAdditions = StopWatch.createStarted();
                stopAdditions.suspend();
                var stopValues = StopWatch.createStarted();
                stopValues.suspend();
                var stopKeys = StopWatch.createStarted();
                stopKeys.suspend();
                var stopGetByKey = StopWatch.createStarted();
                stopGetByKey.suspend();
                var stopDeletions = StopWatch.createStarted();
                stopDeletions.suspend();
                var mem=0.0;
                for(var i=0; i<1; i++) {
                    var memBeforeAdd = runGcAndGetUsedMemoryInMB();
                    var map = supplier.get();
                    stopAdditions.resume();
                    for (List<Triple> triples : randomTriple) {
                        triples.forEach(t -> {
                            var key = keyNodeResolver.apply(t).getIndexingValue().hashCode();
                            var list = map.get(key);
                            if(list == null) {
                                list = new ArrayList<>();
                                map.put(key, list);
                            }
                            list.add(t);
                        });
                    }
                    stopAdditions.suspend();
                    var memAfterAdd = runGcAndGetUsedMemoryInMB();
                    mem += (memAfterAdd-memBeforeAdd);
                    values: {
                        var size = 0;
                        stopValues.resume();
                        for (ArrayList<Triple> value : map.values()) {
                            size += value.size();
                        }
                        Assert.assertEquals(randomTriple.stream().mapToInt(List::size).sum(), size);
                        stopValues.suspend();
                    }
                    values: {
                        var size = 0;
                        stopValues.resume();
                        for (ArrayList<Triple> value : map.values()) {
                            size += value.size();
                        }
                        Assert.assertEquals(randomTriple.stream().mapToInt(List::size).sum(), size);
                        stopValues.suspend();
                    }
                    var keys = new ArrayList<Integer>();
                    stopKeys.resume();
                    for (Integer key : map.keySet()) {
                        keys.add(key);
                    }
                    stopKeys.suspend();
                    stream: {
                        final boolean[] concrete = {false};
                        stopGetByKey.resume();
                        var size = 0;
                        for (Integer key : keys) {
                            var list = map.get(key);
                            size += list.size();
                        }
                        stopGetByKey.suspend();
                        Assert.assertEquals(randomTriple.stream().mapToInt(List::size).sum(), size);
                    }
                    stopDeletions.resume();
                    for (Integer key : keys) {
                        map.remove(key);
                    }
                    stopDeletions.suspend();
                    Assert.assertTrue(map.isEmpty());
                }
                stopAdditions.stop();
                stopDeletions.stop();
                System.out.println(String.format("type: '%50s' \t with %6d triples - add: %s delete: %s collect values: %s collect keys: %s get by key: %s - combined: %s - mem: %5.3f MB",
                        supplier.get().getClass().getSimpleName(),
                        numberOfTriples,
                        stopAdditions.formatTime(),
                        stopDeletions.formatTime(),
                        stopValues.formatTime(),
                        stopKeys.formatTime(),
                        stopGetByKey.formatTime(),
                        DurationFormatUtils.formatDurationHMS(
                                (3*stopAdditions.getTime())
                                        + stopDeletions.getTime()
                                        + (10 *(stopValues.getTime()
                                        + stopKeys.getTime()
                                        + stopGetByKey.getTime()))),
                        mem));
            }
            System.out.println("");
        }
    }
}

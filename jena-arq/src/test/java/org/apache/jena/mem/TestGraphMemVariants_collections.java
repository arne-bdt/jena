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

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.sorted.experiment.CollectionUsingHashCodesBase;
import org.apache.jena.mem.sorted.experiment.SortedArrayList;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class TestGraphMemVariants_collections extends TestGraphMemVariantsBase {

    @Test
    public void pizza_owl_rdf() {
        loadGraphsMeasureTimeAndMemory_load(graphImplementationsToTest, 100,
                "./../jena-examples/src/main/resources/data/pizza.owl.rdf");
    }

    @Test
    public void cheeses_ttl() {
        loadGraphsMeasureTimeAndMemory_load(graphImplementationsToTest, 50,
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
        loadGraphsMeasureTimeAndMemory_load(graphImplementationsToTest, 1,
                "./../jena-examples/src/main/resources/data/BSBM_50000.ttl.gz");
    }

    /**
     * Due to copyright, data cannot be added to the repository.
     * Download https://www.entsoe.eu/Documents/CIM_documents/Grid_Model_CIM/ENTSO-E_Conformity_Assessment_Scheme_v3.0.zip
     * from https://www.entsoe.eu/digital/cim/cim-conformity-and-interoperability/     *
     */
    @Test
    public void ENTSO_E_Test_Configurations_v3_0_RealGrid_EQ_SSH_SV_and_TP() {
        loadGraphsMeasureTimeAndMemory_load(graphImplementationsToTest, 1,
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
        loadGraphsMeasureTimeAndMemory_load(graphImplementationsToTest, 1,
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml");
    }

    /**
     * Due to copyright, data cannot be added to the repository.
     * Download https://www.entsoe.eu/Documents/CIM_documents/Grid_Model_CIM/ENTSO-E_Conformity_Assessment_Scheme_v3.0.zip
     * from https://www.entsoe.eu/digital/cim/cim-conformity-and-interoperability/     *
     */
    @Test
    public void ENTSO_E_Test_Configurations_v3_0_RealGrid_SSH() {
        loadGraphsMeasureTimeAndMemory_load(graphImplementationsToTest, 1,
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml");
    }

    /**
     * Due to copyright, data cannot be added to the repository.
     * Download https://www.entsoe.eu/Documents/CIM_documents/Grid_Model_CIM/ENTSO-E_Conformity_Assessment_Scheme_v3.0.zip
     * from https://www.entsoe.eu/digital/cim/cim-conformity-and-interoperability/     *
     */
    @Test
    public void ENTSO_E_Test_Configurations_v3_0_RealGrid_SV() {
        loadGraphsMeasureTimeAndMemory_load(graphImplementationsToTest, 1,
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml");
    }

    /**
     * Due to copyright, data cannot be added to the repository.
     * Download https://www.entsoe.eu/Documents/CIM_documents/Grid_Model_CIM/ENTSO-E_Conformity_Assessment_Scheme_v3.0.zip
     * from https://www.entsoe.eu/digital/cim/cim-conformity-and-interoperability/     *
     */
    @Test
    public void ENTSO_E_Test_Configurations_v3_0_RealGrid_TP() {
        loadGraphsMeasureTimeAndMemory_load(graphImplementationsToTest, 1,
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml");
    }

    private void loadGraphsMeasureTimeAndMemory_load(List<Pair<String, Supplier<Graph>>> graphVariantSuppliersWithNames, int graphMultiplier, String... graphUris) {
        final var triplesPerGraph = loadTriples(graphMultiplier, graphUris);
        Comparator<Triple> comparator = Comparator.comparingInt((Triple t) -> t.getSubject().getIndexingValue().hashCode())
                .thenComparing(t -> t.getObject().getIndexingValue().hashCode())
                .thenComparing(t -> t.getPredicate().getIndexingValue().hashCode());
        //for(var numberOfTriples : Arrays.asList(50, 100, 250, 500, 1000, 2000, 5000, 10000, 25000, 50000, 75000, 100000, 150000)) {
        //for(var numberOfTriples : Arrays.asList(50, 100, 250, 500, 1000, 2000, 5000, 10000, 25000, 50000, 75000, 100000, 110000, 120000, 130000, 140000, 150000, 160000, 170000, 180000, 190000, 200000)) {
        for(var numberOfTriples : Arrays.asList(50, 100, 125, 150, 175, 200, 225, 250, 275, 300, 400, 500, 600, 700, 800, 900, 1000, 1200)) {
        //for(var numberOfTriples : Arrays.asList(290, 300, 310, 320, 330, 340, 350, 360, 370, 380, 390, 400, 410)) {
            var randomTriple = selectRandomTriples(triplesPerGraph, numberOfTriples);

            var collections = new ArrayList<Pair<Supplier<Collection<Triple>>, BiConsumer<Collection<Triple>, Triple>>>();
//            collections.add(Pair.of(() -> new ArrayList<>(2),
//                    (c, t) -> {
//                        c.add(t);
//                    }));
            collections.add(Pair.of(() ->new CollectionUsingHashCodesBase<Triple>(),
                    (c, t) -> {
                        c.add(t);
                    }));
            collections.add(Pair.of(() ->new SortedArrayList<Triple>(comparator, 2),
                    (c, t) -> {
                        c.add(t);
                    }));
            collections.add(Pair.of(() -> new HashSet<>(),
                    (c, t) -> {
                        c.add(t);
                    }));

            for (Pair<Supplier<Collection<Triple>>, BiConsumer<Collection<Triple>, Triple>> pair : collections) {
                var stopAdditions = StopWatch.createStarted();
                stopAdditions.suspend();
                var stopIterator = StopWatch.createStarted();
                stopIterator.suspend();
                var stopStreamIterator = StopWatch.createStarted();
                stopStreamIterator.suspend();
                var stopStream = StopWatch.createStarted();
                stopStream.suspend();
                var stopDeletions = StopWatch.createStarted();
                stopDeletions.suspend();
                for(var i=0; i<1000; i++) {
                    var collection = pair.getKey().get();
                    for (List<Triple> triples : randomTriple) {
                        stopAdditions.resume();
                        triples.forEach(t -> pair.getValue().accept(collection, t));
                        stopAdditions.suspend();
                    }
                    Assert.assertEquals(randomTriple.stream().mapToInt(List::size).sum(), collection.size());
                    streamIterator: {
                        var concrete = false;
                        stopStreamIterator.resume();
                        var it = collection.stream().iterator();
                        while(it.hasNext()){
                            concrete = it.next().isConcrete();
                        }
                        stopStreamIterator.suspend();
                    }
                    iterator: {
                        var concrete = false;
                        stopIterator.resume();
                        var it = collection.iterator();
                        while(it.hasNext()){
                            concrete = it.next().isConcrete();
                        }
                        stopIterator.suspend();
                    }
                    stream: {
                        final boolean[] concrete = {false};
                        stopStream.resume();
                        collection.stream().forEach(t -> { concrete[0] = t.isConcrete(); });
                        stopStream.suspend();
                    }
                    for (List<Triple> triples : randomTriple) {
                        stopDeletions.resume();
                        var mid = (int)triples.size() /2;
                        for(int k=mid; k<triples.size(); k++) {
                            var t = triples.get(k);
                            t = Triple.create(t.getSubject(), t.getPredicate(), t.getObject()); /*important to avoid identity equality*/
                            collection.remove(t);
                        }
                        for(int k=mid-1; k>=0; k--) {
                            var t = triples.get(k);
                            t = Triple.create(t.getSubject(), t.getPredicate(), t.getObject()); /*important to avoid identity equality*/
                            collection.remove(t);
                        }
                        stopDeletions.suspend();
                    }
                    //Assert.assertTrue(collection.isEmpty());
                }
                stopAdditions.stop();
                stopDeletions.stop();
                System.out.println(String.format("type: '%50s' \t with %6d triples - add: %s delete: %s iterator: %s stream.iterator: %s stream: %s - combined: %s",
                        pair.getKey().get().getClass().getSimpleName(),
                        numberOfTriples,
                        stopAdditions.formatTime(),
                        stopDeletions.formatTime(),
                        stopIterator.formatTime(),
                        stopStreamIterator.formatTime(),
                        stopStream.formatTime(),
                        DurationFormatUtils.formatDurationHMS(
                                stopAdditions.getTime()
                                        + stopDeletions.getTime()
                                        + (10 *(stopIterator.getTime()
                                        + stopStreamIterator.getTime()
                                        + stopStream.getTime())))));
            }
            System.out.println("");
        }
    }
}

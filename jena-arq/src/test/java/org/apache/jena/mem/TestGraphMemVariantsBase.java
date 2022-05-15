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
import org.apache.jena.datatypes.xsd.impl.XSDDouble;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.hash.GraphMemHash;
import org.apache.jena.mem.hash_no_entry.GraphMemHashNoEntries;
import org.apache.jena.mem.hybrid.GraphMemHybridMaps;
import org.apache.jena.mem.hybrid.HybridTripleMap;
import org.apache.jena.mem.simple.GraphMemSimple;
import org.apache.jena.mem.sorted.GraphMemUsingHashMapSorted;
import org.apache.jena.mem.sorted.experiment.GraphMemUsingHashMapSortedExperiment;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public abstract class TestGraphMemVariantsBase {



    protected List<Pair<String, Supplier<Graph>>> graphImplementationsToTest = List.of(
            Pair.of("GraphMemUsingHashMapSortedExperiment", () -> new GraphMemUsingHashMapSortedExperiment()),
            Pair.of("GraphMemUsingHashMapSortedExperiment", () -> new GraphMemUsingHashMapSortedExperiment()),
            //Pair.of("GraphMemUsingHashMapSortedExperiment", () -> new GraphMemUsingHashMapSortedExperiment()),
            //Pair.of("GraphMemUsingHashMapSortedExperiment", () -> new GraphMemUsingHashMapSortedExperiment()),
            Pair.of("GraphMemUsingHashMapSorted", () -> new GraphMemUsingHashMapSorted()),
            Pair.of("GraphMemUsingHashMapSorted", () -> new GraphMemUsingHashMapSorted())
            //Pair.of("GraphMemUsingHashMapSortedExperiment", () -> new GraphMemUsingHashMapSortedExperiment()),
            //Pair.of("GraphMemUsingHashMapSortedExperiment", () -> new GraphMemUsingHashMapSortedExperiment()),
//            Pair.of("GraphMemUsingHashMap", () -> new GraphMemUsingHashMap()),
//            Pair.of("GraphMemUsingHashMap", () -> new GraphMemUsingHashMap()),
            //Pair.of("GraphMem", () -> new GraphMem()),
            //Pair.of("GraphMem", () -> new GraphMem()),
            //Pair.of("GraphMem", () -> new GraphMem())
            //Pair.of("GraphMemHash", () -> new GraphMemHash()),
            //Pair.of("GraphMemHashNoEntries", () -> new GraphMemHashNoEntries()),
            //Pair.of("GraphMemHashNoEntries", () -> new GraphMemHashNoEntries())
            //Pair.of("GraphMem", () -> new GraphMem()),
            //Pair.of("GraphMemHash", () -> new GraphMemHash())
            //Pair.of("GraphMem", () -> new GraphMem()),
            //Pair.of("GraphMem", () -> new GraphMem()),
            //Pair.of("GraphMemSimple", () -> new GraphMemSimple()),
            //Pair.of("GraphMemUsingHashMapSorted", () -> new GraphMemUsingHashMapSorted()),
            //Pair.of("GraphMemUsingHashMapSortedExperiment", () -> new GraphMemUsingHashMapSortedExperiment())
            //Pair.of("GraphMemUsingHashMap", () -> new GraphMemUsingHashMap()),

    );

    protected static Random random = new Random();

    protected static int count(final ExtendedIterator extendedIterator) {
        var count = 0;
        while(extendedIterator.hasNext()) {
            extendedIterator.next();
            count++;
        }
        extendedIterator.close();
        return count;
    }

    protected static List<List<Triple>> loadTriples(int graphMultiplier, final String... graphUris)
    {
        var triplesPerGraph = new ArrayList<List<Triple>>(graphUris.length*graphMultiplier);
        {
            var memoryBefore = runGcAndGetUsedMemoryInMB();
            var stopwatch = StopWatch.createStarted();
            for(int i=0; i<graphMultiplier; i++) {
                for (String graphUri : graphUris) {
                    var loadingGraph = new GraphMemWithArrayListOnly();
                    RDFDataMgr.read(loadingGraph, graphUri);
                    var tripleList = loadingGraph.triples;
                    triplesPerGraph.add(tripleList);
                    System.out.println("graph uri: '" + graphUri + "' triples: " + tripleList.size());
                }
            }
            stopwatch.stop();
            var memoryAfter = runGcAndGetUsedMemoryInMB();
            System.out.println("Size of triples in ArrayList: " + (int)(memoryAfter-memoryBefore) + " MB - time to load: " + stopwatch.formatTime());
        }
        return triplesPerGraph;
    }

    protected static List<List<Triple>> selectRandomTriples(final List<List<Triple>> triplesPerGraph, final int numberOfRandomTriplesToSelect) {
        var randomlySelectedTriples = new ArrayList<List<Triple>>(triplesPerGraph.size());
        /*find random triples*/
        for (List<Triple> triples : triplesPerGraph) {
            if(numberOfRandomTriplesToSelect < 1) {
                randomlySelectedTriples.add(Collections.emptyList());
                continue;
            }
            var randomlySelectedInGraph = new HashSet<Triple>(numberOfRandomTriplesToSelect);
            do {
                var intStream = random.ints(numberOfRandomTriplesToSelect, 0, triples.size());
                var i = intStream.iterator();
                while(i.hasNext()) {
                    randomlySelectedInGraph.add(triples.get(i.next()));
                    if(randomlySelectedInGraph.size() == numberOfRandomTriplesToSelect) {
                        break;
                    }
                }
            } while (randomlySelectedInGraph.size() <= numberOfRandomTriplesToSelect);
            randomlySelectedTriples.add(randomlySelectedInGraph.stream().collect(Collectors.toList()));
        }
        return randomlySelectedTriples;
    }

    protected static List<Triple> generateRandomTriples(final int numberOfRandomTriples) {
        var randomTriples = new ArrayList<Triple>(numberOfRandomTriples);
        for(int i=0; i<numberOfRandomTriples; i++) {
            randomTriples.add(new Triple(NodeFactory.createURI(Long.toString(random.nextLong())), NodeFactory.createURI(Long.toString(random.nextLong())), NodeFactory.createLiteralByValue(random.nextDouble(), XSDDouble.XSDdecimal)));
        }
        return randomTriples;
    }

    protected static List<Graph> fillGraphs(final Supplier<Graph> graphSupplier, final List<List<Triple>> triplesPerGraph) {
        var graphs = new ArrayList<Graph>(triplesPerGraph.size());
        {
            var memoryBefore = runGcAndGetUsedMemoryInMB();
            var stopwatch = StopWatch.createStarted();
            for (List<Triple> triples : triplesPerGraph) {
                var graph = graphSupplier.get();
                triples.forEach(graph::add);
                graphs.add(graph);
            }
            stopwatch.stop();
            var memoryAfter = runGcAndGetUsedMemoryInMB();
            System.out.println("graphs: " + graphs.size() + " time to fill graphs: " + stopwatch.formatTime() + " additional memory: " + (memoryAfter - memoryBefore) + " MB");
        }
        return graphs;
    }

    protected static double runGcAndGetUsedMemoryInMB() {
        System.runFinalization();
        System.gc();
        Runtime.getRuntime().runFinalization();
        Runtime.getRuntime().gc();
        return BigDecimal.valueOf(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory()).divide(BigDecimal.valueOf(1024l)).divide(BigDecimal.valueOf(1024l)).doubleValue();
    }

}

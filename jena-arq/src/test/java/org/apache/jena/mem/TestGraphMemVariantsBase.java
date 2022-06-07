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
import org.apache.jena.arq.junit.riot.VocabLangRDF;
import org.apache.jena.datatypes.BaseDatatype;
import org.apache.jena.datatypes.DatatypeFormatException;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.datatypes.xsd.impl.XSDDouble;
import org.apache.jena.datatypes.xsd.impl.XSDFloat;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.LiteralLabel;
import org.apache.jena.mem2.*;
import org.apache.jena.rdfs.assembler.VocabRDFS;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Supplier;

public abstract class TestGraphMemVariantsBase {



    protected List<Pair<String, Supplier<Graph>>> graphImplementationsToTest = List.of(

//            Pair.of("GraphMem", () -> new GraphMem()),
//
//            Pair.of("GraphMem2", () -> new GraphMem2()),
//
//            Pair.of("GraphMem3", () -> new GraphMem3()),

            //Pair.of("GraphMem2Fast", () -> new GraphMem2Fast()),

            Pair.of("GraphMem4Fast", () -> new GraphMem4Fast()),

            Pair.of("GraphMem3Fast", () -> new GraphMem3Fast())

            //Pair.of("GraphMem2EqualsOk", () -> new GraphMem2EqualsOk())

            //Pair.of("GraphMem3Fast", () -> new GraphMem3Fast())

            //Pair.of("GraphMemUsingHashMap", () -> new GraphMemUsingHashMap())
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
                    var triples = TypedTripleReader.read(graphUri);
                    triplesPerGraph.add(triples);
                    System.out.println("graph uri: '" + graphUri + "' triples: " + triples.size());
                }
            }
            stopwatch.stop();
            var memoryAfter = runGcAndGetUsedMemoryInMB();
            System.out.println("Size of triples in ArrayList: " + (int)(memoryAfter-memoryBefore) + " MB - time to load: " + stopwatch.formatTime());
        }
        return triplesPerGraph;
    }

    protected static List<List<Triple>> selectRandomTriples(final List<List<Triple>> triplesPerGraph, int numberOfRandomTriplesToSelect) {
        var randomlySelectedTriples = new ArrayList<List<Triple>>(triplesPerGraph.size());
        /*find random triples*/
        for (List<Triple> triples : triplesPerGraph) {
            if(numberOfRandomTriplesToSelect < 1) {
                randomlySelectedTriples.add(Collections.emptyList());
                continue;
            }
            var list = new ArrayList<>(triples);
            triples.forEach(t -> list.add(Triple.create(t.getSubject(), t.getPredicate(), t.getObject())));
            Collections.shuffle(list);
            randomlySelectedTriples.add(list.subList(0, Math.min(numberOfRandomTriplesToSelect, list.size())));
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
            System.out.println(String.format("graphs: %d time to fill graphs: %s additional memory: %5.3f MB",
                    graphs.size(),
                    stopwatch.formatTime(),
                    (memoryAfter - memoryBefore)));
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


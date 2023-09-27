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

import org.apache.commons.lang3.time.StopWatch;
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
import org.junit.Ignore;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

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
//            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml",
            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml",
//            "../testing/BSBM/bsbm-1m.nt.gz",
//            "../testing/BSBM/bsbm-5m.nt.gz",
//            "../testing/BSBM/bsbm-25m.nt.gz",
    })
    public String param0_GraphUri;

    @Param({
            "GraphMem (current)",
            "GraphMem2Fast (current)",
            "GraphMem2Legacy (current)",
            "GraphMem2Roaring (current)",
//              "GraphMem (Jena 4.8.0)",
    })
    public String param1_GraphImplementation;

    private Graph sutCurrent;
    private org.apache.shadedJena480.graph.Graph sut480;
    private List<org.apache.shadedJena480.graph.Triple> triples480;

    private List<Triple> triples;

    /**
     * This method is used to get the memory consumption of the current JVM.
     *
     * @return the memory consumption in MB
     */
    private static double runGcAndGetUsedMemoryInMB() {
        System.runFinalization();
        System.gc();
        Runtime.getRuntime().runFinalization();
        Runtime.getRuntime().gc();
        return BigDecimal.valueOf(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()).divide(BigDecimal.valueOf(1024L)).divide(BigDecimal.valueOf(1024L)).doubleValue();
    }

    @Test
    public void testSingleUpdates() {
        var trialContext = new Context("GraphMem2Fast (current)");
        this.sutCurrent = Releases.current.createGraph(trialContext.getGraphClass());

        var triples = TripleReaderReadingCGMES_2_4_15_WithTypedLiterals
                .read("C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml");

        triples.forEach(this.sutCurrent::add);

        var doubleTriples = new ArrayList<Triple>();
        for (var t : triples) {
            if (t.getObject().isLiteral() && t.getObject().getLiteralDatatype() == XSDDatatype.XSDfloat) {
                doubleTriples.add(t);
            }
        }
        var random = new Random(4721);
        /* Shuffle is important because the order might play a role. We want to test the performance of the
           contains method regardless of the order */
        Collections.shuffle(doubleTriples, random);
        System.out.println("Found " + doubleTriples.size() + " triples with double literals");
        final int iterations = 2;
        final var stopwatch = StopWatch.createStarted();
        for (int i = 0; i < iterations; i++) {
            for (var t : doubleTriples) {
                var oldTriple = this.sutCurrent.find(t.getSubject(), t.getPredicate(), Node.ANY).next();
                var oldValue = (float) oldTriple.getObject().getIndexingValue();
                this.sutCurrent.delete(oldTriple);
                this.sutCurrent.add(Triple.create(t.getSubject(), t.getPredicate(), NodeFactory.createLiteralByValue((oldValue + 1.0f), oldTriple.getObject().getLiteralDatatype())));
            }
        }
        stopwatch.stop();
        System.out.println("Time for " + doubleTriples.size() * iterations + " single updates: " + stopwatch.formatTime() + " on graph with " + this.sutCurrent.size() + " triples");
    }

    @Ignore("Don´t do it this way, it is slower than the single updates")
    public void testBulkUpdates() {
        var trialContext = new Context("GraphMem2Fast (current)");
        this.sutCurrent = Releases.current.createGraph(trialContext.getGraphClass());

        var triples = TripleReaderReadingCGMES_2_4_15_WithTypedLiterals
                .read("C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml");

        triples.forEach(this.sutCurrent::add);

        var doubleTriples = new ArrayList<Triple>();
        for (var t : triples) {
            if (t.getObject().isLiteral() && t.getObject().getLiteralDatatype() == XSDDatatype.XSDfloat) {
                doubleTriples.add(t);
            }
        }
        var random = new Random(4721);
        /* Shuffle is important because the order might play a role. We want to test the performance of the
           contains method regardless of the order */
        Collections.shuffle(doubleTriples, random);
        System.out.println("Found " + doubleTriples.size() + " triples with double literals");
        final int iterations = 2;
        final var stopwatch = StopWatch.createStarted();
        for (int i = 0; i < iterations; i++) {
            for (var t : doubleTriples) {
                var oldTriple = this.sutCurrent.find(t.getSubject(), t.getPredicate(), Node.ANY).next();
                this.sutCurrent.delete(oldTriple);
            }
            for (var t : doubleTriples) {
                this.sutCurrent.add(Triple.create(t.getSubject(), t.getPredicate(), NodeFactory.createLiteralByValue(random.nextFloat(), t.getObject().getLiteralDatatype())));
            }
        }
        stopwatch.stop();
        System.out.println("Time for " + doubleTriples.size() * iterations + "  deletions and inserts in " + iterations + " batches: " + stopwatch.formatTime() + " on graph with " + this.sutCurrent.size() + " triples");
    }

    @Benchmark
    public Graph manySingleUpdates() {
        var doubleTriples = new ArrayList<Triple>();
        for (var t : triples) {
            if (t.getObject().isLiteral() && t.getObject().getLiteralDatatype() == XSDDatatype.XSDfloat) {
                doubleTriples.add(t);
            }
        }
        /* Shuffle is important because the order might play a role. We want to test the performance of the
           contains method regardless of the order */
        Collections.shuffle(doubleTriples, new Random(4721));
        System.out.println();
        System.out.println("Found " + doubleTriples.size() + " triples with double literals");
        for (int i = 0; i < 10; i++) {
            for (var t : doubleTriples) {
                var oldTriple = this.sutCurrent.find(t.getSubject(), t.getPredicate(), Node.ANY).next();
                var oldValue = (float) oldTriple.getObject().getIndexingValue();
                this.sutCurrent.delete(oldTriple);
                this.sutCurrent.add(Triple.create(t.getSubject(), t.getPredicate(), NodeFactory.createLiteralByValue((oldValue + 1.0f), oldTriple.getObject().getLiteralDatatype())));
            }
        }
        return sutCurrent;
    }

    @Ignore("Don´t do it this way, it is slower than the single updates")
    public Graph bulkDeleteAndAdd() {
        var doubleTriples = new ArrayList<Triple>();
        for (var t : triples) {
            if (t.getObject().isLiteral() && t.getObject().getLiteralDatatype() == XSDDatatype.XSDfloat) {
                doubleTriples.add(t);
            }
        }
        /* Shuffle is important because the order might play a role. We want to test the performance of the
           contains method regardless of the order */

        var random = new Random(4721);
        Collections.shuffle(doubleTriples, random);
        System.out.println();
        System.out.println("Found " + doubleTriples.size() + " triples with double literals");
        for (int i = 0; i < 10; i++) {
            for (var t : doubleTriples) {
                var oldTriple = this.sutCurrent.find(t.getSubject(), t.getPredicate(), Node.ANY).next();
                this.sutCurrent.delete(oldTriple);
            }
            for (var t : doubleTriples) {
                this.sutCurrent.add(Triple.create(t.getSubject(), t.getPredicate(), NodeFactory.createLiteralByValue(random.nextFloat(), t.getObject().getLiteralDatatype())));
            }
        }
        return sutCurrent;
    }

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        var trialContext = new Context(param1_GraphImplementation);
        switch (trialContext.getJenaVersion()) {
            case CURRENT: {
                this.sutCurrent = Releases.current.createGraph(trialContext.getGraphClass());

                this.triples = TripleReaderReadingCGMES_2_4_15_WithTypedLiterals
                        .read(param0_GraphUri);

                triples.forEach(this.sutCurrent::add);
            }
            break;
            case JENA_4_8_0: {
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
                .measurementIterations(15)
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }
}

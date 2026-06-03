/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *   SPDX-License-Identifier: Apache-2.0
 */

package org.apache.jena.mem.dataset;

import java.util.Iterator;
import java.util.List;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.jmh.JmhDefaultOptions;
import org.apache.jena.mem.graph.helper.Releases;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.system.Txn;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * Measures the per-row {@link Triple}/{@link Quad} allocation that happens at the
 * {@link Graph} &harr; {@link DatasetGraph} view boundary (discussion #3967).
 * <p>
 * The interesting metric here is <b>not</b> {@code AverageTime} but the GC profiler's
 * {@code gc.alloc.rate.norm} = <i>bytes allocated per benchmark op</i>. Each op iterates
 * (or probes) all {@code N} stored statements once, so {@code gc.alloc.rate.norm / N}
 * is the allocation attributable to <i>one row crossing the view boundary</i>
 * (minus a small, constant per-op transaction overhead that is identical across all
 * methods and SUTs, so it cancels out when comparing).
 * <p>
 * Two storage models are compared on the same data, loaded into both the default graph
 * and one named graph:
 * <ul>
 *   <li><b>TxnMem</b> = {@code DatasetGraphInMemory} ({@link DatasetGraphFactory#createTxnMem()}):
 *       hybrid storage — default graph in a {@code TripleTable}, named graphs in a
 *       quad-indexed {@code QuadTable}.</li>
 *   <li><b>General</b> = {@code DatasetGraphMapLink} ({@link DatasetGraphFactory#createGeneral()}):
 *       triple storage — a {@code Map<Node,Graph>}; {@code getDefaultGraph()}/{@code getGraph()}
 *       hand back the stored {@link Graph} directly, so the triple API does no conversion.</li>
 * </ul>
 *
 * Expected shape of the results (why neither model is "free" at the boundary):
 * <pre>
 *                                         TxnMem (quad/hybrid)     General (triple)
 *  defaultGraph_find_triples   (Graph API) Triple-&gt;Quad-&gt;Triple     stored Triple
 *                                          ~2 allocs/row            ~0 allocs/row
 *  namedGraph_find_triples     (Graph API) Quad-&gt;Triple             stored Triple
 *                                          ~1 alloc/row             ~0 allocs/row
 *  namedGraph_find_quads    (DatasetGraph) stored Quad             Triple-&gt;Quad
 *                                          ~0 allocs/row            ~1 alloc/row
 *  defaultGraph_contains       (Graph API) Triple-&gt;Quad-&gt;Triple     stored Triple
 *                                          ~2 allocs/probe          ~0 allocs/probe
 * </pre>
 *
 * The {@code defaultGraph_find_triples} row on TxnMem is the headline: data that is
 * <i>stored</i> as a {@link Triple} and <i>consumed</i> as a {@link Triple} still
 * allocates a {@link Quad} (32 B) and a throwaway {@link Triple} (24 B) per row, and
 * the stored {@link Triple} is never handed out (so no reference reuse is possible today).
 */
@State(Scope.Benchmark)
public class TestDatasetGraphViewConversion {

    @Param({
            "../testing/cheeses-0.1.ttl",
            "../testing/pizza.owl.rdf",
            "../testing/data.nt.gz",
    })
    public String param0_GraphUri;

    @Param({
            "TxnMem",   // DatasetGraphInMemory  - quad/hybrid storage
            "General",  // DatasetGraphMapLink   - triple storage
            // "TxnMemCow",  // DatasetGraphInMemoryCowTxn - enable to profile the COW variant
            // "TxnMemMvcc", // DatasetGraphInMemoryMvccTxn - enable to profile the MVCC variant
    })
    public String param1_DatasetImplementation;

    private static final Node graphName = NodeFactory.createURI("http://example/benchmark/g");

    private DatasetGraph dsg;
    /** Cached Graph views (a GraphView for TxnMem, the stored Graph for General). */
    private Graph defaultGraphView;
    private Graph namedGraphView;
    /** The stored statements, used as present-probes for contains and to report N. */
    private List<Triple> triples;

    /**
     * Default graph, read through the {@link Graph} (triple) API.
     * TxnMem: TripleTable -&gt; wrap to Quad -&gt; GraphView unwraps to Triple (2 allocs/row).
     * General: returns the stored Triple (0 allocs/row).
     */
    @Benchmark
    public long defaultGraph_find_triples() {
        return Txn.calculateRead(dsg, () -> count(defaultGraphView.find(Node.ANY, Node.ANY, Node.ANY)));
    }

    /**
     * Named graph, read through the {@link Graph} (triple) API.
     * TxnMem: QuadTable -&gt; GraphView unwraps Quad to Triple (1 alloc/row).
     * General: returns the stored Triple (0 allocs/row).
     */
    @Benchmark
    public long namedGraph_find_triples() {
        return Txn.calculateRead(dsg, () -> count(namedGraphView.find(Node.ANY, Node.ANY, Node.ANY)));
    }

    /**
     * Named graph, read through the {@link DatasetGraph} (quad) API.
     * TxnMem: returns the stored Quad (0 allocs/row).
     * General: graph.find returns Triple -&gt; wrap to Quad (1 alloc/row).
     */
    @Benchmark
    public long namedGraph_find_quads() {
        return Txn.calculateRead(dsg, () -> count(dsg.find(graphName, Node.ANY, Node.ANY, Node.ANY)));
    }

    /**
     * Default graph membership test through the {@link Graph} (triple) API.
     * Same conversion chain as {@link #defaultGraph_find_triples()} but per probe, so it
     * shows that even a boolean {@code contains} allocates on the quad/hybrid store today.
     */
    @Benchmark
    public boolean defaultGraph_contains() {
        return Txn.calculateRead(dsg, () -> {
            boolean acc = false;
            for (Triple t : triples)
                acc ^= defaultGraphView.contains(t);
            return acc;
        });
    }

    private static long count(Iterator<?> it) {
        long c = 0;
        while (it.hasNext()) {
            it.next();   // force materialization of the converted Triple/Quad
            c++;
        }
        return c;
    }

    @Setup(Level.Trial)
    public void setup() {
        dsg = createDataset(param1_DatasetImplementation);
        triples = Releases.current.readTriples(param0_GraphUri);

        // Same data in the default graph and in one named graph.
        Txn.executeWrite(dsg, () -> {
            for (Triple t : triples) {
                Node s = t.getSubject(), p = t.getPredicate(), o = t.getObject();
                dsg.add(Quad.create(Quad.defaultGraphIRI, s, p, o)); // -> default graph storage
                dsg.add(Quad.create(graphName, s, p, o));            // -> named graph storage
            }
        });

        // Cache the view handles so we measure find/contains, not view construction.
        defaultGraphView = dsg.getDefaultGraph();
        namedGraphView = dsg.getGraph(graphName);

        System.out.printf("[setup] impl=%s file=%s N=%d%n",
                          param1_DatasetImplementation, param0_GraphUri, triples.size());
    }

    private static DatasetGraph createDataset(String impl) {
        switch (impl) {
            case "TxnMem":    return DatasetGraphFactory.createTxnMem();
            case "General":   return DatasetGraphFactory.createGeneral();
            case "TxnMemCow": return DatasetGraphFactory.createTxnMemCow();
            case "TxnMemMvcc":return DatasetGraphFactory.createTxnMemMvcc();
            default: throw new IllegalArgumentException("Unknown dataset implementation: " + impl);
        }
    }

    @Test
    public void benchmark() throws Exception {
        // All knobs are overridable from the command line for a quick run, e.g.:
        //   mvn test -Dtest=TestDatasetGraphViewConversion \
        //       -Djmh.file=../testing/cheeses-0.1.ttl -Djmh.time=1 -Djmh.wi=2 -Djmh.mi=3
        var builder = JmhDefaultOptions.getDefaults(this.getClass())
                // Allocation per op is the point of this benchmark: report gc.alloc.rate.norm.
                .addProfiler(GCProfiler.class)
                // Keep a full run feasible; bump these up for publication-quality numbers.
                .warmupIterations(Integer.getInteger("jmh.wi", 3))
                .measurementIterations(Integer.getInteger("jmh.mi", 5));
        var seconds = Integer.getInteger("jmh.time", 0);
        if (seconds > 0) {
            builder.warmupTime(TimeValue.seconds(seconds)).measurementTime(TimeValue.seconds(seconds));
        }
        var onlyFile = System.getProperty("jmh.file");
        if (onlyFile != null)
            builder.param("param0_GraphUri", onlyFile);
        var onlyImpl = System.getProperty("jmh.impl");
        if (onlyImpl != null)
            builder.param("param1_DatasetImplementation", onlyImpl);
        var results = new Runner(builder.build()).run();
        Assert.assertNotNull(results);
    }
}

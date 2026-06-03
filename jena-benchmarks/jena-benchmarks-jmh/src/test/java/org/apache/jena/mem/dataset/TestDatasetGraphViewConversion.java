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
import org.openjdk.jmh.infra.Blackhole;
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
 * Storage models compared on the same data, loaded into both the default graph and one
 * named graph:
 * <ul>
 *   <li><b>TxnMem</b> = {@code DatasetGraphInMemory} ({@link DatasetGraphFactory#createTxnMem()}):
 *       hybrid storage — default graph in a {@code TripleTable}, named graphs in a
 *       quad-indexed {@code QuadTable}; {@code find} is {@code Stream}-based.</li>
 *   <li><b>General</b> = {@code DatasetGraphMapLink} ({@link DatasetGraphFactory#createGeneral()}):
 *       triple storage — a {@code Map<Node,Graph>}; {@code getDefaultGraph()}/{@code getGraph()}
 *       hand back the stored {@link Graph} directly, so the triple API does no conversion.</li>
 *   <li><b>TxnMemCow</b> / <b>TxnMemMvcc</b>: the copy-on-write / MVCC in-memory variants.</li>
 * </ul>
 *
 * <h2>Important: the conversion is a non-escaping temporary the JIT may scalar-replace</h2>
 * The conversion creates one {@link Triple} or {@link Quad} per row. When the row is dropped
 * ({@code param2_Consumption=count}) that temporary does not escape, so C2 escape analysis
 * <em>may</em> scalar-replace it and {@code gc.alloc.rate.norm} drops to ~0 — measuring the
 * conversion away. Whether this happens is JVM/scale/data dependent (observed eliding at ~6.75M
 * rows on one JVM, but not at ~2M on another). The {@code consume} mode (default) hands every row
 * to a {@code Blackhole} so it escapes, the way a real caller (binding, collection, SPARQL
 * solution) would retain it; it is the conservative choice that never under-reports. Read the
 * <b>delta between the triple API and the quad API on the same store</b> rather than absolute
 * numbers: it is byte-exact — one {@code Quad} = 32 B, one {@code Triple} = 24 B,
 * {@code Triple->Quad->Triple} = 56 B. (Absolute per-row numbers also include the underlying
 * graph's own iteration cost, which itself varies with data and size.)
 * <p>
 * Two costs are tangled here and should be read separately:
 * <ol>
 *   <li>the <b>find-pipeline</b> allocation of the store itself (huge for TxnMem's {@code Stream}
 *       path, ~0 for the lean stores), independent of the Quad/Triple question; and</li>
 *   <li>the <b>conversion</b> object at the view boundary, which only matters once the find
 *       pipeline is lean enough not to dwarf it (i.e. General / Cow / Mvcc, not stock TxnMem).</li>
 * </ol>
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
            "TxnMem",     // DatasetGraphInMemory       - quad/hybrid storage, Stream-based find
            "General",    // DatasetGraphMapLink        - triple storage, native graph iterators
            "TxnMemCow",  // DatasetGraphInMemoryCowTxn - copy-on-write variant
            "TxnMemMvcc", // DatasetGraphInMemoryMvccTxn- MVCC variant
    })
    public String param1_DatasetImplementation;

    /**
     * How the iterated row is consumed.
     * <ul>
     *   <li>{@code consume} - hand each row to a {@link Blackhole} so it <i>escapes</i>; this is the
     *       realistic case (a caller keeps the row) and is the only way to measure the conversion,
     *       because otherwise C2 escape analysis scalar-replaces the converted {@link Triple}/{@link Quad}.</li>
     *   <li>{@code count} - just advance the iterator and drop the row; on a hot loop the JIT can prove
     *       the converted object never escapes and elides the allocation entirely. Useful to demonstrate
     *       that escape-analysis effect, not to measure the conversion.</li>
     * </ul>
     */
    @Param({ "consume", "count" })
    public String param2_Consumption;

    private static final Node graphName = NodeFactory.createURI("http://example/benchmark/g");

    private DatasetGraph dsg;
    /** Cached Graph views (a GraphView for TxnMem, the stored Graph for General). */
    private Graph defaultGraphView;
    private Graph namedGraphView;
    /** The stored statements, used as present-probes for contains and to report N. */
    private List<Triple> triples;

    /**
     * Default graph, read through the {@link Graph} (triple) API.
     * TxnMem: TripleTable -&gt; wrap to Quad -&gt; GraphView unwraps to Triple.
     * General: returns the stored Triple (no conversion).
     */
    @Benchmark
    public long defaultGraph_find_triples(Blackhole bh) {
        return Txn.calculateRead(dsg, () -> read(defaultGraphView.find(Node.ANY, Node.ANY, Node.ANY), bh));
    }

    /**
     * Named graph, read through the {@link Graph} (triple) API.
     * TxnMem: QuadTable -&gt; GraphView unwraps Quad to Triple.
     * General: returns the stored Triple (no conversion).
     */
    @Benchmark
    public long namedGraph_find_triples(Blackhole bh) {
        return Txn.calculateRead(dsg, () -> read(namedGraphView.find(Node.ANY, Node.ANY, Node.ANY), bh));
    }

    /**
     * Named graph, read through the {@link DatasetGraph} (quad) API.
     * TxnMem: returns the stored Quad (no conversion).
     * General: graph.find returns Triple -&gt; wrap to Quad.
     */
    @Benchmark
    public long namedGraph_find_quads(Blackhole bh) {
        return Txn.calculateRead(dsg, () -> read(dsg.find(graphName, Node.ANY, Node.ANY, Node.ANY), bh));
    }

    /**
     * Default graph membership test through the {@link Graph} (triple) API.
     * Same conversion chain as {@link #defaultGraph_find_triples} but per probe. The boolean
     * result escapes, so (unlike a drop-the-row scan) the per-probe find pipeline is measured.
     * Independent of {@code param2_Consumption}.
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

    /** Iterate, optionally letting each converted row escape into the Blackhole (see param2_Consumption). */
    private long read(Iterator<?> it, Blackhole bh) {
        final boolean consume = "consume".equals(param2_Consumption);
        long c = 0;
        while (it.hasNext()) {
            Object row = it.next();
            if (consume)
                bh.consume(row);   // hard escape point: prevents EA from scalar-replacing the Triple/Quad
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
        // Default to the realistic "consume" mode so a normal run isn't doubled.
        // Use -Djmh.consume=count, or -Djmh.consume=both to compare and see the escape-analysis effect.
        var consume = System.getProperty("jmh.consume", "consume");
        if (!"both".equals(consume))
            builder.param("param2_Consumption", consume);
        var results = new Runner(builder.build()).run();
        Assert.assertNotNull(results);
    }
}

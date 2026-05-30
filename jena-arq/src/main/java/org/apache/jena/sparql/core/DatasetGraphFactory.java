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

package org.apache.jena.sparql.core;

import java.util.Iterator;
import java.util.Objects;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.core.mem.DatasetGraphInMemory;
import org.apache.jena.sparql.core.mem.DatasetGraphInMemoryCowTxn;
import org.apache.jena.sparql.core.mem.DatasetGraphInMemoryMvccTxn;
import org.apache.jena.sparql.core.mem.GraphMemIndexedSetCowTxn;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.jena.sys.JenaSystem;

public class DatasetGraphFactory
{
    static { JenaSystem.init(); }

    /** Create an in-memory {@link Dataset}.
     * <p>
     * See also {@link #createTxnMem()} for a transactional dataset.
     * <p>
     * This implementation copies models when {@link Dataset#addNamedModel(String, Model)} is called.
     * <p>
     * This implementation provides "best effort" transactions; it only provides MRSW locking.
     * Use {@link #createTxnMem} for a proper in-memory transactional {@code DatasetGraph}.
     *
     * @see #createTxnMem
     */
    public static DatasetGraph create() {
        return new DatasetGraphMap();
    }

    /**
     * Create an in-memory, transactional {@link Dataset}.
     * <p>
     * This fully supports transactions, including abort to roll-back changes.
     * It provides "autocommit" if operations are performed
     * outside a transaction but with a performance impact
     * (the implementation adds a begin/commit around each add or delete
     * so overheads can accumulate).
     *
     * @return a transactional, in-memory, modifiable DatasetGraph
     * @see #createTxnMemCow()
     */
    public static DatasetGraph createTxnMem() { return new DatasetGraphInMemory(); }

    /**
     * Create an in-memory, transactional {@link DatasetGraph} backed by per-graph
     * copy-on-write snapshots — see {@link DatasetGraphInMemoryCowTxn} for the
     * design. Functionally equivalent to {@link #createTxnMem()}: same
     * {@link Transactional} contract, same isolation guarantees, same auto-wrap
     * behaviour for reads and writes outside a transaction.
     * <p>
     * Differs in implementation strategy: readers are lock-free and snapshot-
     * isolated via a single volatile reference, while writers fork a private
     * working copy at first-write and atomically republish on commit. The
     * practical effect compared with {@link #createTxnMem()} is:
     * <ul>
     *   <li>Bulk loads (one large write transaction) are several times faster.</li>
     *   <li>Multi-graph workloads are dramatically faster: each named graph has
     *       its own store, instead of contending on one indexed quad table.</li>
     *   <li>Concurrent reader throughput under a writer is orders of magnitude
     *       higher (readers do not serialize on writers).</li>
     *   <li>A workload of many tiny write transactions is comparable, sometimes
     *       slightly slower, because each write transaction allocates a fork.</li>
     * </ul>
     * Uses the default {@link GraphMemIndexedSetCowTxn.ForkMode#SEQUENTIAL} fork
     * strategy, which is the right choice for typical workloads. See
     * {@link #createTxnMemCow(GraphMemIndexedSetCowTxn.ForkMode)} if you have
     * very large graphs and want to evaluate the parallel fork.
     *
     * @return a transactional, in-memory, copy-on-write DatasetGraph
     */
    public static DatasetGraph createTxnMemCow() {
        return new DatasetGraphInMemoryCowTxn();
    }

    /**
     * Variant of {@link #createTxnMemCow()} that lets the caller pick the per-
     * graph {@link GraphMemIndexedSetCowTxn.ForkMode}.
     * <p>
     * {@link GraphMemIndexedSetCowTxn.ForkMode#SEQUENTIAL} is the recommended
     * default and is what {@link #createTxnMemCow()} uses.
     * {@link GraphMemIndexedSetCowTxn.ForkMode#PARALLEL} dispatches the
     * copy-on-write fork to the common ForkJoinPool; the dispatch overhead is
     * only worth paying on very large graphs being bulk-modified, and is
     * slower than SEQUENTIAL for the small-graph / many-small-writes patterns
     * typical of Fuseki workloads. Benchmark your own workload before choosing
     * PARALLEL.
     *
     * @param forkMode propagated to every per-graph instance created by this dataset
     * @return a transactional, in-memory, copy-on-write DatasetGraph using {@code forkMode}
     */
    public static DatasetGraph createTxnMemCow(GraphMemIndexedSetCowTxn.ForkMode forkMode) {
        return new DatasetGraphInMemoryCowTxn(Objects.requireNonNull(forkMode, "forkMode"));
    }

    /**
     * Create a transactional, in-memory {@link DatasetGraph} backed by the MVCC
     * variant {@link DatasetGraphInMemoryMvccTxn}. Functionally equivalent to
     * {@link #createTxnMem()} and {@link #createTxnMemCow()} — same data, same
     * full transaction support — but with a different performance profile: a
     * single version-stamped store shared by all transactions, so {@code begin}
     * never copies (O(1) regardless of graph size). This favours large graphs
     * with many small write transactions; reads pay a small per-candidate
     * version-visibility check. Benchmark against {@link #createTxnMemCow()} for
     * your workload.
     *
     * @return a transactional, in-memory, MVCC {@code DatasetGraph}
     */
    public static DatasetGraph createTxnMemMvcc() {
        return new DatasetGraphInMemoryMvccTxn();
    }

    /**
     * Create a general-purpose  {@link DatasetGraph}.<br/>
     * Any graphs needed are in-memory unless explicitly added with {@link DatasetGraph#addGraph(Node, Graph)}.
     * <p>
     * This dataset type can contain graphs from any source.
     * These are held as links to the supplied graph and not copied.
     * <p>
     * <em>This dataset does not support the graph indexing feature of jena-text.</em>
     * <p>
     * This dataset does not support serialized transactions (it only provides MRSW locking).
     *
     * @see #createTxnMem
     * @return a general-purpose Dataset
     */
    public static DatasetGraph createGeneral() {
        return createGeneral(graphMakerMem.create(null));
    }

    /**
     * Create a general-purpose  {@link DatasetGraph}.<br/>
     * Any graphs needed are in-memory unless explicitly added with {@link DatasetGraph#addGraph(Node, Graph)}.
     * <p>
     * This dataset type can contain graphs from any source.
     * These are held as links to the supplied graph and not copied.
     * <p>
     * <em>This dataset does not support the graph indexing feature of jena-text.</em>
     * <p>
     * This dataset does not support serialized transactions (it only provides MRSW locking).
     *
     * @see #createTxnMem
     * @return a general-purpose Dataset
     */
    public static DatasetGraph createGeneral(Graph dftGraph) {
        return new DatasetGraphMapLink(dftGraph, graphMakerMem);
    }

    /**
     * Clone the structure of a {@link DatasetGraph}.
     */
    public static DatasetGraph cloneStructure(DatasetGraph dsg) {
        Objects.requireNonNull(dsg, "DatasetGraph must be provided");
        DatasetGraphMapLink dsg2 = new DatasetGraphMapLink(dsg.getDefaultGraph());
        for ( Iterator<Node> names = dsg.listGraphNodes(); names.hasNext(); ) {
            Node gn = names.next();
            dsg2.addGraph(gn, dsg.getGraph(gn));
        }
        return dsg2;
    }

    /**
     * Create a DatasetGraph starting with a single graph.
     * New graphs that are explicitly added are held by reference.
     */
    public static DatasetGraph create(Graph dftGraph) {
        return new DatasetGraphMapLink(dftGraph);
    }

    /**
     * Create a DatasetGraph using the {@link GraphMaker} for all graphs.
     */
    public static DatasetGraph createWithGraphMaker(GraphMaker graphMaker) {
        return new DatasetGraphMap(graphMaker);
    }

    /**
     * Create a DatasetGraph which only ever has a single default graph.
     */
    public static DatasetGraph wrap(Graph graph) { return DatasetGraphOne.create(graph); }

    /**
     * An always empty {@link DatasetGraph}.
     * It has one graph (the default graph) with zero triples.
     * No changes allowed - this is not a sink.
     */
    public static DatasetGraph empty() { return DatasetGraphZero.create(); }

    /** Interface for making graphs when a dataset needs to add a new graph.
     *  Return null for no graph created.
     */
    public interface GraphMaker { public Graph create(Node name); }

    /** A graph maker that doesn't make graphs. */
    public static GraphMaker graphMakerNull = (name) -> null;

    /** A graph maker that creates unnamed Jena default graphs */
    public static GraphMaker graphMakerMem = (name) -> GraphFactory.createDefaultGraph();
}

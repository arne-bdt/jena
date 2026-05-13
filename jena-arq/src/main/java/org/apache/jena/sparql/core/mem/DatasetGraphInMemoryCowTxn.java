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

package org.apache.jena.sparql.core.mem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.store.cow.CowStore;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapStd;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.DatasetGraphTriplesQuads;
import org.apache.jena.sparql.core.GraphView;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.system.G;
import org.apache.jena.system.Txn;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;

/**
 * Transactional, in-memory {@link org.apache.jena.sparql.core.DatasetGraph}
 * built on per-graph {@link GraphMemIndexedSetCowTxn} instances.
 * <p>
 * Each named graph (and the default graph) is stored as a separate
 * {@code GraphMemIndexedSetCowTxn}, keyed in an immutable topology map.
 * The default graph occupies the slot {@link Quad#defaultGraphIRI}; named
 * graphs occupy slots keyed by their graph node. Topology snapshots are
 * published via a single {@code volatile} reference and replaced atomically
 * on commit.
 * <p>
 * <b>Transaction model.</b>
 * <ul>
 *   <li>One writer at a time across the whole dataset
 *       ({@link #datasetWriteLock}).</li>
 *   <li>Readers are lock-free for the read path; at {@code begin(READ)} they
 *       briefly hold {@link #publicationLock}'s read lock to capture the
 *       topology and each per-graph snapshot atomically with respect to a
 *       writer's commit.</li>
 *   <li>Writers commit under {@link #publicationLock}'s write lock so the
 *       per-graph commits and the new topology become visible together.</li>
 *   <li>{@code begin(WRITE)} does no per-graph work eagerly; graphs are
 *       enlisted ({@code begin(WRITE)} or promoted) on first write only.
 *       Reads inside a write transaction route to the writer's working
 *       copy for enlisted graphs and to the latest published snapshot for
 *       all others (safe because the dataset writer is exclusive).</li>
 * </ul>
 * The result is genuine dataset-wide snapshot isolation with O(1)
 * {@code begin(WRITE)} and pay-for-what-you-touch write costs.
 * <p>
 * <b>Fork mode.</b> The {@link GraphMemIndexedSetCowTxn.ForkMode} passed at
 * construction is propagated to every per-graph instance — both those
 * created up-front (the default graph) and any new named graphs added
 * during a write transaction. Callers benchmarking sequential vs. parallel
 * forks therefore see consistent behaviour across the whole dataset.
 */
public class DatasetGraphInMemoryCowTxn extends DatasetGraphTriplesQuads implements Transactional {

    private final GraphMemIndexedSetCowTxn.ForkMode forkMode;

    /** Serialises dataset writers. Acquired in begin(WRITE) (or successful promote), released in end. */
    private final ReentrantLock datasetWriteLock = new ReentrantLock();

    /**
     * Coordinates per-graph snapshot capture (reader's {@code begin}) with
     * per-graph commit and new-topology publication (writer's {@code commit}).
     * Held briefly: reader holds the read lock just long enough to capture
     * the topology and start a per-graph read transaction on each graph;
     * writer holds the write lock just long enough to commit each enlisted
     * graph and swap in the new topology.
     */
    private final ReentrantReadWriteLock publicationLock = new ReentrantReadWriteLock();

    /**
     * Currently published topology. Volatile so reads outside any transaction
     * see the latest version with a single volatile load.
     */
    private volatile GraphTopology published;

    /**
     * Monotonic write-transaction counter. Incremented by every committed
     * write — including no-op commits — so an ISOLATED promote can detect
     * <i>any</i> intervening writer, regardless of whether that writer
     * actually changed data. Mirrors {@code DatasetGraphInMemory}'s
     * generation counter.
     */
    private final AtomicLong generation = new AtomicLong(0);

    /** Per-thread transaction state; {@code null} when no transaction is active. */
    private final ThreadLocal<DsTxnState> activeTxn = new ThreadLocal<>();

    private final PrefixMap prefixes = new PrefixMapStd();

    /**
     * Cross-graph operations parallelise their per-graph
     * iterator/stream <em>construction</em> across the common
     * {@link java.util.concurrent.ForkJoinPool} when the dataset is
     * configured with {@link GraphMemIndexedSetCowTxn.ForkMode#PARALLEL}
     * and the number of named graphs reaches this threshold. Below the
     * threshold, FJP submission overhead typically dominates per-graph
     * construction work, so the sequential captured-view path is faster.
     * <p>
     * The threshold is intentionally conservative: parallelism helps most
     * when per-graph {@code find()} construction is meaningfully larger
     * than FJP dispatch overhead — e.g. with {@code LAZY} indexing
     * strategies that build their index on first partial-pattern lookup,
     * or with large per-graph triple sets. For typical Fuseki workloads
     * with a handful of graphs and {@code EAGER} indexes, the captured-view
     * sequential path is already several times faster than
     * {@link org.apache.jena.sparql.core.mem.DatasetGraphInMemory}.
     */
    private static final int PARALLEL_CROSS_GRAPH_THRESHOLD = 16;

    // --- Construction ---------------------------------------------------------

    public DatasetGraphInMemoryCowTxn() {
        this(GraphMemIndexedSetCowTxn.ForkMode.SEQUENTIAL);
    }

    public DatasetGraphInMemoryCowTxn(GraphMemIndexedSetCowTxn.ForkMode forkMode) {
        this.forkMode = forkMode;
        this.published = new GraphTopology(Map.of(Quad.defaultGraphIRI, newGraph()));
    }

    /** Fork mode used for newly created per-graph instances. */
    public GraphMemIndexedSetCowTxn.ForkMode getForkMode() {
        return forkMode;
    }

    private GraphMemIndexedSetCowTxn newGraph() {
        return new GraphMemIndexedSetCowTxn(forkMode);
    }

    // --- Topology -------------------------------------------------------------

    /**
     * Immutable snapshot of which graphs the dataset contains. Replaced
     * atomically on commit; readers pin a reference to a particular instance
     * for the duration of their transaction.
     */
    private record GraphTopology(Map<Node, GraphMemIndexedSetCowTxn> graphs) {
        GraphMemIndexedSetCowTxn get(Node name) {
            return graphs.get(name);
        }
    }

    // --- Per-thread transaction state ----------------------------------------

    private static final class DsTxnState {
        TxnType type;
        ReadWrite mode;
        /** Topology pinned at begin (read txns also start per-graph reads on every entry). */
        GraphTopology pinned;
        /** Generation counter value at begin; used to detect concurrent commits. */
        long startGeneration;
        /**
         * Per-graph {@link CowStore} read views captured atomically at
         * {@code begin(READ)}. Non-null only in READ mode before any
         * promotion. Enables thread-safe parallel cross-graph reads —
         * workers from a {@link java.util.concurrent.ForkJoinPool} use
         * the captured references directly, avoiding the per-graph
         * {@code ThreadLocal} that the per-thread {@code readStore()}
         * path would consult. Also keeps the read view stable past the
         * end of an auto-wrapped READ transaction, so iterators returned
         * from {@link #findInAnyNamedGraphs} continue to observe the
         * same snapshot after the wrapping txn ends.
         */
        Map<Node, CowStore> capturedViews;
        /** Graphs newly added during this write transaction; null until first add. */
        Map<Node, GraphMemIndexedSetCowTxn> additions;
        /** Graphs marked removed during this write transaction; null until first remove. */
        Set<Node> removals;
        /** Graphs with an active per-graph write transaction started by this dataset txn. */
        Set<Node> enlisted;
        /** True once any mutation has happened (data or topology). */
        boolean dirty;
    }

    private DsTxnState require() {
        DsTxnState t = activeTxn.get();
        if (t == null)
            throw new JenaTransactionException("Not in a transaction");
        return t;
    }

    // --- Lookup helpers -------------------------------------------------------

    /**
     * Resolve the live graph instance for {@code name} in the current
     * thread's context: the active transaction's view if any, otherwise the
     * latest published topology. Returns {@code null} if the named graph is
     * absent (the default graph is never absent).
     */
    private GraphMemIndexedSetCowTxn currentGraph(Node name) {
        DsTxnState t = activeTxn.get();
        if (t == null)
            return published.get(name);
        if (t.additions != null) {
            GraphMemIndexedSetCowTxn g = t.additions.get(name);
            if (g != null) return g;
        }
        if (t.removals != null && t.removals.contains(name))
            return null;
        return t.pinned.get(name);
    }

    /**
     * Merged view of the current topology for cross-graph operations. Returns
     * the pinned (or published) map directly when no additions/removals are
     * pending, so the no-topology-change case is O(1) and allocation-free.
     */
    private Map<Node, GraphMemIndexedSetCowTxn> currentTopology() {
        DsTxnState t = activeTxn.get();
        if (t == null)
            return published.graphs();
        if (t.additions == null && t.removals == null)
            return t.pinned.graphs();
        Map<Node, GraphMemIndexedSetCowTxn> merged = new HashMap<>(t.pinned.graphs());
        if (t.additions != null) merged.putAll(t.additions);
        if (t.removals != null) merged.keySet().removeAll(t.removals);
        return merged;
    }

    /**
     * Ensure {@code name} resolves to a live graph instance under a write
     * transaction, creating a fresh one if necessary and enlisting it so its
     * write transaction is committed (or aborted) with the dataset's.
     */
    private GraphMemIndexedSetCowTxn graphForWrite(Node name) {
        DsTxnState t = require();
        if (t.mode != ReadWrite.WRITE && !promote())
            throw new JenaTransactionException(
                    "Cannot write inside a non-promotable READ transaction");
        GraphMemIndexedSetCowTxn g = lookupForCurrentTxn(t, name);
        if (g == null) {
            g = newGraph();
            if (t.additions == null) t.additions = new HashMap<>();
            t.additions.put(name, g);
            if (t.removals != null) t.removals.remove(name);
        }
        enlistForWrite(t, name, g);
        t.dirty = true;
        return g;
    }

    /**
     * Same as {@link #graphForWrite(Node)} but returns {@code null} when the
     * named graph doesn't exist (instead of creating it). Used by deletes
     * which should be a no-op against a non-existent graph.
     */
    private GraphMemIndexedSetCowTxn graphForWriteIfExists(Node name) {
        DsTxnState t = require();
        if (t.mode != ReadWrite.WRITE && !promote())
            throw new JenaTransactionException(
                    "Cannot write inside a non-promotable READ transaction");
        GraphMemIndexedSetCowTxn g = lookupForCurrentTxn(t, name);
        if (g == null) return null;
        enlistForWrite(t, name, g);
        t.dirty = true;
        return g;
    }

    private GraphMemIndexedSetCowTxn lookupForCurrentTxn(DsTxnState t, Node name) {
        if (t.additions != null) {
            GraphMemIndexedSetCowTxn g = t.additions.get(name);
            if (g != null) return g;
        }
        if (t.removals != null && t.removals.contains(name))
            return null;
        return t.pinned.get(name);
    }

    private void enlistForWrite(DsTxnState t, Node name, GraphMemIndexedSetCowTxn g) {
        if (t.enlisted == null) t.enlisted = new HashSet<>();
        if (!t.enlisted.add(name)) return;
        // First time we touch this graph in this dataset write txn.
        if (!g.isInTransaction()) {
            // Freshly created (additions) or not yet enlisted: start a write txn.
            g.begin(TxnType.WRITE);
        } else if (g.transactionMode() == ReadWrite.READ) {
            // Came from begin(READ_PROMOTE) on the dataset; promote in place.
            if (!g.promote())
                throw new JenaTransactionException(
                        "Per-graph promotion failed for " + name);
        }
        // else: already WRITE (should not happen because we hold datasetWriteLock).
    }

    // --- Transactional --------------------------------------------------------

    @Override public boolean supportsTransactions()     { return true; }
    @Override public boolean supportsTransactionAbort() { return true; }

    @Override
    public void begin(TxnType type) {
        if (activeTxn.get() != null)
            throw new JenaTransactionException("Nested transactions are not supported");
        DsTxnState s = new DsTxnState();
        s.type = type;
        if (type == TxnType.WRITE) {
            datasetWriteLock.lock();
            s.mode = ReadWrite.WRITE;
            s.pinned = published;
            s.startGeneration = generation.get();
            // Per-graph WRITE transactions are started lazily on first write.
        } else {
            s.mode = ReadWrite.READ;
            publicationLock.readLock().lock();
            try {
                s.pinned = published;
                s.startGeneration = generation.get();
                // Eager per-graph begin: captures every graph's snapshot
                // atomically with respect to a writer's publication. The
                // capturedViews map records each captured snapshot reference
                // so that subsequent cross-graph operations can dispatch to
                // ForkJoinPool workers (which do not have the per-thread
                // transaction state set up here) without falling back to
                // each graph's latest published snapshot.
                Map<Node, CowStore> views = new HashMap<>(s.pinned.graphs().size());
                for (Map.Entry<Node, GraphMemIndexedSetCowTxn> e : s.pinned.graphs().entrySet()) {
                    GraphMemIndexedSetCowTxn g = e.getValue();
                    g.begin(type);
                    views.put(e.getKey(), g.readView());
                }
                s.capturedViews = views;
            } finally {
                publicationLock.readLock().unlock();
            }
        }
        activeTxn.set(s);
    }

    @Override
    public boolean promote(Promote mode) {
        DsTxnState t = require();
        if (t.mode == ReadWrite.WRITE) return true;
        if (t.type == TxnType.READ)
            return false;

        if (mode == Promote.READ_COMMITTED) {
            datasetWriteLock.lock();
        } else {
            // ISOLATED: fail-fast if the dataset already moved past our snapshot;
            // otherwise block on the writer lock, then re-check after acquiring
            // (a concurrent writer may have committed while we waited).
            if (t.startGeneration != generation.get()) return false;
            datasetWriteLock.lock();
            if (t.startGeneration != generation.get()) {
                datasetWriteLock.unlock();
                return false;
            }
        }
        if (mode == Promote.READ_COMMITTED) {
            // Drop the old snapshot view; re-pin to latest published. Per-graph
            // read txns end now; subsequent reads go through published (stable
            // because we hold datasetWriteLock), and writes lazy-fork as usual.
            for (GraphMemIndexedSetCowTxn g : t.pinned.graphs().values()) g.end();
            t.pinned = published;
            t.startGeneration = generation.get();
        }
        // ISOLATED: keep per-graph READ txns alive; they get promoted to WRITE
        // on first per-graph write via enlistForWrite.
        // After any promote, capturedViews is stale for graphs we'll write to
        // (writer's working copy must be visible). Drop it to force the
        // cross-graph operations onto the per-thread path, which correctly
        // composes reads of the writer's working copy with reads of the
        // captured published snapshots on the per-graph TxnState.
        t.capturedViews = null;
        t.mode = ReadWrite.WRITE;
        return true;
    }

    @Override
    public void commit() {
        DsTxnState t = require();
        if (t.mode == ReadWrite.WRITE)
            commitWrite(t);
        else
            endReadTxn(t);
    }

    private void commitWrite(DsTxnState t) {
        try {
            publicationLock.writeLock().lock();
            try {
                if (t.enlisted != null) {
                    for (Node name : t.enlisted) {
                        GraphMemIndexedSetCowTxn g = resolveEnlisted(t, name);
                        if (g != null) g.commit();
                    }
                }
                if (t.dirty)
                    published = buildPublishedTopology(t);
                // Always bump the generation: an ISOLATED promote that began
                // before this commit must fail even if nothing was actually
                // written, matching DatasetGraphInMemory.
                generation.incrementAndGet();
            } finally {
                publicationLock.writeLock().unlock();
            }
        } finally {
            datasetWriteLock.unlock();
            activeTxn.remove();
        }
    }

    /**
     * Build the new published topology by applying this write transaction's
     * additions and removals to the pinned topology. Returns the pinned
     * topology unchanged when nothing about the graph set changed.
     */
    private GraphTopology buildPublishedTopology(DsTxnState t) {
        if ((t.additions == null || t.additions.isEmpty())
                && (t.removals == null || t.removals.isEmpty()))
            return t.pinned;
        Map<Node, GraphMemIndexedSetCowTxn> next = new HashMap<>(t.pinned.graphs());
        if (t.additions != null) next.putAll(t.additions);
        if (t.removals != null) next.keySet().removeAll(t.removals);
        return new GraphTopology(Map.copyOf(next));
    }

    private GraphMemIndexedSetCowTxn resolveEnlisted(DsTxnState t, Node name) {
        if (t.additions != null) {
            GraphMemIndexedSetCowTxn g = t.additions.get(name);
            if (g != null) return g;
        }
        return t.pinned.get(name);
    }

    @Override
    public void abort() {
        DsTxnState t = require();
        if (t.mode == ReadWrite.WRITE)
            abortWrite(t);
        else
            endReadTxn(t);
    }

    private void abortWrite(DsTxnState t) {
        try {
            if (t.enlisted != null) {
                for (Node name : t.enlisted) {
                    GraphMemIndexedSetCowTxn g = resolveEnlisted(t, name);
                    if (g != null) g.abort();
                }
            }
        } finally {
            datasetWriteLock.unlock();
            activeTxn.remove();
        }
    }

    private void endReadTxn(DsTxnState t) {
        try {
            for (GraphMemIndexedSetCowTxn g : t.pinned.graphs().values()) g.end();
        } finally {
            activeTxn.remove();
        }
    }

    @Override
    public void end() {
        DsTxnState t = activeTxn.get();
        if (t == null) return;
        if (t.mode == ReadWrite.WRITE) {
            // Reaching end() in WRITE mode without commit() or abort() is a
            // programming error. Force an abort to release the writer lock,
            // then surface the mistake — mirrors DatasetGraphInMemory.
            abortWrite(t);
            throw new JenaTransactionException(
                    "Write transaction was not committed or aborted before end()");
        }
        endReadTxn(t);
    }

    @Override public boolean isInTransaction() { return activeTxn.get() != null; }

    @Override
    public ReadWrite transactionMode() {
        DsTxnState t = activeTxn.get();
        return t == null ? null : t.mode;
    }

    @Override
    public TxnType transactionType() {
        DsTxnState t = activeTxn.get();
        return t == null ? null : t.type;
    }

    // --- Mutation routing (DatasetGraphTriplesQuads) -------------------------

    @Override
    protected void addToDftGraph(Node s, Node p, Node o) {
        mutate(() -> graphForWrite(Quad.defaultGraphIRI).add(Triple.create(s, p, o)));
    }

    @Override
    protected void addToNamedGraph(Node g, Node s, Node p, Node o) {
        mutate(() -> graphForWrite(g).add(Triple.create(s, p, o)));
    }

    @Override
    protected void deleteFromDftGraph(Node s, Node p, Node o) {
        mutate(() -> {
            GraphMemIndexedSetCowTxn g = graphForWriteIfExists(Quad.defaultGraphIRI);
            if (g != null) g.delete(Triple.create(s, p, o));
        });
    }

    @Override
    protected void deleteFromNamedGraph(Node g, Node s, Node p, Node o) {
        mutate(() -> {
            GraphMemIndexedSetCowTxn graph = graphForWriteIfExists(g);
            if (graph != null) graph.delete(Triple.create(s, p, o));
        });
    }

    /** Auto-wrap a mutation in a write transaction when not already in one. */
    private void mutate(Runnable r) {
        if (isInTransaction()) {
            r.run();
        } else {
            Txn.executeWrite(this, r);
        }
    }

    // --- Find paths -----------------------------------------------------------

    @Override
    protected Iterator<Quad> findInDftGraph(Node s, Node p, Node o) {
        return access(() -> {
            CowStore view = readViewFor(Quad.defaultGraphIRI);
            if (view == null) return Iter.<Quad>nullIterator();
            return G.triples2quadsDftGraph(view.find(Triple.createMatch(s, p, o)));
        });
    }

    @Override
    protected Iterator<Quad> findInSpecificNamedGraph(Node g, Node s, Node p, Node o) {
        return access(() -> {
            CowStore view = readViewFor(g);
            if (view == null) return Iter.<Quad>nullIterator();
            return view.find(Triple.createMatch(s, p, o))
                    .mapWith(t -> Quad.create(g, t));
        });
    }

    @Override
    protected Iterator<Quad> findInAnyNamedGraphs(Node s, Node p, Node o) {
        return access(() -> {
            Triple match = Triple.createMatch(s, p, o);
            Map<Node, CowStore> views = capturedNamedGraphViews();
            if (views != null) {
                // READ path: use captured views so the result is correct on
                // any thread (including FJP workers) and after the wrapping
                // txn ends. When the dataset is configured PARALLEL and the
                // graph count is above the threshold, construct per-graph
                // iterators in parallel and chain them with `andThen`. The
                // chain is consumed sequentially so callers can short-circuit
                // through the chained iterators normally; what's parallelised
                // is the (often expensive) per-graph find() call itself.
                if (shouldParallelizeCrossGraph(views))
                    return parallelFindInNamedGraphs(views, match);
                return sequentialFindInNamedGraphs(views, match);
            }
            // WRITE path (or post-promote): fall back to the per-thread
            // route so the writer's working copy is visible on the named
            // graphs we have enlisted, while reads on non-enlisted graphs
            // still see their captured per-graph snapshots.
            return sequentialFindInNamedGraphsByThread(match);
        });
    }

    /**
     * The captured per-named-graph read views for the active transaction,
     * or {@code null} if cross-graph reads should fall back to the
     * per-thread route (no transaction, WRITE transaction, or post-promote).
     */
    private Map<Node, CowStore> capturedNamedGraphViews() {
        DsTxnState t = activeTxn.get();
        if (t == null) return null;
        return t.capturedViews;
    }

    private boolean shouldParallelizeCrossGraph(Map<Node, CowStore> views) {
        if (forkMode != GraphMemIndexedSetCowTxn.ForkMode.PARALLEL) return false;
        // -1 because defaultGraphIRI is in the map but never matches a named-graph query.
        return views.size() - 1 >= PARALLEL_CROSS_GRAPH_THRESHOLD;
    }

    /**
     * Build per-graph iterators in parallel on the common ForkJoinPool and
     * chain them with {@link ExtendedIterator#andThen}. Iteration of the
     * chain is sequential; only the (often-expensive, e.g. LAZY index build)
     * construction of the underlying iterators is parallelised.
     */
    private ExtendedIterator<Quad> parallelFindInNamedGraphs(Map<Node, CowStore> views, Triple match) {
        List<ExtendedIterator<Quad>> perGraph = views.entrySet().parallelStream()
                .filter(e -> !Quad.defaultGraphIRI.equals(e.getKey()))
                .map(e -> {
                    Node name = e.getKey();
                    CowStore view = e.getValue();
                    return view.find(match).mapWith(t -> Quad.create(name, t));
                })
                .collect(Collectors.toList());
        return concat(perGraph);
    }

    private ExtendedIterator<Quad> sequentialFindInNamedGraphs(Map<Node, CowStore> views, Triple match) {
        ExtendedIterator<Quad> result = NiceIterator.emptyIterator();
        for (Map.Entry<Node, CowStore> e : views.entrySet()) {
            if (Quad.defaultGraphIRI.equals(e.getKey())) continue;
            Node name = e.getKey();
            ExtendedIterator<Quad> it = e.getValue().find(match).mapWith(t -> Quad.create(name, t));
            result = result.andThen(it);
        }
        return result;
    }

    /**
     * Per-thread sequential fallback. Used when capturedViews is unavailable
     * (no transaction, WRITE transaction, or post-promote). Lazy:
     * {@code find()} on graph K is delayed until the caller has exhausted
     * graphs 1..K-1, so short-circuit consumers like {@code ASK} pay for
     * only the graphs they touch.
     */
    private Iterator<Quad> sequentialFindInNamedGraphsByThread(Triple match) {
        Map<Node, GraphMemIndexedSetCowTxn> topo = currentTopology();
        Iterator<Map.Entry<Node, GraphMemIndexedSetCowTxn>> entries = topo.entrySet().iterator();
        return Iter.flatMap(entries, e -> {
            if (Quad.defaultGraphIRI.equals(e.getKey()))
                return Iter.nullIterator();
            Node name = e.getKey();
            return e.getValue().find(match).mapWith(t -> Quad.create(name, t));
        });
    }

    private static <T> ExtendedIterator<T> concat(List<ExtendedIterator<T>> iters) {
        ExtendedIterator<T> result = NiceIterator.emptyIterator();
        for (ExtendedIterator<T> it : iters)
            result = result.andThen(it);
        return result;
    }

    // --- Stream paths ---------------------------------------------------------

    /**
     * Specialised {@link DatasetGraph#stream(Node, Node, Node, Node)} that
     * routes cross-graph queries through the same captured-view path as
     * {@link #findInAnyNamedGraphs} and, under
     * {@link GraphMemIndexedSetCowTxn.ForkMode#PARALLEL} with enough named
     * graphs, builds the per-graph streams in parallel on the common
     * ForkJoinPool. Single-graph queries delegate straight to the
     * captured view's {@link CowStore#stream(Triple)} so the returned
     * stream is independently parallelisable via {@code .parallel()} by
     * the caller — they hold a stable view reference and don't touch any
     * per-thread transaction state.
     * <p>
     * The default super-interface implementation just wraps {@link #find}
     * via {@code Iter.asStream}; that path is correct but doesn't expose
     * the parallelism, and inherits the caveats of the iterator path.
     */
    @Override
    public Stream<Quad> stream(Node g, Node s, Node p, Node o) {
        Triple match = Triple.createMatch(s, p, o);

        if (Quad.isUnionGraph(g)) {
            // Defer to the inherited union-graph routing (deduplicates triples).
            return Iter.asStream(findNG(g, s, p, o));
        }

        // Specific default graph (defaultGraphIRI or defaultGraphNodeGenerated).
        if (Quad.isDefaultGraph(g)) {
            return access(() -> {
                CowStore view = readViewFor(Quad.defaultGraphIRI);
                if (view == null) return Stream.<Quad>empty();
                return view.stream(match).map(t -> Quad.create(Quad.defaultGraphIRI, t));
            });
        }

        // Wildcard graph: null or Node.ANY. Mirrors DatasetGraphBaseFind.find:
        // the default graph plus every named graph.
        if (g == null || Node.ANY.equals(g)) {
            return access(() -> {
                Map<Node, CowStore> views = capturedNamedGraphViews();
                Stream<Quad> namedGraphs;
                if (views != null) {
                    namedGraphs = shouldParallelizeCrossGraph(views)
                            ? parallelStreamInNamedGraphs(views, match)
                            : sequentialStreamInNamedGraphs(views, match);
                } else {
                    namedGraphs = streamInNamedGraphsByThread(match);
                }
                CowStore dft = readViewFor(Quad.defaultGraphIRI);
                if (dft == null) return namedGraphs;
                return Stream.concat(
                        dft.stream(match).map(t -> Quad.create(Quad.defaultGraphIRI, t)),
                        namedGraphs);
            });
        }

        // Specific named graph.
        return access(() -> {
            CowStore view = readViewFor(g);
            if (view == null) return Stream.<Quad>empty();
            return view.stream(match).map(t -> Quad.create(g, t));
        });
    }

    /**
     * The captured read view for {@code name}, or the per-thread view if no
     * captured-views map is available. Returns {@code null} only for a
     * named graph that does not exist in the current transaction's
     * topology.
     */
    private CowStore readViewFor(Node name) {
        DsTxnState t = activeTxn.get();
        if (t != null && t.capturedViews != null) {
            CowStore v = t.capturedViews.get(name);
            if (v != null) return v;
        }
        GraphMemIndexedSetCowTxn g = currentGraph(name);
        return (g == null) ? null : g.readView();
    }

    private Stream<Quad> parallelStreamInNamedGraphs(Map<Node, CowStore> views, Triple match) {
        // Build per-graph streams in parallel; outer stream is sequential.
        List<Stream<Quad>> perGraph = views.entrySet().parallelStream()
                .filter(e -> !Quad.defaultGraphIRI.equals(e.getKey()))
                .map(e -> {
                    Node name = e.getKey();
                    CowStore view = e.getValue();
                    return view.stream(match).map(t -> Quad.create(name, t));
                })
                .collect(Collectors.toList());
        return perGraph.stream().flatMap(s -> s);
    }

    private Stream<Quad> sequentialStreamInNamedGraphs(Map<Node, CowStore> views, Triple match) {
        return views.entrySet().stream()
                .filter(e -> !Quad.defaultGraphIRI.equals(e.getKey()))
                .flatMap(e -> {
                    Node name = e.getKey();
                    return e.getValue().stream(match).map(t -> Quad.create(name, t));
                });
    }

    private Stream<Quad> streamInNamedGraphsByThread(Triple match) {
        return currentTopology().entrySet().stream()
                .filter(e -> !Quad.defaultGraphIRI.equals(e.getKey()))
                .flatMap(e -> {
                    Node name = e.getKey();
                    return e.getValue().stream(match.getSubject(),
                                               match.getPredicate(),
                                               match.getObject())
                            .map(t -> Quad.create(name, t));
                });
    }

    // --- Graph container view -------------------------------------------------

    @Override
    public Graph getDefaultGraph() {
        return GraphView.createDefaultGraph(this);
    }

    @Override
    public Graph getGraph(Node graphNode) {
        return GraphView.createNamedGraph(this, graphNode);
    }

    @Override
    public Graph getUnionGraph() {
        return GraphView.createUnionGraph(this);
    }

    @Override
    public Iterator<Node> listGraphNodes() {
        return access(() -> {
            Map<Node, GraphMemIndexedSetCowTxn> topo = currentTopology();
            // Named graphs only, and only those that currently hold at least one triple
            // — matches DatasetGraphInMemory, where an empty named graph is indistinguishable
            // from one that was never added.
            return topo.entrySet().stream()
                    .filter(e -> !Quad.defaultGraphIRI.equals(e.getKey()))
                    .filter(e -> !e.getValue().isEmpty())
                    .map(Map.Entry::getKey)
                    .iterator();
        });
    }

    @Override
    public boolean containsGraph(Node graphNode) {
        if (Quad.isDefaultGraph(graphNode)) return true;
        if (Quad.isUnionGraph(graphNode)) return true;
        return access(() -> {
            GraphMemIndexedSetCowTxn g = currentGraph(graphNode);
            return g != null && !g.isEmpty();
        });
    }

    // --- contains / isEmpty optimisations -------------------------------------
    //
    // DatasetGraphBase's defaults build an iterator and ask hasNext(). We can
    // do considerably better by short-circuiting on per-graph CowStore.contains
    // (an O(1) indexed lookup when (s,p,o) is concrete + EAGER index, and
    // short-circuiting per-graph for wildcards). The captured-view path also
    // keeps the boolean answer stable past the end of an auto-wrapped READ
    // transaction, mirroring the find/stream paths above.

    /**
     * Test whether the dataset contains a quad matching the pattern. Routes
     * by graph node — default graph, union graph, wildcard, or specific
     * named graph — and short-circuits on the first match.
     * <p>
     * The default {@link org.apache.jena.sparql.core.DatasetGraphBase
     * DatasetGraphBase} implementation builds a {@code find(...)} iterator
     * and asks {@code hasNext()}. Each pre-graph view's
     * {@link CowStore#contains(Triple)} is dramatically faster for the
     * (s,p,o)-concrete case (a single hash lookup with EAGER indexing) and
     * still short-circuits at the dataset level for wildcards.
     */
    @Override
    public boolean contains(Node g, Node s, Node p, Node o) {
        Triple match = Triple.createMatch(s, p, o);
        return access(() -> {
            if (Quad.isDefaultGraph(g)) {
                CowStore view = readViewFor(Quad.defaultGraphIRI);
                return view != null && view.contains(match);
            }
            if (Quad.isUnionGraph(g)) {
                return anyNamedGraphContains(match);
            }
            if (g == null || Node.ANY.equals(g)) {
                // Wildcard: default graph plus any named graph.
                CowStore dft = readViewFor(Quad.defaultGraphIRI);
                if (dft != null && dft.contains(match)) return true;
                return anyNamedGraphContains(match);
            }
            CowStore view = readViewFor(g);
            return view != null && view.contains(match);
        });
    }

    private boolean anyNamedGraphContains(Triple match) {
        Map<Node, CowStore> views = capturedNamedGraphViews();
        if (views != null) {
            for (Map.Entry<Node, CowStore> e : views.entrySet()) {
                if (Quad.defaultGraphIRI.equals(e.getKey())) continue;
                if (e.getValue().contains(match)) return true;
            }
            return false;
        }
        // Per-thread fallback (WRITE txn or auto-wrap outside a txn).
        for (Map.Entry<Node, GraphMemIndexedSetCowTxn> e : currentTopology().entrySet()) {
            if (Quad.defaultGraphIRI.equals(e.getKey())) continue;
            if (e.getValue().contains(match)) return true;
        }
        return false;
    }

    /**
     * Whether the dataset has no quads in any graph. The default
     * implementation builds a wildcard {@code find(...)} iterator just to
     * check {@code hasNext()}; we ask each captured view's
     * {@link CowStore#isEmpty()} directly and short-circuit on the first
     * non-empty graph.
     */
    @Override
    public boolean isEmpty() {
        return access(() -> {
            Map<Node, CowStore> views = capturedNamedGraphViews();
            if (views != null) {
                for (CowStore v : views.values()) {
                    if (!v.isEmpty()) return false;
                }
                return true;
            }
            for (GraphMemIndexedSetCowTxn g : currentTopology().values()) {
                if (!g.isEmpty()) return false;
            }
            return true;
        });
    }

    // --- Misc DatasetGraph --------------------------------------------------

    @Override
    public PrefixMap prefixes() {
        return prefixes;
    }

    @Override
    public long size() {
        return access(() -> {
            long n = 0;
            for (Map.Entry<Node, GraphMemIndexedSetCowTxn> e : currentTopology().entrySet()) {
                if (Quad.defaultGraphIRI.equals(e.getKey())) continue;
                if (!e.getValue().isEmpty()) n++;
            }
            return n;
        });
    }

    @Override
    public void close() {
        if (isInTransaction()) abort();
    }

    /**
     * Read inside a transaction if one is active, otherwise wrap in an
     * auto-managed READ transaction so reads always observe a consistent view.
     */
    private <T> T access(java.util.function.Supplier<T> source) {
        return isInTransaction() ? source.get() : Txn.calculateRead(this, source);
    }
}

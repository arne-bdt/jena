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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
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
 * The default graph is held in a dedicated final field and the named
 * graphs in an immutable topology map. Named graphs come and go (via
 * {@code addGraph} / {@code removeGraph}); the default graph is created
 * up-front and only ever has its <em>contents</em> mutated. Keeping the
 * two separate eliminates the per-iteration "skip the default graph"
 * filter that a unified map would require on every cross-graph
 * operation.
 * <p>
 * <b>Transaction model.</b>
 * <ul>
 *   <li>One writer at a time across the whole dataset
 *       ({@link #datasetWriteLock}).</li>
 *   <li>Readers are lock-free for the read path; at {@code begin(READ)} they
 *       briefly hold {@link #publicationLock}'s read lock to capture the
 *       named-graph topology and each per-graph snapshot atomically with
 *       respect to a writer's commit.</li>
 *   <li>Writers commit under {@link #publicationLock}'s write lock so the
 *       per-graph commits and the new named-graph topology become visible
 *       together.</li>
 *   <li>{@code begin(WRITE)} does no per-graph work eagerly; the default
 *       graph and any named graphs are enlisted on first write only.
 *       Reads inside a write transaction route to the writer's working
 *       copy for enlisted graphs and to the latest published snapshot for
 *       all others (safe because the dataset writer is exclusive).</li>
 * </ul>
 * The result is genuine dataset-wide snapshot isolation with O(1)
 * {@code begin(WRITE)} and pay-for-what-you-touch write costs.
 * <p>
 * {@code promote()} on a {@code READ_PROMOTE} or
 * {@code READ_COMMITTED_PROMOTE} transaction may <em>block</em> waiting for
 * any concurrent dataset writer to commit or abort. For
 * {@code READ_COMMITTED} this is unconditional; for {@code ISOLATED} the
 * call first fails fast if the dataset generation has already moved past
 * the one captured at {@code begin()}, then blocks on
 * {@link #datasetWriteLock} (a concurrent writer may yet abort, in which
 * case promotion succeeds), then re-checks once the lock is held. Callers
 * that need a non-blocking attempt should detect the concurrent writer
 * themselves rather than rely on {@code promote} to fail-fast.
 * <p>
 * <b>Fork mode.</b> The {@link GraphMemIndexedSetCowTxn.ForkMode} passed at
 * construction is propagated to every per-graph instance — both the
 * default graph and any new named graphs added during a write transaction.
 * Callers benchmarking sequential vs. parallel forks therefore see
 * consistent behaviour across the whole dataset.
 */
public class DatasetGraphInMemoryCowTxn extends DatasetGraphTriplesQuads implements Transactional {

    private final GraphMemIndexedSetCowTxn.ForkMode forkMode;

    /** Serialises dataset writers. Acquired in begin(WRITE) (or successful promote), released in end. */
    private final ReentrantLock datasetWriteLock = new ReentrantLock();

    /**
     * Coordinates per-graph snapshot capture (reader's {@code begin}) with
     * per-graph commit and new-topology publication (writer's {@code commit}).
     * Held briefly: reader holds the read lock just long enough to capture
     * the default-graph view, the named-graph topology, and every
     * per-named-graph view; writer holds the write lock just long enough
     * to commit each enlisted graph and swap in the new named-graph
     * topology.
     */
    private final ReentrantReadWriteLock publicationLock = new ReentrantReadWriteLock();

    /**
     * The default graph. Created once at dataset construction and never
     * replaced — its contents may be mutated, but the instance is stable.
     * {@code removeGraph(Quad.defaultGraphIRI)} clears its contents in place;
     * {@code clear()} likewise empties the default graph in place. The instance manages its own
     * {@code volatile published} snapshot internally, so the dataset does
     * not need a separate field for the default graph's current state.
     */
    private final GraphMemIndexedSetCowTxn defaultGraph;

    /**
     * Named-graph topology — the set of named graphs currently in the
     * dataset and their per-graph store instances. Volatile so reads
     * outside any transaction see the latest version with a single
     * volatile load; atomically replaced on commit by writers that added
     * or removed named graphs.
     */
    private volatile GraphTopology namedTopology;

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
        this.defaultGraph = newGraph();
        this.namedTopology = new GraphTopology(Map.of());
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
     * Immutable snapshot of the named graphs in the dataset. The default
     * graph is never a member — it lives in {@link #defaultGraph}, a
     * separate final field. Replaced atomically on commit; readers pin a
     * reference to a particular instance for the duration of their
     * transaction.
     */
    private record GraphTopology(Map<Node, GraphMemIndexedSetCowTxn> graphs) {
        GraphMemIndexedSetCowTxn get(Node name) {
            return graphs.get(name);
        }
        boolean contains(Node name) {
            return graphs.containsKey(name);
        }
    }

    // --- Per-thread transaction state ----------------------------------------

    private static final class DsTxnState {
        TxnType type;
        ReadWrite mode;
        /** Named-graph topology pinned at begin. */
        GraphTopology pinnedNamed;
        /** Generation counter value at begin; used to detect concurrent commits. */
        long startGeneration;
        /**
         * Default-graph read view captured at {@code begin(READ)}. Non-null
         * only in READ mode before any promotion. Enables thread-safe
         * default-graph reads — workers from a
         * {@link java.util.concurrent.ForkJoinPool} use the captured
         * reference directly, avoiding the per-thread state lookup that
         * the live {@code defaultGraph.readView()} path would consult on
         * each call. Also keeps the read view stable past the end of an
         * auto-wrapped READ transaction.
         */
        CowStore capturedDefaultView;
        /**
         * Per-named-graph read views captured atomically at
         * {@code begin(READ)}. Same purpose as
         * {@link #capturedDefaultView} but for the named graphs in
         * {@link #pinnedNamed}.
         */
        Map<Node, CowStore> capturedNamedViews;
        /** Named graphs newly added during this write transaction; null until first add. */
        Map<Node, GraphMemIndexedSetCowTxn> additions;
        /** Named graphs marked removed during this write transaction; null until first remove. */
        Set<Node> removals;
        /** Named graphs with an active per-graph write transaction; null until first write. */
        Set<Node> enlistedNamed;
        /** True if the default graph was written to during this write transaction. */
        boolean defaultEnlisted;
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
     * Resolve the live named graph instance for {@code name} in the current
     * thread's context: the active transaction's view if any, otherwise
     * the latest published topology. Returns {@code null} if the named
     * graph is absent. Not used for the default graph (which is always
     * {@link #defaultGraph}).
     */
    private GraphMemIndexedSetCowTxn currentNamedGraph(Node name) {
        DsTxnState t = activeTxn.get();
        if (t == null)
            return namedTopology.get(name);
        if (t.additions != null) {
            GraphMemIndexedSetCowTxn g = t.additions.get(name);
            if (g != null) return g;
        }
        if (t.removals != null && t.removals.contains(name))
            return null;
        return t.pinnedNamed.get(name);
    }

    /**
     * Merged view of the named-graph topology for cross-graph operations.
     * Returns the pinned (or published) map directly when no additions or
     * removals are pending, so the no-topology-change case is O(1) and
     * allocation-free.
     */
    private Map<Node, GraphMemIndexedSetCowTxn> currentNamedTopology() {
        DsTxnState t = activeTxn.get();
        if (t == null)
            return namedTopology.graphs();
        if (t.additions == null && t.removals == null)
            return t.pinnedNamed.graphs();
        Map<Node, GraphMemIndexedSetCowTxn> merged = new HashMap<>(t.pinnedNamed.graphs());
        if (t.additions != null) merged.putAll(t.additions);
        if (t.removals != null) merged.keySet().removeAll(t.removals);
        return merged;
    }

    /**
     * Default-graph read view for the current thread: the captured view
     * if available, otherwise the live published view. Never returns
     * {@code null} — the default graph always exists.
     */
    private CowStore currentDefaultView() {
        DsTxnState t = activeTxn.get();
        if (t != null && t.capturedDefaultView != null) return t.capturedDefaultView;
        return defaultGraph.readView();
    }

    /**
     * Named-graph read view for the current thread: the captured view
     * if available, otherwise the live published view. Returns
     * {@code null} only when the named graph doesn't exist in the current
     * transaction's topology.
     */
    private CowStore currentNamedView(Node name) {
        DsTxnState t = activeTxn.get();
        if (t != null && t.capturedNamedViews != null) {
            CowStore v = t.capturedNamedViews.get(name);
            if (v != null) return v;
        }
        GraphMemIndexedSetCowTxn g = currentNamedGraph(name);
        return (g == null) ? null : g.readView();
    }

    private Map<Node, CowStore> capturedNamedGraphViews() {
        DsTxnState t = activeTxn.get();
        if (t == null) return null;
        return t.capturedNamedViews;
    }

    /**
     * Ensure the default graph is enlisted under a write transaction,
     * starting (or promoting) its per-graph transaction if needed. Always
     * returns the {@link #defaultGraph} instance.
     */
    private GraphMemIndexedSetCowTxn defaultGraphForWrite() {
        DsTxnState t = require();
        if (t.mode != ReadWrite.WRITE && !promote())
            throw new JenaTransactionException(
                    "Cannot write inside a non-promotable READ transaction");
        if (!t.defaultEnlisted) {
            startPerGraphWrite(defaultGraph, "default");
            t.defaultEnlisted = true;
        }
        t.dirty = true;
        return defaultGraph;
    }

    /**
     * Ensure {@code name} resolves to a live named graph under a write
     * transaction, creating a fresh one if necessary and enlisting it so
     * its write transaction is committed (or aborted) with the dataset's.
     */
    private GraphMemIndexedSetCowTxn namedGraphForWrite(Node name) {
        DsTxnState t = require();
        if (t.mode != ReadWrite.WRITE && !promote())
            throw new JenaTransactionException(
                    "Cannot write inside a non-promotable READ transaction");
        GraphMemIndexedSetCowTxn g = lookupNamedForCurrentTxn(t, name);
        if (g == null) {
            g = newGraph();
            if (t.additions == null) t.additions = new HashMap<>();
            t.additions.put(name, g);
            if (t.removals != null) t.removals.remove(name);
        }
        enlistNamedForWrite(t, name, g);
        t.dirty = true;
        return g;
    }

    /**
     * Same as {@link #namedGraphForWrite(Node)} but returns {@code null}
     * when the named graph doesn't exist. Used by deletes which should be
     * a no-op against a non-existent graph.
     */
    private GraphMemIndexedSetCowTxn namedGraphForWriteIfExists(Node name) {
        DsTxnState t = require();
        if (t.mode != ReadWrite.WRITE && !promote())
            throw new JenaTransactionException(
                    "Cannot write inside a non-promotable READ transaction");
        GraphMemIndexedSetCowTxn g = lookupNamedForCurrentTxn(t, name);
        if (g == null) return null;
        enlistNamedForWrite(t, name, g);
        t.dirty = true;
        return g;
    }

    private GraphMemIndexedSetCowTxn lookupNamedForCurrentTxn(DsTxnState t, Node name) {
        if (t.additions != null) {
            GraphMemIndexedSetCowTxn g = t.additions.get(name);
            if (g != null) return g;
        }
        if (t.removals != null && t.removals.contains(name))
            return null;
        return t.pinnedNamed.get(name);
    }

    private void enlistNamedForWrite(DsTxnState t, Node name, GraphMemIndexedSetCowTxn g) {
        if (t.enlistedNamed == null) t.enlistedNamed = new HashSet<>();
        if (!t.enlistedNamed.add(name)) return;
        startPerGraphWrite(g, name.toString());
    }

    /**
     * Start (or promote) a per-graph WRITE transaction on {@code g}. The
     * graph must be either not in a transaction (a fresh addition or a
     * graph we have not yet touched in this dataset txn) or in READ mode
     * via a prior {@code begin(READ_PROMOTE)} that we now promote in
     * place.
     */
    private void startPerGraphWrite(GraphMemIndexedSetCowTxn g, String label) {
        if (!g.isInTransaction()) {
            g.begin(TxnType.WRITE);
        } else if (g.transactionMode() == ReadWrite.READ) {
            if (!g.promote())
                throw new JenaTransactionException(
                        "Per-graph promotion failed for " + label);
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
            try {
                s.mode = ReadWrite.WRITE;
                s.pinnedNamed = namedTopology;
                s.startGeneration = generation.get();
                // Per-graph WRITE transactions are started lazily on first write.
                activeTxn.set(s);
            } catch (Throwable th) {
                datasetWriteLock.unlock();
                throw th;
            }
        } else {
            s.mode = ReadWrite.READ;
            publicationLock.readLock().lock();
            try {
                s.pinnedNamed = namedTopology;
                s.startGeneration = generation.get();
                // Capture the default graph's view atomically with the named
                // graph views below. The defaultGraph instance manages its
                // own per-thread txn state, so we start a READ on it too.
                beginAllPerGraphReads(type, s);
                activeTxn.set(s);
            } finally {
                publicationLock.readLock().unlock();
            }
        }
    }

    /**
     * Open a per-graph READ transaction on the default graph and every
     * pinned named graph, capturing their current snapshots into
     * {@code s.capturedDefaultView} / {@code s.capturedNamedViews}. If any
     * per-graph {@code begin} throws partway through, every per-graph txn
     * we already opened is {@code end()}-ed before the original exception
     * propagates — so the caller is left with no leaked per-graph state on
     * this thread.
     */
    private void beginAllPerGraphReads(TxnType type, DsTxnState s) {
        boolean defaultStarted = false;
        Map<Node, GraphMemIndexedSetCowTxn> started = null;
        try {
            defaultGraph.begin(type);
            defaultStarted = true;
            s.capturedDefaultView = defaultGraph.readView();
            // Eager per-graph begin on every named graph: each captures
            // its current published snapshot, recorded in
            // capturedNamedViews so that subsequent cross-graph
            // operations can dispatch to ForkJoinPool workers without
            // consulting the per-thread state on each call.
            Map<Node, CowStore> views = new HashMap<>(s.pinnedNamed.graphs().size());
            started = new HashMap<>(s.pinnedNamed.graphs().size());
            for (Map.Entry<Node, GraphMemIndexedSetCowTxn> e : s.pinnedNamed.graphs().entrySet()) {
                GraphMemIndexedSetCowTxn g = e.getValue();
                g.begin(type);
                started.put(e.getKey(), g);
                views.put(e.getKey(), g.readView());
            }
            s.capturedNamedViews = views;
        } catch (Throwable th) {
            if (started != null) {
                for (GraphMemIndexedSetCowTxn g : started.values()) {
                    try { g.end(); } catch (Throwable th2) { th.addSuppressed(th2); }
                }
            }
            if (defaultStarted) {
                try { defaultGraph.end(); } catch (Throwable th2) { th.addSuppressed(th2); }
            }
            throw th;
        }
    }

    @Override
    public boolean promote(Promote mode) {
        DsTxnState t = require();
        if (t.mode == ReadWrite.WRITE) return true;
        if (t.type == TxnType.READ)
            return false;

        if (mode == Promote.READ_COMMITTED) {
            datasetWriteLock.lock();
            // End per-graph READ txns; re-pin to latest published. Per-graph
            // read state ends now; subsequent reads route through the live
            // published view (stable because we hold datasetWriteLock), and
            // writes lazy-fork as usual.
            defaultGraph.end();
            for (GraphMemIndexedSetCowTxn g : t.pinnedNamed.graphs().values()) g.end();
            t.pinnedNamed = namedTopology;
            t.startGeneration = generation.get();
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
            // ISOLATED: keep per-graph READ txns alive; they get promoted to
            // WRITE on first per-graph write via startPerGraphWrite.
        }
        // After any promote, captured views are stale on graphs we'll
        // write to (writer's working copy must be visible). Drop them to
        // force cross-graph operations onto the per-thread path, which
        // correctly composes reads of the writer's working copy with
        // reads of the captured published snapshots on the per-graph
        // TxnState.
        t.capturedDefaultView = null;
        t.capturedNamedViews = null;
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
                if (t.defaultEnlisted) defaultGraph.commit();
                if (t.enlistedNamed != null) {
                    for (Node name : t.enlistedNamed) {
                        GraphMemIndexedSetCowTxn g = resolveEnlistedNamed(t, name);
                        if (g != null) g.commit();
                    }
                }
                // If we got here from a promote(READ_*PROMOTE), per-graph READ
                // transactions were started eagerly at dataset begin and are
                // still open on every pinned graph we did not write to. End
                // them now so the next dataset txn on this thread does not
                // hit "nested transactions" on those graphs.
                if (t.type != TxnType.WRITE)
                    endNonEnlistedPinned(t);
                if (t.dirty)
                    namedTopology = buildNamedTopology(t);
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
     * End the per-graph READ transaction on every graph in {@code pinnedNamed}
     * that was not enlisted for write, plus the default graph if it was not
     * enlisted. Called from commitWrite/abortWrite when the dataset txn was
     * promoted from a READ variant — so per-graph READ txns were opened by
     * {@link #begin(TxnType)} but never converted to WRITE. Safe to call when
     * {@code t.enlistedNamed} is null; {@code end()} is a no-op if the
     * per-graph activeTxn slot is empty.
     */
    private void endNonEnlistedPinned(DsTxnState t) {
        if (!t.defaultEnlisted) defaultGraph.end();
        Set<Node> enlisted = t.enlistedNamed;
        for (Map.Entry<Node, GraphMemIndexedSetCowTxn> e : t.pinnedNamed.graphs().entrySet()) {
            if (enlisted != null && enlisted.contains(e.getKey())) continue;
            e.getValue().end();
        }
    }

    /**
     * Build the new published named-graph topology by applying this write
     * transaction's additions and removals to the pinned topology. Returns
     * the pinned topology unchanged when nothing about the named-graph set
     * changed.
     */
    private GraphTopology buildNamedTopology(DsTxnState t) {
        if ((t.additions == null || t.additions.isEmpty())
                && (t.removals == null || t.removals.isEmpty()))
            return t.pinnedNamed;
        Map<Node, GraphMemIndexedSetCowTxn> next = new HashMap<>(t.pinnedNamed.graphs());
        if (t.additions != null) next.putAll(t.additions);
        if (t.removals != null) next.keySet().removeAll(t.removals);
        return new GraphTopology(Map.copyOf(next));
    }

    private GraphMemIndexedSetCowTxn resolveEnlistedNamed(DsTxnState t, Node name) {
        if (t.additions != null) {
            GraphMemIndexedSetCowTxn g = t.additions.get(name);
            if (g != null) return g;
        }
        return t.pinnedNamed.get(name);
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
            if (t.defaultEnlisted) defaultGraph.abort();
            if (t.enlistedNamed != null) {
                for (Node name : t.enlistedNamed) {
                    GraphMemIndexedSetCowTxn g = resolveEnlistedNamed(t, name);
                    if (g != null) g.abort();
                }
            }
            // Mirror commitWrite: release per-graph READ txns opened by
            // begin(READ_*PROMOTE) that we never promoted to WRITE.
            if (t.type != TxnType.WRITE)
                endNonEnlistedPinned(t);
        } finally {
            datasetWriteLock.unlock();
            activeTxn.remove();
        }
    }

    private void endReadTxn(DsTxnState t) {
        try {
            defaultGraph.end();
            for (GraphMemIndexedSetCowTxn g : t.pinnedNamed.graphs().values()) g.end();
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
        mutate(() -> defaultGraphForWrite().add(Triple.create(s, p, o)));
    }

    @Override
    protected void addToNamedGraph(Node g, Node s, Node p, Node o) {
        mutate(() -> namedGraphForWrite(g).add(Triple.create(s, p, o)));
    }

    @Override
    protected void deleteFromDftGraph(Node s, Node p, Node o) {
        mutate(() -> defaultGraphForWrite().delete(Triple.create(s, p, o)));
    }

    @Override
    protected void deleteFromNamedGraph(Node g, Node s, Node p, Node o) {
        mutate(() -> {
            GraphMemIndexedSetCowTxn graph = namedGraphForWriteIfExists(g);
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

    @Override
    public void removeGraph(Node graphName) {
        mutate(() -> doRemoveGraph(graphName));
    }

    /**
     * Drop a named graph from the dataset. The default graph cannot be
     * dropped: requests to remove it clear its contents in place, matching
     * {@code DatasetGraphInMemory}. For a named graph, the dataset's
     * topology no longer contains the graph after commit; on abort, the
     * graph remains as it was.
     */
    private void doRemoveGraph(Node graphName) {
        DsTxnState t = require();
        if (t.mode != ReadWrite.WRITE && !promote())
            throw new JenaTransactionException(
                    "Cannot remove graph in a non-promotable READ transaction");
        if (Quad.isDefaultGraph(graphName)) {
            defaultGraphForWrite().clear();
            t.dirty = true;
            return;
        }
        // Drop any addition made earlier in this txn. additions are always
        // enlisted (see namedGraphForWrite), so abort the per-graph WRITE
        // we started for it and forget the enlistment.
        if (t.additions != null) {
            GraphMemIndexedSetCowTxn added = t.additions.remove(graphName);
            if (added != null) {
                if (t.enlistedNamed != null) t.enlistedNamed.remove(graphName);
                added.abort();
                t.dirty = true;
                return;
            }
        }
        // Otherwise this is a removal of a pre-existing pinned graph.
        if (!t.pinnedNamed.contains(graphName))
            return;     // no such graph; nothing to remove
        if (t.removals == null) t.removals = new HashSet<>();
        t.removals.add(graphName);
        // If the pinned graph was enlisted for write before being removed,
        // discard the per-graph write so it isn't republished. The per-graph
        // READ txn that was started at dataset begin remains and will be
        // ended by commitWrite/abortWrite below.
        if (t.enlistedNamed != null && t.enlistedNamed.remove(graphName)) {
            GraphMemIndexedSetCowTxn pinned = t.pinnedNamed.get(graphName);
            if (pinned != null) pinned.abort();
        }
        t.dirty = true;
    }

    // --- Find paths -----------------------------------------------------------

    @Override
    protected Iterator<Quad> findInDftGraph(Node s, Node p, Node o) {
        return access(() ->
                G.triples2quadsDftGraph(currentDefaultView().find(Triple.createMatch(s, p, o))));
    }

    @Override
    protected Iterator<Quad> findInSpecificNamedGraph(Node g, Node s, Node p, Node o) {
        return access(() -> {
            CowStore view = currentNamedView(g);
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
                // txn ends. Under ForkMode.PARALLEL with enough named graphs,
                // build the per-graph iterators in parallel and chain them
                // with `andThen` — iteration of the chain stays sequential
                // so callers can short-circuit normally; what's parallelised
                // is the (often expensive) per-graph find() call itself.
                if (shouldParallelizeCrossGraph(views))
                    return parallelFindInNamedGraphs(views, match);
                return sequentialFindInNamedGraphs(views, match);
            }
            // WRITE path (or post-promote): the per-thread route. The
            // writer's working copy is visible on enlisted graphs; reads
            // on non-enlisted graphs still see their captured per-graph
            // snapshots via the per-graph TxnState.
            return sequentialFindInNamedGraphsByThread(match);
        });
    }

    private boolean shouldParallelizeCrossGraph(Map<Node, CowStore> views) {
        if (forkMode != GraphMemIndexedSetCowTxn.ForkMode.PARALLEL) return false;
        return views.size() >= PARALLEL_CROSS_GRAPH_THRESHOLD;
    }

    /**
     * Build per-graph iterators in parallel on the common ForkJoinPool and
     * chain them with {@link ExtendedIterator#andThen}. Iteration of the
     * chain is sequential; only the (often-expensive, e.g. LAZY index build)
     * construction of the underlying iterators is parallelised.
     */
    private ExtendedIterator<Quad> parallelFindInNamedGraphs(Map<Node, CowStore> views, Triple match) {
        List<ExtendedIterator<Quad>> perGraph = views.entrySet().parallelStream()
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
            Node name = e.getKey();
            ExtendedIterator<Quad> it = e.getValue().find(match).mapWith(t -> Quad.create(name, t));
            result = result.andThen(it);
        }
        return result;
    }

    /**
     * Per-thread sequential fallback. Used when capturedNamedViews is
     * unavailable (no transaction, WRITE transaction, or post-promote).
     * Lazy: {@code find()} on graph K is delayed until the caller has
     * exhausted graphs 1..K-1, so short-circuit consumers like {@code ASK}
     * pay for only the graphs they touch.
     */
    private Iterator<Quad> sequentialFindInNamedGraphsByThread(Triple match) {
        Map<Node, GraphMemIndexedSetCowTxn> topo = currentNamedTopology();
        Iterator<Map.Entry<Node, GraphMemIndexedSetCowTxn>> entries = topo.entrySet().iterator();
        return Iter.flatMap(entries, e -> {
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
     * Specialised {@link org.apache.jena.sparql.core.DatasetGraph#stream}
     * that routes cross-graph queries through the same captured-view path
     * as {@link #findInAnyNamedGraphs} and, under
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
            // No access() wrap here: findNG() is itself access()-wrapped.
            return Iter.asStream(findNG(g, s, p, o));
        }

        // Specific default graph (defaultGraphIRI or defaultGraphNodeGenerated).
        if (Quad.isDefaultGraph(g)) {
            return access(() ->
                    currentDefaultView().stream(match)
                            .map(t -> Quad.create(Quad.defaultGraphIRI, t)));
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
                Stream<Quad> dft = currentDefaultView().stream(match)
                        .map(t -> Quad.create(Quad.defaultGraphIRI, t));
                return Stream.concat(dft, namedGraphs);
            });
        }

        // Specific named graph.
        return access(() -> {
            CowStore view = currentNamedView(g);
            if (view == null) return Stream.<Quad>empty();
            return view.stream(match).map(t -> Quad.create(g, t));
        });
    }

    private Stream<Quad> parallelStreamInNamedGraphs(Map<Node, CowStore> views, Triple match) {
        // Build per-graph streams in parallel; outer stream is sequential.
        List<Stream<Quad>> perGraph = views.entrySet().parallelStream()
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
                .flatMap(e -> {
                    Node name = e.getKey();
                    return e.getValue().stream(match).map(t -> Quad.create(name, t));
                });
    }

    private Stream<Quad> streamInNamedGraphsByThread(Triple match) {
        return currentNamedTopology().entrySet().stream()
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
        return access(() ->
                currentNamedTopology().entrySet().stream()
                        // Empty named graphs are filtered out — matches
                        // DatasetGraphInMemory, where an empty named graph is
                        // indistinguishable from one that was never added.
                        .filter(e -> !e.getValue().isEmpty())
                        .map(Map.Entry::getKey)
                        .iterator());
    }

    @Override
    public boolean containsGraph(Node graphNode) {
        if (Quad.isDefaultGraph(graphNode)) return true;
        if (Quad.isUnionGraph(graphNode)) return true;
        return access(() -> {
            GraphMemIndexedSetCowTxn g = currentNamedGraph(graphNode);
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
     * and asks {@code hasNext()}. Each per-graph view's
     * {@link CowStore#contains(Triple)} is dramatically faster for the
     * (s,p,o)-concrete case (a single hash lookup with EAGER indexing) and
     * still short-circuits at the dataset level for wildcards.
     */
    @Override
    public boolean contains(Node g, Node s, Node p, Node o) {
        Triple match = Triple.createMatch(s, p, o);
        return access(() -> {
            if (Quad.isDefaultGraph(g)) {
                return currentDefaultView().contains(match);
            }
            if (Quad.isUnionGraph(g)) {
                return anyNamedGraphContains(match);
            }
            if (g == null || Node.ANY.equals(g)) {
                // Wildcard: default graph plus any named graph.
                if (currentDefaultView().contains(match)) return true;
                return anyNamedGraphContains(match);
            }
            CowStore view = currentNamedView(g);
            return view != null && view.contains(match);
        });
    }

    private boolean anyNamedGraphContains(Triple match) {
        Map<Node, CowStore> views = capturedNamedGraphViews();
        if (views != null) {
            for (CowStore v : views.values()) {
                if (v.contains(match)) return true;
            }
            return false;
        }
        // Per-thread fallback (WRITE txn or auto-wrap outside a txn).
        for (GraphMemIndexedSetCowTxn g : currentNamedTopology().values()) {
            if (g.contains(match)) return true;
        }
        return false;
    }

    /**
     * Whether the dataset has no quads in any graph. The default
     * implementation builds a wildcard {@code find(...)} iterator just to
     * check {@code hasNext()}; we ask each captured view's
     * {@link CowStore#isEmpty()} directly, starting with the default
     * graph, and short-circuit on the first non-empty graph.
     */
    @Override
    public boolean isEmpty() {
        return access(() -> {
            if (!currentDefaultView().isEmpty()) return false;
            Map<Node, CowStore> views = capturedNamedGraphViews();
            if (views != null) {
                for (CowStore v : views.values()) {
                    if (!v.isEmpty()) return false;
                }
                return true;
            }
            for (GraphMemIndexedSetCowTxn g : currentNamedTopology().values()) {
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
            for (GraphMemIndexedSetCowTxn g : currentNamedTopology().values()) {
                if (!g.isEmpty()) n++;
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
    private <T> T access(Supplier<T> source) {
        return isInTransaction() ? source.get() : Txn.calculateRead(this, source);
    }
}

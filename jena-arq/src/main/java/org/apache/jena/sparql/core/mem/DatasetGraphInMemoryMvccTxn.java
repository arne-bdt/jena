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

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.IndexingStrategy;
import org.apache.jena.mem.store.mvcc.MvccTripleStore;
import org.apache.jena.mem.store.mvcc.MvccVersionControl;
import org.apache.jena.mem.store.mvcc.MvccWriteTxn;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapStd;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.GraphView;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.DatasetGraphTriplesQuads;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.system.G;
import org.apache.jena.system.Txn;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.NullIterator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Transactional, MVCC variant of an in-memory {@link org.apache.jena.sparql.core.DatasetGraph}.
 * It composes one {@link MvccTripleStore} per graph (a default graph plus a map
 * of named graphs), all sharing a single {@link MvccVersionControl} timeline, so
 * the whole dataset commits on one clock.
 * <p>
 * Unlike the copy-on-write {@link DatasetGraphInMemoryCowTxn}, it captures no
 * per-graph snapshots at {@code begin}: a read transaction pins one global
 * version and every read filters each graph's current generation at that version.
 * Because slots are version-stamped and never removed before a vacuum that
 * respects active readers, reading the latest generation filtered at an older
 * version yields exactly the snapshot at that version. {@code begin} is therefore
 * O(1) regardless of the number of named graphs.
 * <p>
 * Concurrency: one writer at a time (the shared writer lock), lock-free readers.
 * A write transaction buffers per-graph overlays applied at commit; abort discards
 * them with no undo.
 */
public class DatasetGraphInMemoryMvccTxn extends DatasetGraphTriplesQuads implements Transactional {

    private final MvccVersionControl vc = new MvccVersionControl();
    private final IndexingStrategy indexingStrategy;
    private final MvccTripleStore.ParallelMode parallelMode;

    /**
     * Minimum number of named graphs before a cross-graph read collects its
     * per-graph iterators in parallel (only under {@link MvccTripleStore.ParallelMode#PARALLEL}).
     * Mirrors the copy-on-write dataset's threshold.
     */
    private static final int PARALLEL_CROSS_GRAPH_THRESHOLD = 16;
    private final MvccTripleStore defaultStore;
    private final ConcurrentHashMap<Node, MvccTripleStore> namedStores = new ConcurrentHashMap<>();
    private final PrefixMap prefixes = new PrefixMapStd();
    private final ThreadLocal<DsTxnState> activeTxn = new ThreadLocal<>();
    /** Cached direct-to-store view of the default graph (see {@link DefaultGraphView}). */
    private final Graph defaultGraph;

    private static final class DsTxnState {
        TxnType type;
        ReadWrite mode;
        /** Pinned version: the read snapshot (READ) or the committed base (WRITE). */
        long version;
        /** Write version (committed base + 1); valid under WRITE. */
        long writeVersion;
        /** The reader pin held for vacuum tracking, or -1 if none (WRITE / promoted). */
        long readPin = -1;
        /** Per-store overlays, created lazily on first write to each store. */
        Map<MvccTripleStore, MvccWriteTxn> writeTxns;
    }

    public DatasetGraphInMemoryMvccTxn() {
        this(IndexingStrategy.EAGER);
    }

    public DatasetGraphInMemoryMvccTxn(IndexingStrategy indexingStrategy) {
        this(indexingStrategy, MvccTripleStore.ParallelMode.SEQUENTIAL);
    }

    /**
     * Create a dataset with the given parallel mode — the MVCC analogue of the
     * copy-on-write dataset's fork mode. Under
     * {@link MvccTripleStore.ParallelMode#PARALLEL} the per-graph MVCC stores
     * parallelise their implicit index builds, and cross-graph reads over at least
     * {@value #PARALLEL_CROSS_GRAPH_THRESHOLD} named graphs collect their per-graph
     * iterators in parallel.
     *
     * @param indexingStrategy the indexing strategy for every per-graph store
     * @param parallelMode     the parallel mode for every per-graph store and for
     *                         cross-graph reads
     */
    public DatasetGraphInMemoryMvccTxn(IndexingStrategy indexingStrategy, MvccTripleStore.ParallelMode parallelMode) {
        this.indexingStrategy = indexingStrategy;
        this.parallelMode = parallelMode;
        this.defaultStore = new MvccTripleStore(indexingStrategy, vc, parallelMode);
        this.defaultGraph = new DefaultGraphView();
    }

    /** @return the parallel mode this dataset was created with. */
    public MvccTripleStore.ParallelMode getParallelMode() {
        return parallelMode;
    }

    // --- Transactional --------------------------------------------------------

    @Override public boolean supportsTransactions()     { return true; }
    @Override public boolean supportsTransactionAbort() { return true; }

    @Override
    public void begin(TxnType type) {
        if (activeTxn.get() != null) {
            throw new JenaTransactionException("Nested transactions are not supported");
        }
        final DsTxnState s = new DsTxnState();
        s.type = type;
        if (type == TxnType.WRITE) {
            vc.lockWriter();
            try {
                s.mode = ReadWrite.WRITE;
                s.version = vc.committedVersion();
                s.writeVersion = vc.nextWriteVersion();
                s.writeTxns = new HashMap<>();
                activeTxn.set(s);
            } catch (Throwable th) {
                vc.unlockWriter();
                throw th;
            }
        } else {
            s.mode = ReadWrite.READ;
            // Pin the version for vacuum tracking, race-free against commit/vacuum.
            s.readPin = vc.pinReader();
            s.version = s.readPin;
            activeTxn.set(s);
        }
    }

    @Override
    public boolean promote(Promote mode) {
        final DsTxnState t = require();
        if (t.mode == ReadWrite.WRITE) {
            return true;
        }
        if (t.type == TxnType.READ) {
            return false;
        }
        if (mode == Promote.READ_COMMITTED) {
            vc.lockWriter();
        } else { // ISOLATED
            if (t.version != vc.committedVersion()) {
                return false;
            }
            vc.lockWriter();
            if (t.version != vc.committedVersion()) {
                vc.unlockWriter();
                return false;
            }
        }
        // Becoming a writer: release the read pin (the writer lock now provides
        // exclusion; reads route through the overlay / committed base).
        if (t.readPin >= 0) {
            vc.unpinReader(t.readPin);
            t.readPin = -1;
        }
        t.mode = ReadWrite.WRITE;
        t.version = vc.committedVersion();
        t.writeVersion = vc.nextWriteVersion();
        t.writeTxns = new HashMap<>();
        return true;
    }

    @Override
    public void commit() {
        final DsTxnState t = require();
        try {
            if (t.mode == ReadWrite.WRITE) {
                for (Map.Entry<MvccTripleStore, MvccWriteTxn> e : t.writeTxns.entrySet()) {
                    if (e.getValue().hasChanges()) {
                        e.getKey().commit(e.getValue());
                    }
                }
                // Advance the shared commit counter once (even for an empty write
                // commit) so ISOLATED promotion observes that a writer committed.
                vc.publish(t.writeVersion);
            }
        } finally {
            finish(t);
        }
    }

    @Override
    public void abort() {
        final DsTxnState t = require();
        finish(t); // per-store overlays discarded; nothing applied
    }

    @Override
    public void end() {
        final DsTxnState t = activeTxn.get();
        if (t == null) {
            return;
        }
        if (t.mode == ReadWrite.WRITE) {
            // Reaching end() in WRITE mode without commit()/abort() is a
            // programming error: discard overlays (releasing the writer slot)
            // and surface the mistake, mirroring DatasetGraphInMemory.
            finish(t);
            throw new JenaTransactionException(
                    "Write transaction was not committed or aborted before end()");
        }
        finish(t);
    }

    private void finish(DsTxnState t) {
        if (t.mode == ReadWrite.WRITE) {
            vc.unlockWriter();
        }
        if (t.readPin >= 0) {
            vc.unpinReader(t.readPin);
            t.readPin = -1;
        }
        activeTxn.remove();
    }

    @Override public boolean isInTransaction() { return activeTxn.get() != null; }

    @Override
    public ReadWrite transactionMode() {
        final DsTxnState t = activeTxn.get();
        return t == null ? null : t.mode;
    }

    @Override
    public TxnType transactionType() {
        final DsTxnState t = activeTxn.get();
        return t == null ? null : t.type;
    }

    private DsTxnState require() {
        final DsTxnState t = activeTxn.get();
        if (t == null) {
            throw new JenaTransactionException("Not in a transaction");
        }
        return t;
    }

    /** Run a read inside a transaction (auto-wrapping in a READ txn if needed). */
    private <T> T access(Supplier<T> source) {
        return isInTransaction() ? source.get() : Txn.calculateRead(this, source);
    }

    /** Run a mutation inside a transaction (auto-wrapping in a WRITE txn if needed). */
    private void mutate(Runnable r) {
        if (isInTransaction()) {
            r.run();
        } else {
            Txn.executeWrite(this, r);
        }
    }

    // --- Per-store read/write routing -----------------------------------------

    private MvccWriteTxn writeTxnFor(MvccTripleStore store) {
        final DsTxnState t = require();
        if (t.mode != ReadWrite.WRITE && !promote()) {
            throw new JenaTransactionException(
                    "Cannot write in a non-promotable READ transaction");
        }
        return t.writeTxns.computeIfAbsent(store, s -> store.openWriteTxn());
    }

    /** A read iterator over one store, honouring read-your-writes under WRITE. */
    private ExtendedIterator<Triple> storeFind(MvccTripleStore store, Triple match, DsTxnState t) {
        if (t.mode == ReadWrite.WRITE) {
            final MvccWriteTxn w = t.writeTxns.get(store);
            if (w != null) {
                return w.find(match);
            }
        }
        return store.findAt(t.version, match);
    }

    private boolean storeIsEmpty(MvccTripleStore store, DsTxnState t) {
        if (t.mode == ReadWrite.WRITE) {
            final MvccWriteTxn w = t.writeTxns.get(store);
            if (w != null) {
                return w.isEmpty();
            }
        }
        return store.isEmptyAt(t.version);
    }

    /** A read membership test over one store, honouring read-your-writes under WRITE. */
    private boolean storeContains(MvccTripleStore store, Triple match, DsTxnState t) {
        if (t.mode == ReadWrite.WRITE) {
            final MvccWriteTxn w = t.writeTxns.get(store);
            if (w != null) {
                return w.contains(match);
            }
        }
        return store.containsAt(t.version, match);
    }

    /** A visible-triple count over one store, honouring read-your-writes under WRITE. */
    private int storeCount(MvccTripleStore store, DsTxnState t) {
        if (t.mode == ReadWrite.WRITE) {
            final MvccWriteTxn w = t.writeTxns.get(store);
            if (w != null) {
                return w.count();
            }
        }
        return store.countAt(t.version);
    }

    /** A read stream over one store, honouring read-your-writes under WRITE. */
    private Stream<Triple> storeStream(MvccTripleStore store, Triple match, DsTxnState t) {
        if (t.mode == ReadWrite.WRITE) {
            final MvccWriteTxn w = t.writeTxns.get(store);
            if (w != null) {
                return w.stream(match);
            }
        }
        return store.streamAt(t.version, match);
    }

    // --- Mutation routing (DatasetGraphTriplesQuads) --------------------------

    @Override
    protected void addToDftGraph(Node s, Node p, Node o) {
        mutate(() -> writeTxnFor(defaultStore).add(Triple.create(s, p, o)));
    }

    @Override
    protected void addToNamedGraph(Node g, Node s, Node p, Node o) {
        mutate(() -> {
            final MvccTripleStore store = namedStores.computeIfAbsent(
                    g, k -> new MvccTripleStore(indexingStrategy, vc, parallelMode));
            writeTxnFor(store).add(Triple.create(s, p, o));
        });
    }

    @Override
    protected void deleteFromDftGraph(Node s, Node p, Node o) {
        mutate(() -> writeTxnFor(defaultStore).remove(Triple.create(s, p, o)));
    }

    @Override
    protected void deleteFromNamedGraph(Node g, Node s, Node p, Node o) {
        mutate(() -> {
            final MvccTripleStore store = namedStores.get(g);
            if (store != null) {
                writeTxnFor(store).remove(Triple.create(s, p, o));
            }
        });
    }

    @Override
    public void removeGraph(Node graphName) {
        mutate(() -> {
            if (Quad.isDefaultGraph(graphName)) {
                clearStore(defaultStore);
            } else {
                final MvccTripleStore store = namedStores.get(graphName);
                if (store != null) {
                    clearStore(store);
                }
            }
        });
    }

    /** Remove every triple currently visible in {@code store} within this txn. */
    private void clearStore(MvccTripleStore store) {
        final MvccWriteTxn w = writeTxnFor(store);
        for (Triple t : w.find(MATCH_ALL).toList()) {
            w.remove(t);
        }
    }

    private static final Triple MATCH_ALL = Triple.createMatch(null, null, null);

    // --- Find paths -----------------------------------------------------------

    @Override
    protected Iterator<Quad> findInDftGraph(Node s, Node p, Node o) {
        final Triple match = Triple.createMatch(s, p, o);
        return access(() -> G.triples2quadsDftGraph(storeFind(defaultStore, match, require())));
    }

    @Override
    protected Iterator<Quad> findInSpecificNamedGraph(Node g, Node s, Node p, Node o) {
        final Triple match = Triple.createMatch(s, p, o);
        return access(() -> {
            final MvccTripleStore store = namedStores.get(g);
            if (store == null) {
                return Iter.<Quad>nullIterator();
            }
            return storeFind(store, match, require()).mapWith(t -> Quad.create(g, t));
        });
    }

    @Override
    protected Iterator<Quad> findInAnyNamedGraphs(Node s, Node p, Node o) {
        final Triple match = Triple.createMatch(s, p, o);
        return access(() -> {
            final DsTxnState t = require();
            if (shouldParallelizeCrossGraph(t)) {
                return parallelFindInNamedGraphs(match, t);
            }
            final Iterator<Map.Entry<Node, MvccTripleStore>> entries =
                    namedStores.entrySet().iterator();
            return Iter.flatMap(entries, e ->
                    storeFind(e.getValue(), match, t).mapWith(tr -> Quad.create(e.getKey(), tr)));
        });
    }

    /**
     * Cross-graph collection may go parallel only for read views: a WRITE
     * transaction's per-store overlays ({@link MvccWriteTxn}) are single-threaded,
     * so their reads must stay on the writer thread. It is also gated on
     * {@link MvccTripleStore.ParallelMode#PARALLEL} and a minimum graph count.
     */
    private boolean shouldParallelizeCrossGraph(DsTxnState t) {
        return parallelMode == MvccTripleStore.ParallelMode.PARALLEL
                && t.mode != ReadWrite.WRITE
                && namedStores.size() >= PARALLEL_CROSS_GRAPH_THRESHOLD;
    }

    /**
     * Build each named graph's per-graph iterator in parallel on the common pool and
     * chain them. Iteration of the chain stays lazy and sequential, so short-circuit
     * consumers still stop early — only the (potentially scanning) per-graph
     * {@code find} construction is parallelised. Read views only (see
     * {@link #shouldParallelizeCrossGraph}); {@code storeFind} then routes through
     * the lock-free {@link MvccTripleStore#findAt}, which is thread-safe.
     */
    private Iterator<Quad> parallelFindInNamedGraphs(Triple match, DsTxnState t) {
        final List<ExtendedIterator<Quad>> perGraph = namedStores.entrySet().parallelStream()
                .map(e -> storeFind(e.getValue(), match, t).mapWith(tr -> Quad.create(e.getKey(), tr)))
                .collect(Collectors.toList());
        ExtendedIterator<Quad> result = NiceIterator.emptyIterator();
        for (ExtendedIterator<Quad> it : perGraph) {
            result = result.andThen(it);
        }
        return result;
    }

    // --- Graph container view -------------------------------------------------

    @Override
    public Graph getDefaultGraph() {
        return defaultGraph;
    }

    @Override
    public Graph getGraph(Node graphNode) {
        if (graphNode == null || Quad.isDefaultGraph(graphNode)) {
            return defaultGraph;
        }
        if (Quad.isUnionGraph(graphNode)) {
            // The union is a cross-graph read with dedup: leave it to the generic view.
            return GraphView.createUnionGraph(this);
        }
        return new NamedGraphView(graphNode);
    }

    @Override
    public Graph getUnionGraph() {
        return GraphView.createUnionGraph(this);
    }

    /**
     * A triple-level view of one of the dataset's graphs that reads and writes the
     * backing {@link MvccTripleStore} <em>directly</em>. It extends {@link GraphView}, so
     * it stays a {@code GraphView} whose {@link GraphView#getDataset()} is this dataset
     * (keeping the controlling {@link Transactional} reachable, exactly as the generic
     * view does); it only overrides the find / contains / size / stream / add / delete
     * hot paths to skip the per-triple {@code Triple}&harr;{@code Quad} mapping, the
     * {@code find()}-based {@code contains}, and the {@code find()}-based {@code size} of
     * {@code GraphBase}.
     * <p>
     * Transaction state is the dataset's per-thread {@link DsTxnState}: inside a dataset
     * transaction reads honour the write overlay (read-your-writes) under WRITE or the
     * version pinned at {@code begin} under READ; outside any transaction they read the
     * latest committed state through a transient view, mirroring
     * {@link GraphMemIndexedSetMvccTxn}. Writes route through {@link #mutate} /
     * {@link #writeTxnFor(MvccTripleStore)}, so a bare {@code add}/{@code delete} still
     * auto-wraps in a dataset write transaction.
     */
    private abstract class StoreBackedGraphView extends GraphView {

        StoreBackedGraphView(Node graphName) {
            super(DatasetGraphInMemoryMvccTxn.this, graphName);
        }

        /** The backing store, or {@code null} if this graph has none yet (a named graph never written). */
        protected abstract MvccTripleStore storeOrNull();

        /** The backing store to write into, created on demand. */
        protected abstract MvccTripleStore storeForWrite();

        /** Route a triple read to the overlay/snapshot in-txn, or a transient view out of txn. */
        private ExtendedIterator<Triple> doFind(Triple match) {
            final MvccTripleStore store = storeOrNull();
            if (store == null) {
                return NullIterator.instance();
            }
            final DsTxnState t = activeTxn.get();
            return (t == null)
                    ? store.transientReadView().find(match)
                    : storeFind(store, match, t);
        }

        @Override
        protected ExtendedIterator<Triple> graphBaseFind(Triple m) {
            return doFind(m == null ? MATCH_ALL : m);
        }

        @Override
        protected ExtendedIterator<Triple> graphBaseFind(Node s, Node p, Node o) {
            return doFind(Triple.createMatch(s, p, o));
        }

        @Override
        protected boolean graphBaseContains(Triple m) {
            final MvccTripleStore store = storeOrNull();
            if (store == null) {
                return false;
            }
            final DsTxnState t = activeTxn.get();
            return (t == null)
                    ? store.transientReadView().contains(m)
                    : storeContains(store, m, t);
        }

        @Override
        protected int graphBaseSize() {
            final MvccTripleStore store = storeOrNull();
            if (store == null) {
                return 0;
            }
            final DsTxnState t = activeTxn.get();
            return (t == null)
                    ? store.transientReadView().count()
                    : storeCount(store, t);
        }

        @Override
        public Stream<Triple> stream(Node s, Node p, Node o) {
            final MvccTripleStore store = storeOrNull();
            if (store == null) {
                return Stream.empty();
            }
            final Triple match = Triple.createMatch(s, p, o);
            final DsTxnState t = activeTxn.get();
            return (t == null)
                    ? store.transientReadView().stream(match)
                    : storeStream(store, match, t);
        }

        @Override
        public Stream<Triple> stream() {
            return stream(Node.ANY, Node.ANY, Node.ANY);
        }

        @Override
        public void performAdd(Triple t) {
            mutate(() -> writeTxnFor(storeForWrite()).add(t));
        }

        @Override
        public void performDelete(Triple t) {
            mutate(() -> {
                final MvccTripleStore store = storeOrNull();
                if (store != null) {
                    writeTxnFor(store).remove(t);
                }
            });
        }
    }

    /** The default graph, backed by the always-present {@link #defaultStore}. */
    private final class DefaultGraphView extends StoreBackedGraphView {
        private DefaultGraphView() {
            super(Quad.defaultGraphNodeGenerated);
        }

        @Override
        protected MvccTripleStore storeOrNull() {
            return defaultStore;
        }

        @Override
        protected MvccTripleStore storeForWrite() {
            return defaultStore;
        }
    }

    /**
     * A named graph, backed by its entry in {@link #namedStores}. The store is absent
     * until the graph is first written (mirroring {@link #addToNamedGraph}); reads of an
     * absent store are empty and a delete against it is a no-op.
     */
    private final class NamedGraphView extends StoreBackedGraphView {
        private final Node gname;

        private NamedGraphView(Node graphName) {
            super(graphName);
            this.gname = graphName;
        }

        @Override
        protected MvccTripleStore storeOrNull() {
            return namedStores.get(gname);
        }

        @Override
        protected MvccTripleStore storeForWrite() {
            return namedStores.computeIfAbsent(gname, k -> new MvccTripleStore(indexingStrategy, vc, parallelMode));
        }
    }

    @Override
    public Iterator<Node> listGraphNodes() {
        return access(() -> {
            final DsTxnState t = require();
            return namedStores.entrySet().stream()
                    .filter(e -> !storeIsEmpty(e.getValue(), t))
                    .map(Map.Entry::getKey)
                    .iterator();
        });
    }

    @Override
    public boolean containsGraph(Node graphNode) {
        if (Quad.isDefaultGraph(graphNode) || Quad.isUnionGraph(graphNode)) {
            return true;
        }
        return access(() -> {
            final MvccTripleStore store = namedStores.get(graphNode);
            return store != null && !storeIsEmpty(store, require());
        });
    }

    @Override
    public PrefixMap prefixes() {
        return prefixes;
    }

    /** {@inheritDoc} The dataset's "size" is the number of (non-empty) named graphs. */
    @Override
    public long size() {
        return access(() -> {
            final DsTxnState t = require();
            return namedStores.values().stream()
                    .filter(store -> !storeIsEmpty(store, t))
                    .count();
        });
    }

    @Override
    public boolean isEmpty() {
        return access(() -> {
            final DsTxnState t = require();
            if (!storeIsEmpty(defaultStore, t)) {
                return false;
            }
            for (MvccTripleStore store : namedStores.values()) {
                if (!storeIsEmpty(store, t)) {
                    return false;
                }
            }
            return true;
        });
    }

    @Override
    public void clear() {
        mutate(() -> {
            clearStore(defaultStore);
            for (MvccTripleStore store : namedStores.values()) {
                clearStore(store);
            }
        });
    }

    @Override
    public Stream<Quad> stream(Node g, Node s, Node p, Node o) {
        final Triple match = Triple.createMatch(s, p, o);
        // Default graph and concrete named graphs stream their store natively (the store's
        // own Stream), tagging each triple with its graph node — no find() iterator bridge.
        if (Quad.isDefaultGraph(g)) {
            return access(() -> storeStream(defaultStore, match, require())
                    .map(tr -> Quad.create(Quad.defaultGraphIRI, tr)));
        }
        if (!isWildcard(g) && !Quad.isUnionGraph(g)) {
            return access(() -> {
                final MvccTripleStore store = namedStores.get(g);
                return store == null
                        ? Stream.<Quad>empty()
                        : storeStream(store, match, require()).map(tr -> Quad.create(g, tr));
            });
        }
        if (Quad.isUnionGraph(g)) {
            // The union graph deduplicates triples across graphs: keep the find()-based path.
            return Iter.asStream(find(g, s, p, o));
        }
        // Wildcard graph term (null / Node.ANY): the default graph plus every named graph,
        // mirroring DatasetGraphBaseFind.find. Streams natively and — like the find path,
        // under ParallelMode.PARALLEL for a read view with enough named graphs — builds the
        // per-graph named streams in parallel rather than via the find() iterator bridge.
        return access(() -> {
            final DsTxnState t = require();
            final Stream<Quad> dft = storeStream(defaultStore, match, t)
                    .map(tr -> Quad.create(Quad.defaultGraphIRI, tr));
            return Stream.concat(dft, streamInNamedGraphs(match, t));
        });
    }

    /**
     * Stream every named graph's matching quads. Under
     * {@link MvccTripleStore.ParallelMode#PARALLEL} for a read view with at least
     * {@value #PARALLEL_CROSS_GRAPH_THRESHOLD} named graphs, the per-graph streams
     * are built in parallel on the common pool (the outer stream stays sequential,
     * so consumption is lazy); otherwise a sequential flat-map. Read views only —
     * see {@link #shouldParallelizeCrossGraph}.
     */
    private Stream<Quad> streamInNamedGraphs(Triple match, DsTxnState t) {
        if (shouldParallelizeCrossGraph(t)) {
            final List<Stream<Quad>> perGraph = namedStores.entrySet().parallelStream()
                    .map(e -> storeStream(e.getValue(), match, t).map(tr -> Quad.create(e.getKey(), tr)))
                    .collect(Collectors.toList());
            return perGraph.stream().flatMap(quadStream -> quadStream);
        }
        return namedStores.entrySet().stream()
                .flatMap(e -> storeStream(e.getValue(), match, t).map(tr -> Quad.create(e.getKey(), tr)));
    }

    @Override
    public void close() {
        super.close();
    }
}

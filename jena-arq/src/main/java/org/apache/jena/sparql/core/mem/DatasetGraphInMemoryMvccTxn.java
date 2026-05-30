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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
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
    private final MvccTripleStore defaultStore;
    private final ConcurrentHashMap<Node, MvccTripleStore> namedStores = new ConcurrentHashMap<>();
    private final PrefixMap prefixes = new PrefixMapStd();
    private final ThreadLocal<DsTxnState> activeTxn = new ThreadLocal<>();

    private static final class DsTxnState {
        TxnType type;
        ReadWrite mode;
        /** Pinned version: the read snapshot (READ) or the committed base (WRITE). */
        long version;
        /** Write version (committed base + 1); valid under WRITE. */
        long writeVersion;
        /** Per-store overlays, created lazily on first write to each store. */
        Map<MvccTripleStore, MvccWriteTxn> writeTxns;
    }

    public DatasetGraphInMemoryMvccTxn() {
        this(IndexingStrategy.EAGER);
    }

    public DatasetGraphInMemoryMvccTxn(IndexingStrategy indexingStrategy) {
        this.indexingStrategy = indexingStrategy;
        this.defaultStore = new MvccTripleStore(indexingStrategy, vc);
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
            s.version = vc.committedVersion();
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

    // --- Mutation routing (DatasetGraphTriplesQuads) --------------------------

    @Override
    protected void addToDftGraph(Node s, Node p, Node o) {
        mutate(() -> writeTxnFor(defaultStore).add(Triple.create(s, p, o)));
    }

    @Override
    protected void addToNamedGraph(Node g, Node s, Node p, Node o) {
        mutate(() -> {
            final MvccTripleStore store = namedStores.computeIfAbsent(
                    g, k -> new MvccTripleStore(indexingStrategy, vc));
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
            final Iterator<Map.Entry<Node, MvccTripleStore>> entries =
                    namedStores.entrySet().iterator();
            return Iter.flatMap(entries, e ->
                    storeFind(e.getValue(), match, t).mapWith(tr -> Quad.create(e.getKey(), tr)));
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
        return Iter.asStream(find(g, s, p, o));
    }

    @Override
    public void close() {
        super.close();
    }
}

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

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.graph.impl.GraphWithPerform;
import org.apache.jena.mem.IndexingStrategy;
import org.apache.jena.mem.store.mvcc.MvccReadView;
import org.apache.jena.mem.store.mvcc.MvccTripleStore;
import org.apache.jena.mem.store.mvcc.MvccVersionControl;
import org.apache.jena.mem.store.mvcc.MvccWriteTxn;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.stream.Stream;

/**
 * Transactional, MVCC variant of {@link org.apache.jena.mem.GraphMemIndexedSet}.
 * The non-transactional counterpart — the same {@link MvccTripleStore} behind the
 * plain {@link org.apache.jena.graph.Graph} surface — is
 * {@link org.apache.jena.mem.GraphMemMvcc}; this class adds the transactional
 * behaviour on top.
 * Unlike the copy-on-write {@link GraphMemIndexedSetCowTxn}, it never copies the
 * store: all transactions share a single {@link MvccTripleStore} and isolation is
 * provided by per-triple version stamps. {@code begin} is therefore O(1) — a read
 * transaction pins the latest committed version; a write transaction acquires the
 * writer slot and buffers changes that are applied at commit.
 * <p>
 * Concurrency mirrors the copy-on-write graph's contract: one writer at a time
 * (serialised by the version control's writer lock), any number of lock-free
 * readers.
 * <p>
 * {@code promote()} on a {@code READ_PROMOTE} / {@code READ_COMMITTED_PROMOTE}
 * transaction may block waiting for a concurrent writer: for {@code READ_COMMITTED}
 * unconditionally; for {@code ISOLATED} it first fails fast if the committed
 * version has moved past the one pinned at {@code begin()}, then blocks on the
 * writer slot and re-checks.
 */
public class GraphMemIndexedSetMvccTxn extends GraphBase
        implements Transactional, GraphWithPerform {

    private final MvccTripleStore store;
    private final ThreadLocal<TxnState> activeTxn = new ThreadLocal<>();

    /**
     * Per-thread transaction state. {@code mode == WRITE} iff this transaction
     * holds the writer slot, so no separate lock-held flag is needed.
     */
    private static final class TxnState {
        TxnType type;
        ReadWrite mode;
        /** READ view pinned at begin (also used for the ISOLATED promote check). */
        MvccReadView readView;
        /** Writer overlay; non-null only under WRITE. */
        MvccWriteTxn writeTxn;
    }

    public GraphMemIndexedSetMvccTxn() {
        this(IndexingStrategy.EAGER);
    }

    public GraphMemIndexedSetMvccTxn(IndexingStrategy indexingStrategy) {
        this.store = new MvccTripleStore(indexingStrategy);
    }

    /**
     * Create a graph on a shared version timeline, for use inside a dataset where
     * all graphs commit on one clock.
     *
     * @param indexingStrategy the indexing strategy
     * @param vc               the shared version control
     */
    public GraphMemIndexedSetMvccTxn(IndexingStrategy indexingStrategy, MvccVersionControl vc) {
        this.store = new MvccTripleStore(indexingStrategy, vc);
    }

    /** Wrap an existing store (used by the dataset). */
    GraphMemIndexedSetMvccTxn(MvccTripleStore store) {
        this.store = store;
    }

    /** @return the underlying shared MVCC store. */
    public MvccTripleStore getStore() {
        return store;
    }

    // ----- Indexing-strategy controls -----------------------------------

    /** @return the {@link IndexingStrategy} this graph was constructed with. */
    public IndexingStrategy getIndexingStrategy() {
        return store.getIndexingStrategy();
    }

    /** @return whether the auxiliary index is currently built. */
    public boolean isIndexInitialized() {
        return store.isIndexInitialized();
    }

    /**
     * Build the MANUAL index over the committed triples. Must be called inside a
     * write transaction (the writer slot is held, so the build is exclusive).
     */
    public void initializeIndex() {
        require();
        if (transactionMode() != ReadWrite.WRITE && !promote()) {
            throw new JenaTransactionException("initializeIndex requires a write transaction");
        }
        store.initializeIndex();
    }

    /**
     * Compact the store, reclaiming slots deleted at or before the oldest active
     * reader's version. Must be called inside a write transaction. Auto-vacuum also
     * runs at commit when there is enough dead weight and no reader is lagging.
     */
    public void vacuum() {
        require();
        if (transactionMode() != ReadWrite.WRITE && !promote()) {
            throw new JenaTransactionException("vacuum requires a write transaction");
        }
        store.vacuum();
    }

    // --- Transactional ----------------------------------------------------

    @Override
    public void begin(TxnType type) {
        if (activeTxn.get() != null) {
            throw new JenaTransactionException("Nested transactions are not supported");
        }
        final TxnState s = new TxnState();
        s.type = type;
        if (type == TxnType.WRITE) {
            store.versionControl().lockWriter();
            try {
                s.writeTxn = store.openWriteTxn();
                s.mode = ReadWrite.WRITE;
                activeTxn.set(s);
            } catch (Throwable th) {
                store.versionControl().unlockWriter();
                throw th;
            }
        } else {
            s.mode = ReadWrite.READ;
            s.readView = store.openReadView();
            activeTxn.set(s);
        }
    }

    @Override
    public boolean promote(Promote mode) {
        final TxnState t = require();
        if (t.mode == ReadWrite.WRITE) {
            return true;
        }
        if (t.type == TxnType.READ) {
            return false;
        }
        final MvccVersionControl vc = store.versionControl();
        if (mode == Promote.READ_COMMITTED) {
            vc.lockWriter();
        } else { // ISOLATED
            if (t.readView.version() != vc.committedVersion()) {
                return false;
            }
            vc.lockWriter();
            if (t.readView.version() != vc.committedVersion()) {
                vc.unlockWriter();
                return false;
            }
        }
        try {
            final MvccWriteTxn w = store.openWriteTxn();
            t.readView.close();
            t.readView = null;
            t.writeTxn = w;
            t.mode = ReadWrite.WRITE;
            return true;
        } catch (Throwable th) {
            vc.unlockWriter();
            throw th;
        }
    }

    @Override
    public void commit() {
        final TxnState t = require();
        try {
            if (t.mode == ReadWrite.WRITE) {
                if (t.writeTxn.hasChanges()) {
                    store.commit(t.writeTxn);
                }
                // Always advance the commit counter so an empty write commit is
                // still observable to ISOLATED promotion (a writer committed).
                store.versionControl().publish(t.writeTxn.version());
            }
        } finally {
            finish(t);
        }
    }

    @Override
    public void abort() {
        final TxnState t = require();
        finish(t); // writer overlay is simply discarded; nothing was applied
    }

    @Override
    public void end() {
        final TxnState t = activeTxn.get();
        if (t == null) {
            return;
        }
        if (t.mode == ReadWrite.WRITE) {
            // Reaching end() in WRITE mode without commit()/abort() is a
            // programming error: discard the overlay (releasing the writer slot)
            // and surface the mistake.
            finish(t);
            throw new JenaTransactionException(
                    "Write transaction was not committed or aborted before end()");
        }
        finish(t);
    }

    private void finish(TxnState t) {
        if (t.mode == ReadWrite.WRITE) {
            store.versionControl().unlockWriter();
        } else if (t.readView != null) {
            t.readView.close();
        }
        activeTxn.remove();
    }

    @Override
    public boolean isInTransaction() {
        return activeTxn.get() != null;
    }

    @Override
    public ReadWrite transactionMode() {
        final TxnState t = activeTxn.get();
        return t == null ? null : t.mode;
    }

    @Override
    public TxnType transactionType() {
        final TxnState t = activeTxn.get();
        return t == null ? null : t.type;
    }

    // --- Graph mutation ---------------------------------------------------

    private MvccWriteTxn writeTxn() {
        final TxnState t = require();
        if (t.mode != ReadWrite.WRITE && !promote()) {
            throw new JenaTransactionException(
                    "Cannot write: read-only transaction or promote failed");
        }
        return t.writeTxn;
    }

    @Override
    public void performAdd(Triple t) {
        writeTxn().add(t);
    }

    @Override
    public void performDelete(Triple t) {
        writeTxn().remove(t);
    }

    @Override
    public void clear() {
        super.clear(); // iterates + performDelete, firing per-triple events
    }

    // --- Graph reads ------------------------------------------------------

    private TxnState require() {
        final TxnState t = activeTxn.get();
        if (t == null) {
            throw new JenaTransactionException("Not in a transaction");
        }
        return t;
    }

    /**
     * The read view active on this thread: the writer overlay under WRITE, the
     * pinned snapshot under READ, or a transient view of the latest committed
     * state outside any transaction (so reads never require an explicit
     * transaction, matching the copy-on-write graph).
     */
    private MvccReadView transientView() {
        return store.transientReadView();
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple m) {
        final TxnState t = activeTxn.get();
        if (t == null) {
            return transientView().find(m);
        }
        return t.mode == ReadWrite.WRITE ? t.writeTxn.find(m) : t.readView.find(m);
    }

    @Override
    protected boolean graphBaseContains(Triple m) {
        final TxnState t = activeTxn.get();
        if (t == null) {
            return transientView().contains(m);
        }
        return t.mode == ReadWrite.WRITE ? t.writeTxn.contains(m) : t.readView.contains(m);
    }

    @Override
    protected int graphBaseSize() {
        final TxnState t = activeTxn.get();
        if (t == null) {
            return transientView().count();
        }
        return t.mode == ReadWrite.WRITE ? t.writeTxn.count() : t.readView.count();
    }

    @Override
    public Stream<Triple> stream() {
        return stream(Node.ANY, Node.ANY, Node.ANY);
    }

    @Override
    public Stream<Triple> stream(Node sm, Node pm, Node om) {
        final Triple match = Triple.createMatch(sm, pm, om);
        final TxnState t = activeTxn.get();
        if (t == null) {
            return transientView().stream(match);
        }
        return t.mode == ReadWrite.WRITE ? t.writeTxn.stream(match) : t.readView.stream(match);
    }

    /**
     * The read view active on the calling thread, for capturing a stable view to
     * use later (possibly from another thread) — e.g. parallel cross-graph reads
     * by a dataset. Outside a transaction this is a transient view of the latest
     * committed state; under READ it is the pinned snapshot.
     *
     * @return the active read view
     */
    public MvccReadView readView() {
        final TxnState t = activeTxn.get();
        if (t == null || t.mode == ReadWrite.WRITE) {
            return store.transientReadView();
        }
        return t.readView;
    }
}

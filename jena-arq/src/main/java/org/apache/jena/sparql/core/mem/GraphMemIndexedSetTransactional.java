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

import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import org.apache.jena.atlas.lib.Copyable;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.graph.impl.GraphWithPerform;
import org.apache.jena.mem2.IndexingStrategy;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.shared.AddDeniedException;
import org.apache.jena.shared.DeleteDeniedException;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.util.iterator.ExtendedIterator;

/**
 * A transactional in-memory {@link org.apache.jena.graph.Graph} that
 * implements MR+SW (multiple readers, single writer) snapshot isolation
 * at the granularity of an entire transaction.
 * <p>
 * Phase A baseline: the working copy used by a write transaction is a
 * full deep copy of the published store, so mutations cannot affect any
 * concurrently-held reader snapshot. {@code commit()} atomically
 * publishes the writer's store (a {@code volatile} reference assignment);
 * older snapshots remain valid for as long as readers hold them, kept
 * alive by ordinary JVM reachability.
 * <p>
 * Phase B will replace the deep copy with a per-structure copy-on-write
 * scheme that pays only for what was modified.
 * <p>
 * Concurrency model:
 * <ul>
 *   <li>Multiple readers may hold transactions simultaneously; each
 *       captures the {@code published} snapshot at {@code begin(READ)}
 *       and serves all reads from it. Readers take no lock.</li>
 *   <li>At most one writer is active at a time, serialised by an
 *       internal {@link ReentrantLock}.</li>
 *   <li>{@code READ_PROMOTE} succeeds only if no other writer has
 *       committed since the transaction began. {@code READ_COMMITTED_PROMOTE}
 *       always succeeds (waiting for the lock) but loses isolation
 *       relative to the original snapshot.</li>
 *   <li>The write lock is released exactly once per acquisition, in
 *       {@code commit()}, {@code abort()} or {@code end()}.</li>
 * </ul>
 */
public final class GraphMemIndexedSetTransactional extends GraphBase
        implements Transactional, GraphWithPerform, Copyable<GraphMemIndexedSetTransactional> {

    private final IndexingStrategy indexingStrategy;
    private final ReentrantLock writeLock = new ReentrantLock();
    private volatile GraphMemIndexedSetTxnSnapshot published;
    private final ThreadLocal<Txn> activeTxn = new ThreadLocal<>();

    public GraphMemIndexedSetTransactional() {
        this(IndexingStrategy.EAGER);
    }

    public GraphMemIndexedSetTransactional(IndexingStrategy indexingStrategy) {
        this.indexingStrategy = indexingStrategy;
        this.published = GraphMemIndexedSetTxnSnapshot.empty(indexingStrategy);
    }

    private GraphMemIndexedSetTransactional(GraphMemIndexedSetTxnSnapshot snapshot) {
        this.indexingStrategy = snapshot.strategy;
        this.published = snapshot;
    }

    public IndexingStrategy getIndexingStrategy() {
        return indexingStrategy;
    }

    // -------- Transactional --------

    @Override
    public void begin(TxnType type) {
        if (type == null)
            throw new JenaTransactionException("begin: null TxnType");
        if (activeTxn.get() != null)
            throw new JenaTransactionException("Transaction already active in this thread");
        switch (type) {
            case READ:
                activeTxn.set(Txn.read(published, type));
                break;
            case WRITE:
                writeLock.lock();
                try {
                    activeTxn.set(Txn.write(GraphMemIndexedSetTxnWorkingCopy.from(published), type));
                } catch (RuntimeException e) {
                    writeLock.unlock();
                    throw e;
                }
                break;
            case READ_PROMOTE:
            case READ_COMMITTED_PROMOTE:
                activeTxn.set(Txn.read(published, type));
                break;
            default:
                throw new JenaTransactionException("Unknown TxnType: " + type);
        }
    }

    @Override
    public boolean promote(Promote mode) {
        Txn t = require();
        if (t.mode == ReadWrite.WRITE)
            return true;
        if (t.type != TxnType.READ_PROMOTE && t.type != TxnType.READ_COMMITTED_PROMOTE)
            throw new JenaTransactionException("promote: transaction was not started with a promote TxnType");
        boolean readCommitted = (mode == Promote.READ_COMMITTED);
        if (readCommitted) {
            writeLock.lock();
        } else {
            if (!writeLock.tryLock())
                return false;
            if (t.snapshot != published) {
                writeLock.unlock();
                return false;
            }
        }
        try {
            t.upgradeToWrite(GraphMemIndexedSetTxnWorkingCopy.from(published));
        } catch (RuntimeException e) {
            writeLock.unlock();
            throw e;
        }
        return true;
    }

    @Override
    public void commit() {
        Txn t = require();
        try {
            if (t.mode == ReadWrite.WRITE && t.dirty)
                published = t.working.seal();
        } finally {
            if (t.mode == ReadWrite.WRITE)
                writeLock.unlock();
            activeTxn.remove();
        }
    }

    @Override
    public void abort() {
        Txn t = require();
        try {
            // drop t.working - GC reclaims it once the txn is gone
        } finally {
            if (t.mode == ReadWrite.WRITE)
                writeLock.unlock();
            activeTxn.remove();
        }
    }

    @Override
    public void end() {
        Txn t = activeTxn.get();
        if (t == null)
            return;
        boolean dirtyWrite = (t.mode == ReadWrite.WRITE && t.dirty);
        try {
            if (t.mode == ReadWrite.WRITE)
                writeLock.unlock();
        } finally {
            activeTxn.remove();
        }
        if (dirtyWrite)
            throw new JenaTransactionException("Write transaction ended without commit or abort");
    }

    @Override
    public boolean isInTransaction() {
        return activeTxn.get() != null;
    }

    @Override
    public ReadWrite transactionMode() {
        Txn t = activeTxn.get();
        return t == null ? null : t.mode;
    }

    @Override
    public TxnType transactionType() {
        Txn t = activeTxn.get();
        return t == null ? null : t.type;
    }

    // -------- Graph read API --------

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple triplePattern) {
        return require().view().find(triplePattern);
    }

    @Override
    public Stream<Triple> stream() {
        return require().view().stream();
    }

    @Override
    public Stream<Triple> stream(Node s, Node p, Node o) {
        return require().view().stream(Triple.createMatch(s, p, o));
    }

    @Override
    protected boolean graphBaseContains(Triple t) {
        return require().view().contains(t);
    }

    @Override
    protected int graphBaseSize() {
        return require().view().size();
    }

    @Override
    public boolean isEmpty() {
        return require().view().isEmpty();
    }

    // -------- Graph write API --------

    @Override
    public void performAdd(Triple t) {
        Txn x = requireWriteOrPromote("add");
        x.working.add(t);
        x.dirty = true;
    }

    @Override
    public void performDelete(Triple t) {
        Txn x = requireWriteOrPromote("delete");
        x.working.remove(t);
        x.dirty = true;
    }

    @Override
    public void clear() {
        // Mirrors GraphMem.clear(): super.clear() iterates and fires
        // per-triple delete events plus the removeAll event; the
        // explicit working-copy clear afterwards guarantees emptiness.
        super.clear();
        Txn x = requireWriteOrPromote("clear");
        x.working.clear();
        x.dirty = true;
    }

    // -------- Copyable --------

    /**
     * Return an independent transactional graph whose initial published
     * snapshot is a deep copy of this graph's current published snapshot.
     * Safe to call outside a transaction.
     */
    @Override
    public GraphMemIndexedSetTransactional copy() {
        return new GraphMemIndexedSetTransactional(GraphMemIndexedSetTxnSnapshot.copyOf(published));
    }

    // -------- helpers --------

    private Txn require() {
        Txn t = activeTxn.get();
        if (t == null)
            throw new JenaTransactionException("Not in a transaction");
        return t;
    }

    private Txn requireWriteOrPromote(String op) {
        Txn t = require();
        if (t.mode == ReadWrite.WRITE)
            return t;
        // READ mode: try to promote if eligible.
        if (t.type == TxnType.READ_PROMOTE || t.type == TxnType.READ_COMMITTED_PROMOTE) {
            Promote pmode = (t.type == TxnType.READ_COMMITTED_PROMOTE)
                    ? Promote.READ_COMMITTED : Promote.ISOLATED;
            if (promote(pmode))
                return activeTxn.get();
            // Promote failed for READ_PROMOTE: surface as the appropriate denied exception
            if ("add".equals(op))
                throw new AddDeniedException("Cannot promote READ_PROMOTE transaction (concurrent commit)");
            else
                throw new DeleteDeniedException("Cannot promote READ_PROMOTE transaction (concurrent commit)");
        }
        // Pure READ: not allowed to write.
        if ("add".equals(op))
            throw new AddDeniedException("Cannot " + op + " in READ transaction");
        else
            throw new DeleteDeniedException("Cannot " + op + " in READ transaction");
    }

    /**
     * Per-thread transaction state. Mutable so {@code promote()} can
     * flip from READ to WRITE in place.
     */
    static final class Txn {
        final TxnType type;
        ReadWrite mode;
        GraphMemIndexedSetTxnSnapshot snapshot;
        GraphMemIndexedSetTxnWorkingCopy working;
        boolean dirty;

        private Txn(TxnType type, ReadWrite mode,
                    GraphMemIndexedSetTxnSnapshot snapshot,
                    GraphMemIndexedSetTxnWorkingCopy working) {
            this.type = type;
            this.mode = mode;
            this.snapshot = snapshot;
            this.working = working;
        }

        static Txn read(GraphMemIndexedSetTxnSnapshot snapshot, TxnType type) {
            return new Txn(type, ReadWrite.READ, snapshot, null);
        }

        static Txn write(GraphMemIndexedSetTxnWorkingCopy working, TxnType type) {
            return new Txn(type, ReadWrite.WRITE, null, working);
        }

        GraphMemIndexedSetTxnReadOps view() {
            return mode == ReadWrite.READ ? snapshot : working;
        }

        void upgradeToWrite(GraphMemIndexedSetTxnWorkingCopy wc) {
            this.working = wc;
            this.snapshot = null;
            this.mode = ReadWrite.WRITE;
        }
    }
}

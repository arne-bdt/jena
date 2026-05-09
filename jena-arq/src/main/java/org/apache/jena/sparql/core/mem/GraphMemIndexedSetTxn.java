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
import org.apache.jena.mem.store.TripleStore;
import org.apache.jena.mem.store.indexed.IndexedSetTripleStore;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

/**
 * Transactional variant of {@link org.apache.jena.mem.GraphMemIndexedSet}.
 * <p>
 * Snapshot isolation is achieved by switching {@link TripleStore}
 * instances at transaction boundaries. {@code begin(WRITE)} deep-copies
 * the currently published {@link IndexedSetTripleStore} via
 * {@link IndexedSetTripleStore#copy()}; mutations happen on the working
 * copy; {@code commit()} publishes the working copy as the new visible
 * store. Readers capture the published reference at {@code begin(READ)}
 * and hold it for the whole transaction, so they always see a stable
 * snapshot regardless of concurrent writers.
 * <p>
 * Concurrency: one writer at a time (serialised by a {@link ReentrantLock});
 * any number of concurrent readers, lock-free.
 */
public class GraphMemIndexedSetTxn extends GraphBase
        implements Transactional, GraphWithPerform {

    /** Serialises writers; readers never take this lock. */
    private final ReentrantLock writeLock = new ReentrantLock();

    /**
     * The currently visible {@link TripleStore}. Read by {@code begin(READ)}
     * (captured as the reader's snapshot) and replaced by {@code commit()} of
     * a dirty write transaction. {@code volatile} establishes happens-before
     * between the commit and any later reader's {@code begin}.
     */
    private volatile TripleStore published;

    /** Per-thread transaction state; {@code null} when no transaction is active. */
    private final ThreadLocal<TxnState> activeTxn = new ThreadLocal<>();

    public GraphMemIndexedSetTxn() {
        this(IndexingStrategy.EAGER);
    }

    public GraphMemIndexedSetTxn(IndexingStrategy indexingStrategy) {
        this.published = new IndexedSetTripleStore(indexingStrategy);
    }

    /**
     * Per-transaction record. The lock-held invariant is
     * {@code mode == WRITE  <==>  this transaction holds writeLock}, so no
     * separate "lock-held" or "finalised" flags are needed.
     */
    private static final class TxnState {
        /** The exact type passed to {@link #begin(TxnType)}; never changes. */
        TxnType type;
        /**
         * READ initially for {@code READ} / {@code READ_PROMOTE} /
         * {@code READ_COMMITTED_PROMOTE}; flips to WRITE on a successful
         * {@link #promote(Promote)} or directly via {@code begin(WRITE)}.
         */
        ReadWrite mode;
        /**
         * The store reads and writes go through. While in READ mode this is
         * the {@code published} reference at the point of {@code begin}
         * (i.e. the snapshot). After promote/begin(WRITE), it is the
         * private working copy.
         */
        TripleStore active;
        /** True once any add/delete has run since begin or successful promote. */
        boolean dirty;
    }

    private TxnState require() {
        TxnState t = activeTxn.get();
        if (t == null)
            throw new JenaTransactionException("Not in a transaction");
        return t;
    }

    /**
     * Resolve the store to read from. Outside any transaction this returns
     * the latest published store, so reads are always possible without first
     * starting a transaction (writes still require one).
     */
    private TripleStore readStore() {
        TxnState t = activeTxn.get();
        return (t == null) ? published : t.active;
    }

    /**
     * Resolve the store to write to. Implicitly promotes a
     * {@code READ_PROMOTE}/{@code READ_COMMITTED_PROMOTE} transaction by
     * delegating to the no-arg {@link Transactional#promote()} default,
     * which dispatches to the right {@link Promote} variant for the type.
     */
    private TripleStore writeStore() {
        TxnState t = require();
        if (t.mode != ReadWrite.WRITE && !promote())
            throw new JenaTransactionException(
                    "Cannot write: read-only transaction or promote failed");
        return t.active;
    }

    // --- Transactional ----------------------------------------------------

    @Override
    public void begin(TxnType type) {
        if (activeTxn.get() != null)
            throw new JenaTransactionException("Nested transactions are not supported");
        TxnState s = new TxnState();
        s.type = type;
        if (type == TxnType.WRITE) {
            // Acquire the writer slot first, then deep-copy the published
            // store as the private working copy. Readers continue to see
            // `published` and are unaffected by mutations on s.active.
            writeLock.lock();
            s.mode = ReadWrite.WRITE;
            s.active = published.copy();
        } else {
            // READ, READ_PROMOTE, READ_COMMITTED_PROMOTE all start as readers
            // sharing the same published snapshot. promote() (if called) will
            // upgrade to a working copy.
            s.mode = ReadWrite.READ;
            s.active = published;
        }
        activeTxn.set(s);
    }

    @Override
    public boolean promote(Promote mode) {
        TxnState t = require();
        if (t.mode == ReadWrite.WRITE)
            return true;                        // already a writer
        if (t.type == TxnType.READ)
            throw new JenaTransactionException("Cannot promote a READ transaction");
        // Remaining types: READ_PROMOTE, READ_COMMITTED_PROMOTE.

        if (mode == Promote.READ_COMMITTED) {
            // READ_COMMITTED: always succeeds, but may need to wait for the
            // current writer. After acquiring, our working copy is taken
            // from the *latest* published — anything previously read in
            // this transaction may be stale, by definition.
            writeLock.lock();
        } else {
            // ISOLATED: snapshot must not have moved since begin(). We try
            // the lock without blocking; even if we acquire it, we still
            // abort if a commit happened between begin and now.
            // t.active is the snapshot reference captured at begin (we are
            // still in READ mode, so it has not been replaced).
            if (!writeLock.tryLock())
                return false;
            if (t.active != published) {
                writeLock.unlock();
                return false;
            }
        }
        t.mode = ReadWrite.WRITE;
        t.active = published.copy();
        return true;
    }

    @Override
    public void commit() {
        TxnState t = require();
        try {
            // Only republish if the writer actually changed something. The
            // single volatile write below is the publication point; all
            // structural changes to t.active happen-before it.
            if (t.mode == ReadWrite.WRITE && t.dirty)
                published = t.active;
        } finally {
            if (t.mode == ReadWrite.WRITE)
                writeLock.unlock();
            activeTxn.remove();
        }
    }

    @Override
    public void abort() {
        TxnState t = require();
        try {
            // Working copy is simply discarded; published is unchanged.
        } finally {
            if (t.mode == ReadWrite.WRITE)
                writeLock.unlock();
            activeTxn.remove();
        }
    }

    @Override
    public void end() {
        TxnState t = activeTxn.get();
        if (t == null)
            return;                             // already finalised by commit/abort
        try {
            // Reaching end() with uncommitted dirty writes is a programming
            // error: callers must explicitly commit or abort. We still
            // unlock in the finally below so we never leak the writer slot.
            if (t.mode == ReadWrite.WRITE && t.dirty)
                throw new JenaTransactionException(
                        "Write transaction was not committed or aborted before end()");
        } finally {
            if (t.mode == ReadWrite.WRITE)
                writeLock.unlock();
            activeTxn.remove();
        }
    }

    @Override
    public boolean isInTransaction() {
        return activeTxn.get() != null;
    }

    @Override
    public ReadWrite transactionMode() {
        TxnState t = activeTxn.get();
        return t == null ? null : t.mode;
    }

    @Override
    public TxnType transactionType() {
        TxnState t = activeTxn.get();
        return t == null ? null : t.type;
    }

    // --- Graph mutation ---------------------------------------------------

    @Override
    public void performAdd(Triple t) {
        writeStore().add(t);
        activeTxn.get().dirty = true;
    }

    @Override
    public void performDelete(Triple t) {
        writeStore().remove(t);
        activeTxn.get().dirty = true;
    }

    @Override
    public void clear() {
        // GraphBase.clear() iterates the graph and removes every triple via
        // performDelete — which routes through writeStore() (implicit
        // promote on the way), marks the transaction dirty, and fires the
        // per-triple Graph events. No further work is required here.
        super.clear();
    }

    // --- Graph reads ------------------------------------------------------

    @Override
    public Stream<Triple> stream() {
        return readStore().stream();
    }

    @Override
    public Stream<Triple> stream(Node sm, Node pm, Node om) {
        return readStore().stream(Triple.createMatch(sm, pm, om));
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple m) {
        return readStore().find(m);
    }

    @Override
    protected boolean graphBaseContains(Triple m) {
        return readStore().contains(m);
    }

    @Override
    protected int graphBaseSize() {
        return readStore().countTriples();
    }
}

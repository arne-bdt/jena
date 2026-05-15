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
import org.apache.jena.mem.store.cow.CowSnapshot;
import org.apache.jena.mem.store.cow.CowStore;
import org.apache.jena.mem.store.cow.CowWriteTxn;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

/**
 * Transactional, copy-on-write variant of
 * {@link org.apache.jena.mem.GraphMemIndexedSet}. Routes the
 * begin-write / commit / publish dance through
 * {@link CowSnapshot#forkForWrite()} (returning a mutable
 * {@link CowWriteTxn}) and {@link CowWriteTxn#freeze()} (returning a
 * fresh {@link CowSnapshot} to publish).
 * <p>
 * The read-only / mutable API split lives in the underlying types:
 * {@link CowSnapshot} exposes only read methods; {@link CowWriteTxn}
 * exposes {@code add}/{@code remove}/{@code clear} plus the
 * strategy-control setters. {@link CowSnapshot#forkForWrite()} is the
 * only operation that crosses between the two.
 * <p>
 * Concurrency: one writer at a time (serialised by a
 * {@link ReentrantLock}); any number of concurrent readers, lock-free.
 * <p>
 * {@code promote()} on a {@code READ_PROMOTE} or
 * {@code READ_COMMITTED_PROMOTE} transaction may <em>block</em> waiting for
 * any concurrent writer to commit or abort. For {@code READ_COMMITTED} this
 * is unconditional; for {@code ISOLATED} the call first fails fast if the
 * published snapshot has already moved past the one captured at
 * {@code begin()}, then blocks on the writer lock (a concurrent writer may
 * yet abort, in which case promotion succeeds), then re-checks once the
 * lock is held. Callers that need a non-blocking attempt should detect the
 * concurrent writer themselves rather than rely on {@code promote} to
 * fail-fast.
 */
public class GraphMemIndexedSetCowTxn extends GraphBase
        implements Transactional, GraphWithPerform {

    /** Serialises writers; readers never take this lock. */
    private final ReentrantLock writeLock = new ReentrantLock();

    /**
     * The currently visible snapshot. Read by {@code begin(READ)} (captured
     * as the reader's view) and replaced by {@code commit()} of a dirty
     * write transaction. {@code volatile} establishes happens-before
     * between the commit and any later reader's {@code begin}.
     */
    private volatile CowSnapshot published;

    /** Per-thread transaction state; {@code null} when no transaction is active. */
    private final ThreadLocal<TxnState> activeTxn = new ThreadLocal<>();

    /**
     * Selects between sequential and parallel implementations of
     * {@link CowSnapshot#forkForWrite()} when starting a write transaction.
     * Exposed primarily so benchmarks can compare the two fork strategies
     * on the same workload.
     */
    public enum ForkMode {
        /** Sequential: {@link CowSnapshot#forkForWrite()}. */
        SEQUENTIAL,
        /** Parallel: {@link CowSnapshot#forkForWriteParallel()}. */
        PARALLEL
    }

    private final ForkMode forkMode;

    public GraphMemIndexedSetCowTxn() {
        this(IndexingStrategy.EAGER, ForkMode.SEQUENTIAL);
    }

    public GraphMemIndexedSetCowTxn(IndexingStrategy indexingStrategy) {
        this(indexingStrategy, ForkMode.SEQUENTIAL);
    }

    public GraphMemIndexedSetCowTxn(ForkMode forkMode) {
        this(IndexingStrategy.EAGER, forkMode);
    }

    public GraphMemIndexedSetCowTxn(IndexingStrategy indexingStrategy, ForkMode forkMode) {
        this.published = new CowSnapshot(indexingStrategy);
        this.forkMode = forkMode;
    }

    private CowWriteTxn fork() {
        return switch (forkMode) {
            case SEQUENTIAL -> published.forkForWrite();
            case PARALLEL   -> published.forkForWriteParallel();
        };
    }

    // ----- Indexing-strategy controls -----------------------------------

    /**
     * @return the {@link IndexingStrategy} this graph was constructed with.
     * Note: the actual strategy in use at any given moment may have been
     * upgraded (e.g. LAZY → EAGER on first lookup, or MANUAL after a
     * call to {@link #initializeIndex()}); see {@link #isIndexInitialized()}.
     */
    public IndexingStrategy getIndexingStrategy() {
        return readStore().getIndexingStrategy();
    }

    /**
     * @return whether the index is currently built and ready to serve
     * pattern lookups directly from the active store (the writer's
     * working copy if inside a write transaction; otherwise the published
     * snapshot).
     */
    public boolean isIndexInitialized() {
        return readStore().isIndexInitialized();
    }

    /**
     * Drop the working copy's current strategy and re-install a fresh
     * one of the configured {@link IndexingStrategy} kind. For
     * {@code LAZY}/{@code LAZY_PARALLEL} this means the next pattern
     * lookup will trigger a fresh auto-build; for {@code MANUAL} it
     * means future pattern lookups will again throw until
     * {@link #initializeIndex()} is called; for {@code EAGER} it
     * rebuilds an empty eager index that re-fills as triples flow in.
     * <p>
     * Must be called from inside a write transaction.
     */
    public void resetIndexStrategy() {
        writeStore().resetIndexStrategy();
        activeTxn.get().dirty = true;
    }

    /**
     * Build the eager index sequentially in the working copy and install
     * it as the current strategy. After this call,
     * {@link #isIndexInitialized()} is {@code true}. Must be called from
     * inside a write transaction.
     */
    public void initializeIndex() {
        writeStore().initializeIndex();
        activeTxn.get().dirty = true;
    }

    /**
     * Like {@link #initializeIndex()} but builds in parallel.
     */
    public void initializeIndexParallel() {
        writeStore().initializeIndexParallel();
        activeTxn.get().dirty = true;
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
         * Read view: a {@link CowSnapshot} (captured at begin, or the
         * latest published) for READ-only paths; a {@link CowWriteTxn}
         * after promote/begin(WRITE). Both share the {@link CowStore}
         * read API; only WRITE-mode access uses the {@link CowWriteTxn}
         * mutation API.
         */
        CowStore active;
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
     * the latest published snapshot, so reads are always possible without
     * first starting a transaction (writes still require one).
     */
    private CowStore readStore() {
        TxnState t = activeTxn.get();
        return (t == null) ? published : t.active;
    }

    /**
     * Return the current read view active on this thread — the {@link CowStore}
     * the next read operation would route through. Outside any transaction
     * this is the latest published snapshot; inside a transaction it is the
     * snapshot captured at {@link #begin(TxnType) begin(READ)} or the
     * writer's working copy under WRITE.
     * <p>
     * Useful for callers that want to capture a stable view on one thread
     * and use it later from a different thread — for example, a
     * {@link org.apache.jena.sparql.core.DatasetGraph DatasetGraph} doing
     * parallel cross-graph reads on a {@link java.util.concurrent.ForkJoinPool}
     * cannot consult the per-thread state on each worker, so it captures
     * {@code readView()} on the caller's thread and dispatches
     * {@code view.find(match)} / {@code view.stream(match)} directly on the
     * captured reference. The view's read operations are thread-safe.
     *
     * @return the {@link CowStore} read view active for the calling thread.
     */
    public CowStore readView() {
        return readStore();
    }

    /**
     * Resolve the writer's working copy. Implicitly promotes a
     * {@code READ_PROMOTE}/{@code READ_COMMITTED_PROMOTE} transaction by
     * delegating to the no-arg {@link Transactional#promote()} default,
     * which dispatches to the right {@link Promote} variant for the type.
     */
    private CowWriteTxn writeStore() {
        TxnState t = require();
        if (t.mode != ReadWrite.WRITE && !promote())
            throw new JenaTransactionException(
                    "Cannot write: read-only transaction or promote failed");
        return (CowWriteTxn) t.active;
    }

    // --- Transactional ----------------------------------------------------

    @Override
    public void begin(TxnType type) {
        if (activeTxn.get() != null)
            throw new JenaTransactionException("Nested transactions are not supported");
        TxnState s = new TxnState();
        s.type = type;
        if (type == TxnType.WRITE) {
            // Acquire the writer slot first, then fork the published store
            // for write. After the fork, `published` is implicitly frozen
            // (this txn promises not to mutate it). Readers that captured
            // `published` earlier see no effect from mutations on s.active.
            writeLock.lock();
            try {
                s.active = fork();
                s.mode = ReadWrite.WRITE;
                activeTxn.set(s);
            } catch (Throwable th) {
                writeLock.unlock();
                throw th;
            }
        } else {
            // READ, READ_PROMOTE, READ_COMMITTED_PROMOTE all start as readers
            // sharing the same published snapshot. promote() (if called) will
            // upgrade to a working copy.
            s.mode = ReadWrite.READ;
            s.active = published;
            activeTxn.set(s);
        }
    }

    @Override
    public boolean promote(Promote mode) {
        TxnState t = require();
        if (t.mode == ReadWrite.WRITE)
            return true;                        // already a writer
        if (t.type == TxnType.READ)
            return false;                       // plain READ cannot promote
        // Remaining types: READ_PROMOTE, READ_COMMITTED_PROMOTE.

        if (mode == Promote.READ_COMMITTED) {
            // READ_COMMITTED: always succeeds, but may need to wait for the
            // current writer. After acquiring, our working copy is taken
            // from the *latest* published — anything previously read in
            // this transaction may be stale, by definition.
            writeLock.lock();
        } else {
            // ISOLATED: the snapshot we captured at begin must still be the
            // published one when we finish promoting. Two checks are needed:
            //  1. Fail fast if it has already moved (no point blocking).
            //  2. Block on the writer slot — a concurrent writer may yet
            //     abort, in which case published is unchanged and we can
            //     promote successfully.
            //  3. After acquiring, re-check: if the writer we waited for
            //     committed a *real* change, published has moved and we
            //     must abort. A no-op commit leaves published equal to
            //     t.active and is therefore harmless.
            if (t.active != published)
                return false;
            writeLock.lock();
            if (t.active != published) {
                writeLock.unlock();
                return false;
            }
        }
        try {
            // Take the fork before flipping mode so a failed fork() leaves the
            // txn observably READ and lets the catch path unlock cleanly.
            CowWriteTxn forked = fork();
            t.active = forked;          // honours the configured ForkMode
            t.mode = ReadWrite.WRITE;
            return true;
        } catch (Throwable th) {
            writeLock.unlock();
            throw th;
        }
    }

    @Override
    public void commit() {
        TxnState t = require();
        try {
            // Republish if the writer changed something visible. "Visible"
            // covers data mutations (t.dirty) AND strategy mutations like
            // a LAZY auto-build triggered by a partial-pattern lookup
            // inside this write transaction — without that second clause,
            // the writer's auto-build work would be silently discarded at
            // commit, forcing every future writer to re-build the index.
            // The single volatile write below is the publication point;
            // all structural changes to t.active happen-before it.
            if (t.mode == ReadWrite.WRITE) {
                CowWriteTxn w = (CowWriteTxn) t.active;
                if (t.dirty || w.wasStrategyChanged())
                    published = w.freeze();
            }
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

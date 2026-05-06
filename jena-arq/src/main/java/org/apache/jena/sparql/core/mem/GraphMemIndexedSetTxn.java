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
 * Phase A implementation: snapshot isolation is achieved by switching
 * {@link TripleStore} instances at transaction boundaries. A {@code begin(WRITE)}
 * deep-copies the currently published {@link IndexedSetTripleStore} via
 * {@link IndexedSetTripleStore#copy()}; mutations happen on the working copy;
 * {@code commit()} publishes the working copy as the new visible store. Readers
 * capture the published reference at {@code begin(READ)} and hold it for the
 * whole transaction, so they always see a stable snapshot regardless of
 * concurrent writers.
 * <p>
 * Concurrency: one writer at a time (serialised by a {@link ReentrantLock});
 * any number of concurrent readers, lock-free.
 */
public class GraphMemIndexedSetTxn extends GraphBase
        implements Transactional, GraphWithPerform {

    private final ReentrantLock writeLock = new ReentrantLock();
    private volatile TripleStore published;
    private final ThreadLocal<TxnState> activeTxn = new ThreadLocal<>();

    public GraphMemIndexedSetTxn() {
        this(IndexingStrategy.EAGER);
    }

    public GraphMemIndexedSetTxn(IndexingStrategy indexingStrategy) {
        this.published = new IndexedSetTripleStore(indexingStrategy);
    }

    private static final class TxnState {
        TxnType type;
        ReadWrite mode;
        TripleStore active;
        TripleStore beginSnapshot;
        boolean dirty;
        boolean writeLocked;
        boolean finalised;
    }

    private TxnState require() {
        TxnState t = activeTxn.get();
        if (t == null)
            throw new JenaTransactionException("Not in a transaction");
        return t;
    }

    /**
     * Resolve the {@link TripleStore} to read from. Inside a transaction,
     * returns the snapshot or the working copy depending on the mode. Outside
     * any transaction, returns the latest published store.
     */
    private TripleStore readStore() {
        TxnState t = activeTxn.get();
        return (t == null) ? published : t.active;
    }

    /**
     * Resolve the {@link TripleStore} to write to. Throws if not inside a
     * write transaction (after possibly performing an implicit promote for a
     * promote-typed read transaction).
     */
    private TripleStore writeStore() {
        TxnState t = require();
        if (t.mode == ReadWrite.WRITE)
            return t.active;
        if (t.type == TxnType.READ_PROMOTE || t.type == TxnType.READ_COMMITTED_PROMOTE) {
            boolean ok = promote(t.type == TxnType.READ_COMMITTED_PROMOTE
                    ? Promote.READ_COMMITTED : Promote.ISOLATED);
            if (!ok)
                throw new JenaTransactionException(
                        "Cannot promote: another writer committed since begin()");
            return require().active;
        }
        throw new JenaTransactionException("Read-only transaction; writes are not allowed");
    }

    // --- Transactional ----------------------------------------------------

    @Override
    public void begin(TxnType type) {
        if (activeTxn.get() != null)
            throw new JenaTransactionException("Nested transactions are not supported");
        TxnState s = new TxnState();
        s.type = type;
        switch (type) {
            case READ -> {
                s.mode = ReadWrite.READ;
                s.beginSnapshot = published;
                s.active = s.beginSnapshot;
            }
            case WRITE -> {
                writeLock.lock();
                s.writeLocked = true;
                s.mode = ReadWrite.WRITE;
                s.beginSnapshot = published;
                s.active = s.beginSnapshot.copy();
            }
            case READ_PROMOTE, READ_COMMITTED_PROMOTE -> {
                s.mode = ReadWrite.READ;
                s.beginSnapshot = published;
                s.active = s.beginSnapshot;
            }
        }
        activeTxn.set(s);
    }

    @Override
    public boolean promote(Promote mode) {
        TxnState t = require();
        if (t.mode == ReadWrite.WRITE)
            return true;
        if (t.type == TxnType.READ)
            throw new JenaTransactionException("Cannot promote a READ transaction");
        if (t.type != TxnType.READ_PROMOTE && t.type != TxnType.READ_COMMITTED_PROMOTE)
            throw new JenaTransactionException("Cannot promote transaction of type " + t.type);

        boolean readCommitted = (mode == Promote.READ_COMMITTED);
        if (readCommitted) {
            writeLock.lock();
        } else {
            if (!writeLock.tryLock())
                return false;
            if (t.beginSnapshot != published) {
                writeLock.unlock();
                return false;
            }
        }
        t.writeLocked = true;
        t.mode = ReadWrite.WRITE;
        t.beginSnapshot = published;
        t.active = t.beginSnapshot.copy();
        return true;
    }

    @Override
    public void commit() {
        TxnState t = require();
        try {
            if (t.mode == ReadWrite.WRITE && t.dirty)
                published = t.active;
            t.finalised = true;
        } finally {
            if (t.writeLocked) {
                t.writeLocked = false;
                writeLock.unlock();
            }
            activeTxn.remove();
        }
    }

    @Override
    public void abort() {
        TxnState t = require();
        try {
            t.finalised = true;
            // working copy simply dropped
        } finally {
            if (t.writeLocked) {
                t.writeLocked = false;
                writeLock.unlock();
            }
            activeTxn.remove();
        }
    }

    @Override
    public void end() {
        TxnState t = activeTxn.get();
        if (t == null)
            return;
        try {
            if (t.mode == ReadWrite.WRITE && !t.finalised && t.dirty) {
                throw new JenaTransactionException(
                        "Write transaction was not committed or aborted before end()");
            }
        } finally {
            if (t.writeLocked) {
                t.writeLocked = false;
                writeLock.unlock();
            }
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
        TripleStore s = writeStore();
        s.add(t);
        TxnState st = activeTxn.get();
        if (st != null)
            st.dirty = true;
    }

    @Override
    public void performDelete(Triple t) {
        TripleStore s = writeStore();
        s.remove(t);
        TxnState st = activeTxn.get();
        if (st != null)
            st.dirty = true;
    }

    @Override
    public void clear() {
        // GraphBase.clear() iterates and calls performDelete (firing events).
        super.clear();
        // Then explicitly clear the working copy in case any state remains.
        TripleStore s = writeStore();
        s.clear();
        TxnState st = activeTxn.get();
        if (st != null)
            st.dirty = true;
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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.sparql.core.mem;

import org.apache.jena.atlas.lib.Copyable;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.graph.impl.GraphWithPerform;
import org.apache.jena.mem.GraphMem;
import org.apache.jena.mem.txn.GraphMemTxn;
import org.apache.jena.mem.txn.store.FastTripleStorePersistable;
import org.apache.jena.mem.txn.store.TripleStorePersistable;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import static org.apache.jena.query.TxnType.READ_COMMITTED_PROMOTE;
import static org.apache.jena.query.TxnType.READ_PROMOTE;

public class GraphMemTransactional extends GraphBase implements Transactional, GraphWithPerform, Copyable<GraphMemTransactional> {

    protected volatile FastTripleStorePersistable tripleStore;
    private volatile TripleStorePersistable immutableTripleStore;

    private final ReentrantLock writeLock = new ReentrantLock();

    private record TransactionInfo(TxnType type, ReadWrite mode, TripleStorePersistable tripleStore, AtomicBoolean hasUncommittedChanges) {}
    private final ThreadLocal<TransactionInfo> threadLocalTxnInfo = new ThreadLocal<>();

    public GraphMemTransactional() {
        super();
        this.tripleStore = new FastTripleStorePersistable();
        this.immutableTripleStore = tripleStore.createImmutableChild();
    }

    protected GraphMemTransactional(FastTripleStorePersistable tripleStore) {
        super();
        this.tripleStore = tripleStore;
        this.immutableTripleStore = tripleStore.createImmutableChild();
    }

    /**
     * Creates a copy of this graph.
     * Since the triples and nodes are immutable, the copy contains the same triples and nodes as this graph.
     * Modifications to the copy will not affect this graph.
     *
     * @return independent copy of the current graph
     */
    @SuppressWarnings("unchecked")
    @Override
    public GraphMemTransactional copy() {
        return new GraphMemTransactional(this.tripleStore.copy());
    }

    @Override
    public void begin(TxnType type) {
        if(isInTransaction())
            throw new JenaTransactionException("Already in a transaction.");

        switch (type) {
            case READ -> {
                this.threadLocalTxnInfo.set(new TransactionInfo(type, ReadWrite.READ, this.immutableTripleStore, new AtomicBoolean(false)));
            }
            case WRITE -> {
                this.writeLock.lock();
                this.threadLocalTxnInfo.set(new TransactionInfo(type, ReadWrite.WRITE, this.tripleStore, new AtomicBoolean(false)));
            }
            case READ_PROMOTE, READ_COMMITTED_PROMOTE -> {
                throw new JenaTransactionException("Promote transactions are not supported.");
            }
        }
    }

    @Override
    public boolean promote(Promote mode) {
        throw new JenaTransactionException("Promote transactions are not supported.");
    }

    @Override
    public void commit() {
        final var txnInfo = threadLocalTxnInfo.get();
        if(txnInfo == null) {
            throw new JenaTransactionException("Not in a transaction.");
        }
        if(txnInfo.hasUncommittedChanges.get()) {
            // Create a new immutable triple store for future read transactions
            this.immutableTripleStore = txnInfo.tripleStore.createImmutableChild();
            txnInfo.hasUncommittedChanges.set(false);
        }
    }

    @Override
    public void abort() {
        final var txnInfo = threadLocalTxnInfo.get();
        if(txnInfo == null) {
            throw new JenaTransactionException("Not in a transaction.");
        }
        if(txnInfo.hasUncommittedChanges.get()) {
            // Rollback to the immutable triple store
            this.tripleStore = new FastTripleStorePersistable(this.tripleStore, false);
        }
        if(txnInfo.mode == ReadWrite.WRITE) {
            // For write transactions, we need to release the write lock
            this.writeLock.unlock();
        }
        this.threadLocalTxnInfo.remove();
    }

    @Override
    public void end() {
        abort();
    }

    @Override
    public ReadWrite transactionMode() {
        final var txnInfo = this.threadLocalTxnInfo.get();
        if (txnInfo == null) {
            return null;
        }
        return txnInfo.mode;
    }

    @Override
    public TxnType transactionType() {
        final var txnInfo = this.threadLocalTxnInfo.get();
        if (txnInfo == null) {
            return null;
        }
        return txnInfo.type;
    }

    @Override
    public boolean isInTransaction() {
        return this.threadLocalTxnInfo.get() != null;
    }

    public TripleStorePersistable getTripleStoreForModification() {
        final var txnInfo = this.threadLocalTxnInfo.get();
        if(txnInfo == null)
            throw new JenaTransactionException("Not in a transaction.");
        if(txnInfo.mode != ReadWrite.WRITE)
            throw new JenaTransactionException("Cannot modify graph in a read transaction.");
        txnInfo.hasUncommittedChanges.set(true);
        return txnInfo.tripleStore;
    }

    public TripleStorePersistable getTripleStoreForReading() {
        final var txnInfo = this.threadLocalTxnInfo.get();
        if(txnInfo == null)
            throw new JenaTransactionException("Not in a transaction.");
        return txnInfo.tripleStore;
    }

    @Override
    public void clear() {
        final var store = getTripleStoreForModification();
        super.clear(); /* deletes all triples and sends notifications*/
        store.clear();
    }

    /**
     * Add a triple to the graph without notifying. The default implementation throws an
     * AddDeniedException; subclasses must override if they want to be able to
     * add triples.
     *
     * @param t triple to add
     */
    @Override
    public void performAdd(final Triple t) {
        getTripleStoreForModification().add(t);
    }

    /**
     * Remove a triple from the triple store. The default implementation throws
     * a DeleteDeniedException; subclasses must override if they want to be able
     * to remove triples.
     *
     * @param t triple to delete
     */
    @Override
    public void performDelete(Triple t) {
        getTripleStoreForModification().remove(t);
    }

    /**
     * Returns a {@link Stream} of all triples in the graph.
     * Note: {@link Stream#parallel()} is supported.
     *
     * @return a stream  of triples in this graph.
     */
    @Override
    public Stream<Triple> stream() {
        return getTripleStoreForReading().stream();
    }

    /**
     * Returns a {@link Stream} of Triples matching a pattern.
     * Note: {@link Stream#parallel()} is supported.
     *
     * @param sm subject node match pattern
     * @param pm predicate node match pattern
     * @param om object node match pattern
     * @return a stream  of triples in this graph matching the pattern.
     */
    @Override
    public Stream<Triple> stream(final Node sm, final Node pm, final Node om) {
        return getTripleStoreForReading().stream(Triple.createMatch(sm, pm, om));
    }

    /**
     * Returns an {@link ExtendedIterator} of all triples in the graph matching the given triple match.
     */
    @Override
    public ExtendedIterator<Triple> graphBaseFind(Triple tripleMatch) {
        return getTripleStoreForReading().find(tripleMatch);
    }

    /**
     * Answer true if the graph contains any triple matching <code>t</code>.
     * The default implementation uses <code>find</code> and checks to see
     * if the iterator is non-empty.
     *
     * @param tripleMatch triple match pattern, which may be contained
     */
    @Override
    public boolean graphBaseContains(final Triple tripleMatch) {
        return getTripleStoreForReading().contains(tripleMatch);
    }

    /**
     * Answer the number of triples in this graph. Default implementation counts its
     * way through the results of a findAll. Subclasses must override if they want
     * size() to be efficient.
     */
    @Override
    public int graphBaseSize() {
        return getTripleStoreForReading().countTriples();
    }


}

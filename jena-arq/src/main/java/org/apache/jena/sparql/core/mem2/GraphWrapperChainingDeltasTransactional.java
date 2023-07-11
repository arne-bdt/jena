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

package org.apache.jena.sparql.core.mem2;

import org.apache.jena.graph.*;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.shared.AddDeniedException;
import org.apache.jena.shared.DeleteDeniedException;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

class GraphWrapperChainingDeltasTransactional implements Graph, Transactional {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphWrapperChainingDeltasTransactional.class);

    /**
     * This lock is used to ensure that only one thread can write to the graph at a time.
     * A Semaphore is used instead of a ReentrantLock because it allows the lock to be released
     * in a different thread than the one that acquired it.
     */
    private final Semaphore writeSemaphore = new Semaphore(1);
    private final AtomicLong dataVersion = new AtomicLong(0);

    private final AtomicInteger lengthOfDeltaChain = new AtomicInteger(0);

    private final AtomicInteger openReadTransactions = new AtomicInteger(0);
    private final Supplier<Graph> graphFactory;
    private final ThreadLocal<Boolean> txnInTransaction = ThreadLocal.withInitial(() -> Boolean.FALSE);
    private final ThreadLocal<TxnType> txnType = new ThreadLocal();
    private final ThreadLocal<ReadWrite> txnMode = new ThreadLocal();
    private final ThreadLocal<Graph> txnGraph = new ThreadLocal<>();
    private final ThreadLocal<Long> txnReadTransactionVersion = new ThreadLocal<>();
    private final TransactionCoordinator transactionCoordinator;
    private final Consumer<FastDeltaGraph> committedDeltasConsumer;
    private volatile Graph graphBeforeCurrentWriteTransaction;
    private volatile Graph wrappedGraph;
    private volatile GraphReadOnlyWrapper lastCommittedGraph;

    public GraphWrapperChainingDeltasTransactional(final Supplier<Graph> graphFactory,
                                                   final TransactionCoordinator transactionCoordinator,
                                                   final Consumer<FastDeltaGraph> committedDeltasConsumer) {
        this.graphFactory = graphFactory;
        this.wrappedGraph = graphFactory.get();
        this.committedDeltasConsumer = committedDeltasConsumer;
        this.lastCommittedGraph = new GraphReadOnlyWrapper(wrappedGraph);
        this.transactionCoordinator = transactionCoordinator;
    }

    public GraphWrapperChainingDeltasTransactional(final Graph graphToWrap, final Supplier<Graph> graphFactory,
                                                   final TransactionCoordinator transactionCoordinator,
                                                   final Consumer<FastDeltaGraph> committedDeltasConsumer) {
        this.graphFactory = graphFactory;
        this.wrappedGraph = graphToWrap;
        this.committedDeltasConsumer = committedDeltasConsumer;
        this.lastCommittedGraph = new GraphReadOnlyWrapper(wrappedGraph);
        this.transactionCoordinator = transactionCoordinator;
    }

    private Graph getGraphForCurrentTransaction() {
        final var txnGraph = this.txnGraph.get();
        if (txnGraph == null) {
            throw new JenaTransactionException("Not in a transaction.");
        }
        transactionCoordinator.refreshTimeoutForCurrentThread();
        return txnGraph;
    }

    private static Graph mergeDeltas(Graph graph) {
        if (graph instanceof FastDeltaGraph delta) {
            var base = mergeDeltas(delta);
            delta.getDeletions().forEachRemaining(base::delete);
            delta.getAdditions().forEachRemaining(base::add);
            return base;
        } else {
            return graph;
        }
    }

    public boolean hasOpenReadTransactions() {
        return openReadTransactions.get() > 0;
    }

    public boolean hasOpenWriteTransaction() {
        return writeSemaphore.availablePermits() == 0;
    }

    public int getLengthOfDeltaChain() {
        return lengthOfDeltaChain.get();
    }

    public void mergeDeltasAndExecuteDirectlyOnWrappedGraph(Consumer<Graph> wrappedGraphConsumer) {
        if (isInTransaction()) {
            throw new JenaTransactionException("Cannot access wrapped graph while in a transaction.");
        }
        if (hasOpenReadTransactions()) {
            throw new JenaTransactionException("Cannot access wrapped graph while there are open read transactions.");
        }
        try {
            final var timeoutMs = transactionCoordinator.getTransactionTimeoutMs()
                    + transactionCoordinator.getStaleTransactionRemovalTimerIntervalMs();
            if (!writeSemaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)) {
                LOGGER.error("Failed to acquire write semaphore within " + timeoutMs + " ms.");
                throw new JenaTransactionException("Failed to acquire write semaphore within " + timeoutMs + " ms.");
            }
            wrappedGraph = mergeDeltas(wrappedGraph);
            lengthOfDeltaChain.set(0);
            try {
                wrappedGraphConsumer.accept(wrappedGraph);
            } finally {
                writeSemaphore.release();
            }
        } catch (InterruptedException e) {
            LOGGER.error("Failed to acquire write semaphore.", e);
            new JenaTransactionException("Failed to acquire write semaphore.", e);
        }
    }

    @Override
    public void begin(TxnType txnType) {
        if (isInTransaction())
            new JenaTransactionException("Already in a transaction.");
        ReadWrite readWrite = TxnType.convert(txnType);
        if (readWrite == ReadWrite.WRITE) {
            try {
                final var timeoutMs = transactionCoordinator.getTransactionTimeoutMs()
                        + transactionCoordinator.getStaleTransactionRemovalTimerIntervalMs();
                if (!writeSemaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)) {
                    LOGGER.error("Failed to acquire write semaphore within " + timeoutMs + " ms.");
                    throw new JenaTransactionException("Failed to acquire write semaphore within " + timeoutMs + " ms.");
                }
            } catch (InterruptedException e) {
                LOGGER.error("Failed to acquire write semaphore.", e);
                new JenaTransactionException("Failed to acquire write semaphore.", e);
            }
        }
        this.txnInTransaction.set(true);
        this.txnMode.set(readWrite);
        this.txnType.set(txnType);
        switch (readWrite) {
            case READ:
                txnGraph.set(lastCommittedGraph);
                txnReadTransactionVersion.set(dataVersion.get());
                openReadTransactions.getAndIncrement();
                transactionCoordinator.registerCurrentThread(() -> {
                    openReadTransactions.getAndDecrement();
                });
                break;
            case WRITE:
                graphBeforeCurrentWriteTransaction = wrappedGraph;
                wrappedGraph = new FastDeltaGraph(wrappedGraph);
                txnGraph.set(wrappedGraph);
                transactionCoordinator.registerCurrentThread(() -> {
                    writeSemaphore.release();
                    wrappedGraph = graphBeforeCurrentWriteTransaction;
                });
                break;
        }
    }

    @Override
    public ReadWrite transactionMode() {
        return txnMode.get();
    }

    @Override
    public TxnType transactionType() {
        return txnType.get();
    }

    @Override
    public boolean isInTransaction() {
        return txnInTransaction.get();
    }

    @Override
    public boolean promote(Promote txnType) {
        if (!writeSemaphore.tryAcquire()) {
            return false;
        }
        // if we are promoting to isolated, we need to check that the data hasn't changed
        if (txnType == Promote.ISOLATED
                && txnReadTransactionVersion.get() != dataVersion.get()) {
            writeSemaphore.release();
            return false;
        }
        openReadTransactions.decrementAndGet();
        transactionCoordinator.unregisterCurrentThread();
        txnMode.set(ReadWrite.WRITE);
        graphBeforeCurrentWriteTransaction = wrappedGraph;
        wrappedGraph = new FastDeltaGraph(wrappedGraph);
        txnGraph.set(wrappedGraph);
        transactionCoordinator.registerCurrentThread(() -> {
            writeSemaphore.release();
            wrappedGraph = graphBeforeCurrentWriteTransaction;
        });
        return true;
    }

    @Override
    public void commit() {
        if (isInTransaction()) {
            transactionCoordinator.unregisterCurrentThread();
            switch (transactionMode()) {
                case READ:
                    openReadTransactions.getAndDecrement();
                    break;
                case WRITE:
                    final var delta = (FastDeltaGraph) wrappedGraph;
                    if (delta.hasChanges()) {
                        lastCommittedGraph = new GraphReadOnlyWrapper(wrappedGraph);
                        dataVersion.getAndIncrement();
                        lengthOfDeltaChain.getAndIncrement();
                        committedDeltasConsumer.accept(delta);
                    } else {
                        wrappedGraph = delta.getBase();
                    }
                    break;
                default:
                    endOnceByRemovingThreadLocalsAndUnlocking();
                    throw new JenaTransactionException("Unknown transaction mode: " + transactionMode());
            }
        }
        endOnceByRemovingThreadLocalsAndUnlocking();
    }

    @Override
    public void abort() {
        if (isInTransaction()) {
            transactionCoordinator.unregisterCurrentThread();
            switch (transactionMode()) {
                case READ:
                    openReadTransactions.getAndDecrement();
                    break;
                case WRITE:
                    wrappedGraph = graphBeforeCurrentWriteTransaction;
                    break;
                default:
                    endOnceByRemovingThreadLocalsAndUnlocking();
                    throw new JenaTransactionException("Unknown transaction mode: " + transactionMode());
            }
        }
        endOnceByRemovingThreadLocalsAndUnlocking();
    }

    @Override
    public void end() {
        if (isTransactionMode(ReadWrite.WRITE)) {
            abort();
            new JenaTransactionException("Write transaction - no commit or abort before end()");
        } else {
            abort();
        }
    }

    private boolean isTransactionMode(ReadWrite mode) {
        if (!isInTransaction())
            return false;
        return transactionMode() == mode;
    }

    private void endOnceByRemovingThreadLocalsAndUnlocking() {
        if (isInTransaction()) {
            if (transactionMode() == ReadWrite.WRITE) {
                writeSemaphore.release();
            }
            txnGraph.remove();
            txnMode.remove();
            txnType.remove();
            txnInTransaction.remove();
            txnReadTransactionVersion.remove();
        }
    }

    @Override
    public boolean dependsOn(Graph other) {
        return getGraphForCurrentTransaction().dependsOn(other);
    }

    @Override
    public TransactionHandler getTransactionHandler() {
        return null; // TODO
    }

    @Override
    public Capabilities getCapabilities() {
        return getGraphForCurrentTransaction().getCapabilities();
    }

    @Override
    public GraphEventManager getEventManager() {
        return getGraphForCurrentTransaction().getEventManager();
    }

    @Override
    public PrefixMapping getPrefixMapping() {
        return getGraphForCurrentTransaction().getPrefixMapping();
    }

    @Override
    public void add(Triple t) throws AddDeniedException {
        getGraphForCurrentTransaction().add(t);
    }

    @Override
    public void add(Node s, Node p, Node o) throws AddDeniedException {
        getGraphForCurrentTransaction().add(s, p, o);
    }

    @Override
    public void delete(Triple t) throws DeleteDeniedException {
        getGraphForCurrentTransaction().delete(t);
    }

    @Override
    public void delete(Node s, Node p, Node o) throws DeleteDeniedException {
        getGraphForCurrentTransaction().delete(s, p, o);
    }

    @Override
    public ExtendedIterator<Triple> find(Triple m) {
        return getGraphForCurrentTransaction().find(m);
    }

    @Override
    public ExtendedIterator<Triple> find(Node s, Node p, Node o) {
        return getGraphForCurrentTransaction().find(s, p, o);
    }

    @Override
    public Stream<Triple> stream(Node s, Node p, Node o) {
        return getGraphForCurrentTransaction().stream(s, p, o);
    }

    @Override
    public Stream<Triple> stream() {
        return getGraphForCurrentTransaction().stream();
    }

    @Override
    public ExtendedIterator<Triple> find() {
        return getGraphForCurrentTransaction().find();
    }

    @Override
    public boolean isIsomorphicWith(Graph g) {
        return getGraphForCurrentTransaction().isIsomorphicWith(g);
    }

    @Override
    public boolean contains(Node s, Node p, Node o) {
        return getGraphForCurrentTransaction().contains(s, p, o);
    }

    @Override
    public boolean contains(Triple t) {
        return getGraphForCurrentTransaction().contains(t);
    }

    @Override
    public void clear() {
        getGraphForCurrentTransaction().clear();
    }

    @Override
    public void remove(Node s, Node p, Node o) {
        getGraphForCurrentTransaction().remove(s, p, o);
    }

    @Override
    public void close() {
        final var txnGraph = this.txnGraph.get();
        if (txnGraph != null) {
            txnGraph.close();
        }
        endOnceByRemovingThreadLocalsAndUnlocking();
    }

    @Override
    public boolean isEmpty() {
        return getGraphForCurrentTransaction().isEmpty();
    }

    @Override
    public int size() {
        return getGraphForCurrentTransaction().size();
    }

    @Override
    public boolean isClosed() {
        final var txnGraph = this.txnGraph.get();
        if (txnGraph != null) {
            return txnGraph.isClosed();
        }
        return true;
    }
}

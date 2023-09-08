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
import org.apache.jena.mem2.GraphMem2Fast;
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

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class GraphWrapperTransactional implements Graph, Transactional {

    private static final String ERROR_MSG_FAILED_TO_ACQUIRE_WRITE_SEMAPHORE_WITHIN_X_MS = "Failed to acquire write semaphore within %s ms.";

    private static final int TIMEOUT_FOR_RETRY_TO_APPLY_DELTAS_TO_STALE_GRAPH_MS = 1000;
    private static final int MAX_DELTA_CHAIN_LENGTH = 100;

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphWrapperTransactional.class);
    private final ConcurrentLinkedQueue<FastDeltaGraph> deltasToApplyToTail = new ConcurrentLinkedQueue<>();

    /**
     * This lock is used to ensure that only one thread can write to the graph at a time.
     * A Semaphore is used instead of a ReentrantLock because it allows the lock to be released
     * in a different thread than the one that acquired it.
     */
    private final Semaphore writeSemaphore = new Semaphore(1);
    private final ReentrantLock lockForUpdatingStaleGraph = new java.util.concurrent.locks.ReentrantLock();
    private final ThreadLocal<Boolean> txnInTransaction = ThreadLocal.withInitial(() -> Boolean.FALSE);
    private final ThreadLocal<TxnType> txnType = new ThreadLocal<>();
    private final ThreadLocal<ReadWrite> txnMode = new ThreadLocal<>();
    private final ThreadLocal<Graph> txnGraph = new ThreadLocal<>();

    private final ThreadLocal<GraphDeltaTransactionManager> txnManager = new ThreadLocal<>();
    private final ThreadLocal<Long> txnReadTransactionVersion = new ThreadLocal<>();
    private final AtomicBoolean delayedUpdateHasBeenScheduled = new AtomicBoolean(false);

    private final AtomicLong dataVersion = new AtomicLong(0);

    private final ForkJoinPool forkJoinPool;
    private final TransactionCoordinatorMRPlusSW transactionCoordinator;
    private GraphDeltaTransactionManager active;
    private GraphDeltaTransactionManager stale;

    public GraphWrapperTransactional() {
        this(GraphMem2Fast::new);
    }

    public GraphWrapperTransactional(final Supplier<Graph> graphFactory) {
        this(graphFactory, ForkJoinPool.commonPool());
    }

    public GraphWrapperTransactional(final Graph graphToWrap, final Supplier<Graph> graphFactory) {
        this(graphToWrap, graphFactory, ForkJoinPool.commonPool());
    }

    public GraphWrapperTransactional(final Supplier<Graph> graphFactory, final ForkJoinPool forkJoinPool) {
        this.active = new GraphDeltaTransactionManagerImpl(graphFactory);
        this.stale = new GraphDeltaTransactionManagerImpl(graphFactory);
        this.forkJoinPool = forkJoinPool;
        this.transactionCoordinator = new TransactionCoordinatorMRPlusSW();
    }

    public GraphWrapperTransactional(final Graph graphToWrap, final Supplier<Graph> graphFactory,
                                     final ForkJoinPool forkJoinPool) {
        this.active = new GraphDeltaTransactionManagerImpl(graphFactory, graphToWrap);
        this.stale = new GraphDeltaTransactionManagerImpl(graphFactory, graphToWrap);
        this.forkJoinPool = forkJoinPool;
        this.transactionCoordinator = new TransactionCoordinatorMRPlusSW();
    }

    int getNumberOfDeltasToApplyToTail() {
        return deltasToApplyToTail.size();
    }

    private Graph getGraphForCurrentTransaction() {
        final var graph = this.txnGraph.get();
        if (graph == null) {
            throw new JenaTransactionException("Not in a transaction.");
        }
        transactionCoordinator.refreshTimeoutForCurrentThread();
        return graph;
    }


    @Override
    public void begin(TxnType txnType) {
        if (isInTransaction())
            throw new JenaTransactionException("Already in a transaction.");
        switchStaleAndActiveIfNeededAndPossible();
        final ReadWrite readWrite = TxnType.convert(txnType);
        if (readWrite == ReadWrite.WRITE) {
            try {
                final var timeoutMs = transactionCoordinator.getTransactionTimeoutMs()
                        + transactionCoordinator.getStaleTransactionRemovalTimerIntervalMs();
                if (!writeSemaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)) {
                    throw new JenaTransactionException(String.format(ERROR_MSG_FAILED_TO_ACQUIRE_WRITE_SEMAPHORE_WITHIN_X_MS, timeoutMs));
                }
            } catch (InterruptedException e) {
                endOnceByRemovingThreadLocalsAndUnlocking();
                Thread.currentThread().interrupt();
            }
        }
        this.txnInTransaction.set(true);
        this.txnMode.set(readWrite);
        this.txnType.set(txnType);
        final var txHead = active;
        this.txnManager.set(txHead);
        switch (readWrite) {
            case READ -> {
                final var graphToRead = txHead.getLastCommittedGraphToRead();
                txnGraph.set(graphToRead);
                txnReadTransactionVersion.set(dataVersion.get());
                transactionCoordinator.registerCurrentThread(() -> {
                    txHead.releaseGraphFromRead(graphToRead);
                });
            }
            case WRITE -> {
                txnGraph.set(txHead.beginTransaction());
                transactionCoordinator.registerCurrentThread(() -> {
                    txHead.rollback();
                    writeSemaphore.release();
                });
            }
            default -> throw new IllegalStateException("Unexpected value: " + readWrite);
        }
    }

    private void endOnceByRemovingThreadLocalsAndUnlocking() {
        if (isInTransaction()) {
            var manager = txnManager.get();
            if (transactionMode() == ReadWrite.WRITE) {
                writeSemaphore.release();
                if (manager != null && manager.isTransactionOpen()) {
                    manager.rollback();
                }
            } else {
                if (manager != null) {
                    manager.releaseGraphFromRead(txnGraph.get());
                }
            }
            txnManager.remove();
            txnGraph.remove();
            txnMode.remove();
            txnType.remove();
            txnReadTransactionVersion.remove();
            txnInTransaction.remove();
        }
    }

    private void switchStaleAndActiveIfNeededAndPossible() {
        if (active.getDeltaChainLength() > 0          // only if the active graph has at least one delta switching makes sense
                && !active.isTransactionOpen()        // only if there is no open transaction on the active graph
                && !stale.hasReader()                // and there is no reader on the stale graph
                && !stale.isTransactionOpen()) {     // and there is no open transaction on the stale graph
            try {
                lockForUpdatingStaleGraph.lock();            // wait for the stale graph to be updated
                // check again, because it could have changed in the meantime
                if (this.deltasToApplyToTail.isEmpty()      //check if all deltas have been applied to the stale graph
                        && !active.isTransactionOpen()        // only if there is no open transaction on the active graph
                        && stale.getDeltaChainLength() == 0) { // check if the stale graph has no deltas
                    final var tmp = stale;
                    stale = active;
                    active = tmp;
                    forkJoinPool.execute(this::updateTailIfPossible); // update the stale graph in the background
                }
            } finally {
                lockForUpdatingStaleGraph.unlock();
            }
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
        try {
            // if we are promoting to isolated, we need to check that the data hasn't changed
            if (txnType == Promote.ISOLATED
                    && txnReadTransactionVersion.get() != dataVersion.get()) {
                writeSemaphore.release();
                return false;
            }
            final var manager = txnManager.get();
            manager.releaseGraphFromRead(txnGraph.get());
            transactionCoordinator.unregisterCurrentThread();
            txnMode.set(ReadWrite.WRITE);
            txnGraph.set(manager.beginTransaction());
            transactionCoordinator.registerCurrentThread(() -> {
                manager.rollback();
                writeSemaphore.release();
            });
            return true;
        } catch (Exception e) {
            writeSemaphore.release();
            throw e;
        }
    }

    @Override
    public void commit() {
        try {
            if (isInTransaction()) {
                transactionCoordinator.unregisterCurrentThread();
                final var manager = txnManager.get();
                switch (transactionMode()) {
                    case READ -> manager.releaseGraphFromRead(txnGraph.get());
                    case WRITE -> {
                        final var delta = (FastDeltaGraph) txnGraph.get();
                        manager.commit();
                        if (delta.hasChanges()) {
                            deltasToApplyToTail.add(delta);
                            if (manager.getDeltaChainLength() > MAX_DELTA_CHAIN_LENGTH) {
                                // if there are many consecutive write transactions, they might always be faster than the async call
                                // to update the stale graph. That way, they might block the update for a long time.
                                // In this case, we call the update synchronously to avoid this problem.
                                this.updateTailIfPossible();
                            }
                            forkJoinPool.execute(this::updateTailIfPossible);
                        }
                    }
                    default -> {
                        endOnceByRemovingThreadLocalsAndUnlocking();
                        throw new JenaTransactionException("Unknown transaction mode: " + transactionMode());
                    }
                }
            }
        } finally {
            endOnceByRemovingThreadLocalsAndUnlocking();
        }
    }

    private void updateTailIfPossible() {
        if (stale.isReadyToMerge()) {
            lockForUpdatingStaleGraph.lock();
            try {
                stale.mergeDeltaChain();
                stale.applyDeltas(deltasToApplyToTail);
            } catch (Exception exception) {
                LOGGER.error("Error while updating stale graph.", exception);
            } finally {
                lockForUpdatingStaleGraph.unlock();
            }
        } else {
            // While there are still open read transactions, we wait a bit and try again.
            // There are no new read transactions possible, so we will eventually succeed.
            if (delayedUpdateHasBeenScheduled.compareAndSet(false, true)) {
                CompletableFuture.delayedExecutor(
                                TIMEOUT_FOR_RETRY_TO_APPLY_DELTAS_TO_STALE_GRAPH_MS,
                                java.util.concurrent.TimeUnit.MILLISECONDS, forkJoinPool)
                        .execute(this::delayedUpdateStaleGraphIfPossible);
            }
        }
    }

    private void delayedUpdateStaleGraphIfPossible() {
        delayedUpdateHasBeenScheduled.set(false);
        updateTailIfPossible();
    }

    @Override
    public void abort() {
        try {
            if (isInTransaction()) {
                transactionCoordinator.unregisterCurrentThread();
                final var manager = txnManager.get();
                switch (transactionMode()) {
                    case READ -> manager.releaseGraphFromRead(txnGraph.get());
                    case WRITE -> manager.rollback();
                    default -> {
                        endOnceByRemovingThreadLocalsAndUnlocking();
                        throw new JenaTransactionException("Unknown transaction mode: " + transactionMode());
                    }
                }
            }
        } finally {
            endOnceByRemovingThreadLocalsAndUnlocking();
        }
    }

    @Override
    public void end() {
        if (isTransactionMode(ReadWrite.WRITE)) {
            abort();
            throw new JenaTransactionException("Write transaction - no commit or abort before end()");
        }
        abort();
    }

    private boolean isTransactionMode(final ReadWrite mode) {
        if (!isInTransaction())
            return false;
        return transactionMode() == mode;
    }

    @Override
    public boolean dependsOn(Graph other) {
        return getGraphForCurrentTransaction().dependsOn(other);
    }

    @Override
    public TransactionHandler getTransactionHandler() {
        return getGraphForCurrentTransaction().getTransactionHandler();
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
        final var g = this.txnGraph.get();
        if (g != null) {
            g.close();
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
        final var g = this.txnGraph.get();
        if (g != null) {
            return g.isClosed();
        }
        return true;
    }

    int getActiveGraphLengthOfDeltaChain() {
        return active.getDeltaChainLength();
    }

    int getStaleGraphLengthOfDeltaChain() {
        return stale.getDeltaChainLength();
    }
}

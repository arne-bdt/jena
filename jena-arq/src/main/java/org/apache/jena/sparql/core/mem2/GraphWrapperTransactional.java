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
import org.apache.jena.mem2.GraphMem2;
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

    private static final int TIMEOUT_FOR_RETRY_TO_APPLY_DELTAS_TO_STALE_GRAPH_MS = 50;

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphWrapperTransactional.class);

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

    private final ThreadLocal<GraphChain> txnManager = new ThreadLocal<>();
    private final ThreadLocal<Long> txnReadTransactionVersion = new ThreadLocal<>();
    private final AtomicBoolean delayedUpdateHasBeenScheduled = new AtomicBoolean(false);

    private final AtomicLong dataVersion = new AtomicLong(0);

    private final ExecutorService executorService;
    private final TransactionCoordinatorMRPlusSW transactionCoordinator;
    private volatile GraphChain active;
    private volatile GraphChain stale;

    private final Object syncSwitchingActiveAndStale = new Object();

    public GraphWrapperTransactional() {
        this(GraphMem2Fast::new);
    }

    public GraphWrapperTransactional(final Supplier<Graph> graphFactory) {
        this.active = new GraphChainImpl(graphFactory.get());
        this.stale = new GraphChainImpl(graphFactory.get());
        this.executorService = Executors.newSingleThreadExecutor();
        this.transactionCoordinator = new TransactionCoordinatorMRPlusSW();
    }

    public GraphWrapperTransactional(final Graph graphToWrap, final Supplier<Graph> graphFactory) {
        final Graph graphToUse = graphFactory.get();
        final Graph activeBase, staleBase;
        if (graphToUse.getClass().equals(graphToWrap.getClass()) // graphToWrap has the same typ the factory produces
                && graphToWrap instanceof GraphMem2 graphMem2) { // and the type is GraphMem2, thus it supports copy
            // we copy the graph to wrap to avoid that the graph to wrap is modified
            activeBase = graphMem2.copy();
            staleBase = graphMem2.copy();
        } else {
            activeBase = graphToUse; /*otherwise use the supplied empty graph*/
            if (activeBase instanceof GraphMem2 activeBaseAsMem2) { //if it supports copy
                graphToWrap.find().forEachRemaining(activeBaseAsMem2::add); // only fill the activeBase
                staleBase = activeBaseAsMem2.copy(); // copy activeBase to staleBase
            } else {  // if copy is not supported, we have to fill both graphs
                staleBase = graphFactory.get();
                graphToWrap.find().forEachRemaining(t -> {
                    activeBase.add(t);
                    staleBase.add(t);
                });
            }
        }
        this.active = new GraphChainImpl(activeBase);
        this.stale = new GraphChainImpl(staleBase);
        this.executorService = Executors.newSingleThreadExecutor();
        this.transactionCoordinator = new TransactionCoordinatorMRPlusSW();
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
        final ReadWrite readWrite = TxnType.convert(txnType);
        if (readWrite == ReadWrite.WRITE) {
            try {
                final var timeoutMs = transactionCoordinator.getTransactionTimeoutMs()
                        + transactionCoordinator.getStaleTransactionRemovalTimerIntervalMs();
                if (!writeSemaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)) {
                    throw new JenaTransactionException(String.format(ERROR_MSG_FAILED_TO_ACQUIRE_WRITE_SEMAPHORE_WITHIN_X_MS, (Integer)timeoutMs));
                }
            } catch (InterruptedException e) {
                endOnceByRemovingThreadLocalsAndUnlocking();
                throw new JenaTransactionException("Interrupted while waiting for write semaphore.", e);
            }
            trySwitchStaleAndActiveForNewWriteTransaction();
        }
        this.txnInTransaction.set(true);
        this.txnMode.set(readWrite);
        this.txnType.set(txnType);
        final GraphChain txHead;
        synchronized (syncSwitchingActiveAndStale) {
            txHead = active;
        }
        this.txnManager.set(txHead);
        switch (readWrite) {
            case READ -> {
                final var graphToRead = txHead.getLastCommittedAndIncReaderCounter();
                txnGraph.set(graphToRead);
                txnReadTransactionVersion.set(dataVersion.get());
                transactionCoordinator.registerCurrentThread(() -> {
                    txHead.decrementReaderCounter();
                });
            }
            case WRITE -> {
                txnGraph.set(txHead.prepareGraphForWriting());
                transactionCoordinator.registerCurrentThread(() -> {
                    txHead.discardGraphForWriting();
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
                if (manager != null && manager.hasGraphForWriting()) {
                    manager.discardGraphForWriting();
                }
            } else {
                if (manager != null) {
                    manager.decrementReaderCounter();
                }
            }
        }
        txnManager.remove();
        txnGraph.remove();
        txnMode.remove();
        txnType.remove();
        txnReadTransactionVersion.remove();
        txnInTransaction.remove();
    }

    /***
     * This method tries to switch the stale and the active graph.
     * It must only be entered when the write semaphore is acquired.
     * It only switches if the active graph has at least one delta and there is no reader on the stale graph.
     * If the switch is successful, the stale graph is updated in the background.
     */
    private void trySwitchStaleAndActiveForNewWriteTransaction() {
        if (active.getDeltaChainLength() == 0          // only if the active graph has no deltas
                || stale.hasReader()) {                 // or there are still reader on the stale graph
            return;
        }
        boolean sheduleNextUpdate = false;
        try {
            lockForUpdatingStaleGraph.lock();            // wait for the stale graph to be updated
            if (active.getDeltaChainLength() > 0          // only if the active graph has at least one delta switching makes sense
                    && !stale.hasReader()) {              // and there is no reader on the stale graph
                stale.mergeDeltaChain();
                stale.applyQueuedDeltas();
                synchronized (syncSwitchingActiveAndStale) {
                    final var tmp = active;
                    active = stale;
                    stale = tmp;
                }
                sheduleNextUpdate = true;
            }
        } finally {
            lockForUpdatingStaleGraph.unlock();
        }
        if(sheduleNextUpdate) {
            tryScheduleNextTailUpdate();
        }
    }


    private void tryScheduleNextTailUpdate() {
        // While there are still open read transactions, we wait a bit and try again.
        // When there are no new read transactions possible, so we will eventually succeed.
        if (delayedUpdateHasBeenScheduled.compareAndSet(false, true)) {
            executorService.execute(this::parallelUpdateTailIfPossible);
        }
    }

    private void parallelUpdateTailIfPossible() {
        delayedUpdateHasBeenScheduled.set(false);
        if (active.getDeltaChainLength() == 0) {          // only if the active graph has no deltas
            if (stale.hasReader()) {                 // or there are still reader on the stale graph
                tryScheduleNextTailUpdate();
            }
            return;
        }
        boolean doRetry = false;
        try {
            lockForUpdatingStaleGraph.lock();            // wait for the stale graph to be updated
            if(stale.isReadyToMerge()) {
                stale.mergeDeltaChain();
                stale.applyQueuedDeltas();
                if(writeSemaphore.tryAcquire()) {
                    try {
                        synchronized (syncSwitchingActiveAndStale) {
                            final var tmp = active;
                            active = stale;
                            stale = tmp;
                        }
                        if (stale.isReadyToMerge()) {
                            stale.mergeDeltaChain();
                            stale.applyQueuedDeltas();
                        } else {
                            doRetry = true;
                        }
                    } finally {
                        writeSemaphore.release();
                    }
                } else {
                    doRetry = true;
                }

            } else {
                doRetry = true;
            }
        } finally {
            lockForUpdatingStaleGraph.unlock();
        }
        if(doRetry) {
            tryScheduleNextTailUpdate();
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
            manager.decrementReaderCounter();
            transactionCoordinator.unregisterCurrentThread();
            txnMode.set(ReadWrite.WRITE);
            txnGraph.set(manager.prepareGraphForWriting());
            transactionCoordinator.registerCurrentThread(() -> {
                manager.discardGraphForWriting();
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
                    case READ -> manager.decrementReaderCounter();
                    case WRITE -> {
                        final var delta = (FastDeltaGraph) txnGraph.get();
                        var hasChanges = delta.hasChanges();
                        if (hasChanges) {
                            manager.linkGraphForWritingToChain();
                            dataVersion.incrementAndGet(); // increment the data version to signal that the data has changed
                            stale.queueDelta(delta);
                        } else {
                            manager.discardGraphForWriting();
                        }
                        writeSemaphore.release();
                        if(hasChanges) {
                            tryScheduleNextTailUpdate();
                        }
                    }
                    default -> {
                        throw new JenaTransactionException("Unknown transaction mode: " + transactionMode());
                    }
                }
                txnInTransaction.set(false);
            }
        }
        finally {
            endOnceByRemovingThreadLocalsAndUnlocking();
        }
    }

    @Override
    public void abort() {
        try {
            if (isInTransaction()) {
                transactionCoordinator.unregisterCurrentThread();
                final var manager = txnManager.get();
                switch (transactionMode()) {
                    case READ -> manager.decrementReaderCounter();
                    case WRITE -> {
                        manager.discardGraphForWriting();
                        writeSemaphore.release();
                    }
                    default -> {
                        throw new JenaTransactionException("Unknown transaction mode: " + transactionMode());
                    }
                }
                txnInTransaction.set(false);
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
            executorService.shutdown();
            try {
                transactionCoordinator.close();
            } catch (Exception e) {
                endOnceByRemovingThreadLocalsAndUnlocking();
                throw new RuntimeException(e);
            }
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

    int getActiveGraphLengthOfDeltaQueue() {
        return active.getDeltaQueueLength();
    }

    int getStaleGraphLengthOfDeltaQueue() {
        return stale.getDeltaQueueLength();
    }

    public void printDeltaChainLengths() {
        //printf: Active (instance #) has delta chain length of (length). Stale (instance #) has delta chain length of (length).
        System.out.printf("Active (instance %s) has delta chain length of %s. Stale (instance %s) has delta chain length of %s.%n",
                active.getInstanceId(), active.getDeltaChainLength(), stale.getInstanceId(), stale.getDeltaChainLength());
    }
}

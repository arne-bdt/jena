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

import org.apache.jena.atlas.lib.Copyable;
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

import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * A {@link Graph} that allows for concurrent read and write transactions.
 * <p>
 * Read transactions can be executed concurrently, but write transactions are executed exclusively.
 * Read transactions are executed on a snapshot of last committed graph. They do not have to wait for other read
 * or write transactions to finish.
 * </p>
 * <p>
 * Read transactions are executed immediately, but a new write transactions waits until the previous write transaction
 * has finished.
 * </p>
 * <p>
 * Write transactions may not be executed in the order they were started. The semaphore that is used to ensure that only
 * one write transaction is executed at a time is not fair, for performance reasons.
 * </p>
 * <p>
 * When a write transaction is committed, the changes are visible to all new transactions. The changes are not visible
 * to read transactions that were started before the write transaction was committed.
 * </p>
 * <p>
 * There are two types of graph-chains that are used to manage the concurrency between transactions:
 * </p>
 * <ul>
 *     <li>
 *         The <i>active graph-chain</i> is the graph-chain that is currently used for new read and write transactions.
 *     </li>
 *     <li>
 *         The <i>stale graph-chain</i> is the graph-chain that is used to let older read transactions finish.
 *         New commits are applied to the stale graph-chain.
 *         When the stale graph-chain has no more readers, all outstanding deltas are merged, and then it is switched
 *         with the active graph-chain.
 *     </li>
 * </ul>
 * <p>
 * A background thread is used to merge the deltas of the stale graph-chain and to switch the stale and active
 * graph-chains.
 * </p>
 * <p>
 * A transaction coordinator is used to keep track of the threads that are currently running transactions.
 * It is also responsible for checking if a thread has timed out and calling the runnable that was passed to the
 * {@link TransactionCoordinator#registerCurrentThread(Runnable)} method.
 */
public class GraphMem2Txn implements Graph, Transactional {

    private static final String ERROR_MSG_FAILED_TO_ACQUIRE_WRITE_SEMAPHORE_WITHIN_X_MS = "Failed to acquire write semaphore within %s ms.";

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphMem2Txn.class);
    public static final String NOT_IN_A_TRANSACTION = "Not in a transaction.";

    /**
     * This lock is used to ensure that only one thread can write to the graph at a time.
     * A Semaphore is used instead of a ReentrantLock because it allows the lock to be released
     * in a different thread than the one that acquired it.
     */
    private final Semaphore writeSemaphore = new Semaphore(1, true);

    private record TransactionInfo(UUID transactionID, AtomicBoolean isAlive, TxnType type, ReadWrite mode, Graph graph, GraphChain activeChain, long version) {}
    private final ThreadLocal<TransactionInfo> threadLocalTxnInfo = new ThreadLocal<>();
    private final AtomicLong dataVersion = new AtomicLong(0);
    private final TransactionCoordinator transactionCoordinator;
    private GraphChain active;
    private GraphChain stale;

    private volatile boolean isClosed = false;

    private final Object syncActiveAndStaleSwitching = new Object();

    private static final int DEFAULT_MAX_CHAIN_LENGTH = 2;

    private final int maxChainLength;

    private Thread backgroundUpdateThread = Thread.startVirtualThread(() -> {});

    public GraphMem2Txn(final int maxChainLength) {
        this(GraphMem2Fast::new, maxChainLength, new TransactionCoordinatorImpl());
    }

    public GraphMem2Txn() {
        this(GraphMem2Fast::new);
    }

    public GraphMem2Txn(final Supplier<Graph> graphFactory) {
        this(graphFactory, DEFAULT_MAX_CHAIN_LENGTH, new TransactionCoordinatorImpl());
    }
    public GraphMem2Txn(final Supplier<Graph> graphFactory, final int maxChainLength, final TransactionCoordinator transactionCoordinator) {
        this.maxChainLength = maxChainLength;
        this.active = new GraphChainImpl(graphFactory);
        this.stale = new GraphChainImpl(graphFactory);
        this.transactionCoordinator = transactionCoordinator;
    }

    public GraphMem2Txn(final Graph graphToWrap, final Supplier<Graph> graphFactory) {
        this(graphToWrap, graphFactory, DEFAULT_MAX_CHAIN_LENGTH);
    }
    @SuppressWarnings("unchecked")
    public GraphMem2Txn(final Graph graphToWrap, final Supplier<Graph> graphFactory, final int maxChainLength) {
        this.maxChainLength = maxChainLength;
        final var newGraph = graphFactory.get();
        final Graph activeBase, staleBase;
        if(graphToWrap.getClass().equals(newGraph) && graphToWrap instanceof Copyable<?>) {
            final var copyable = (Copyable<Graph>) graphToWrap;
            activeBase = copyable.copy();
            staleBase = copyable.copy();
        } else
        {
            activeBase = graphFactory.get();
            staleBase = graphFactory.get();
            graphToWrap.find().forEachRemaining(t -> {
                activeBase.add(t);
                staleBase.add(t);
            });
        }
        this.active = new GraphChainImpl(activeBase, graphFactory);
        this.stale = new GraphChainImpl(staleBase, graphFactory);
        this.transactionCoordinator = new TransactionCoordinatorImpl();
    }

    private Graph getGraphForCurrentTransaction() {
        final var tI = this.getTransactionInfoOrNull();
        if (tI == null) {
            throw new JenaTransactionException(NOT_IN_A_TRANSACTION);
        }
        transactionCoordinator.refreshTimeoutForCurrentThread();
        return tI.graph;
    }

    private synchronized void triggerBackgroundUpdate() {
        if (!backgroundUpdateThread.isAlive()) {
            backgroundUpdateThread = Thread.startVirtualThread(this::backgroundUpdate);
        }
    }

    @SuppressWarnings("java:S2142")
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
                    throw new JenaTransactionException(String.format(ERROR_MSG_FAILED_TO_ACQUIRE_WRITE_SEMAPHORE_WITHIN_X_MS, timeoutMs));
                }

            } catch (InterruptedException e) {
                endOnceByRemovingThreadLocalsAndUnlocking();
                throw new JenaTransactionException("Interrupted while waiting for write semaphore.", e);
            }
        }
        final var transactionId = UUID.randomUUID();
        final TransactionInfo info;
        switch (readWrite) {
            case READ -> {
                final GraphChain activeChain;
                synchronized (syncActiveAndStaleSwitching) {
                    activeChain = this.active;
                    info = new TransactionInfo(transactionId, new AtomicBoolean(true), txnType, readWrite, activeChain.getLastCommittedAndAddReader(transactionId), activeChain, dataVersion.get());
                }
                threadLocalTxnInfo.set(info);
                transactionCoordinator.registerCurrentThread(() -> {
                    info.isAlive.set(false);
                    activeChain.removeReader(transactionId);
                    triggerBackgroundUpdate();
                });
            }
            case WRITE -> {
                onBeginWriteTrySwitchActiveAndStale();
                final GraphChain activeChain;
                synchronized (syncActiveAndStaleSwitching) {
                    activeChain = this.active;
                    info = new TransactionInfo(transactionId, new AtomicBoolean(true), txnType, readWrite, activeChain.prepareGraphForWriting(), activeChain, dataVersion.get());
                }
                threadLocalTxnInfo.set(info);
                transactionCoordinator.registerCurrentThread(() -> {
                    info.isAlive.set(false);
                    synchronized (syncActiveAndStaleSwitching) {
                        activeChain.discardGraphForWriting();
                        writeSemaphore.release();
                    }
                    triggerBackgroundUpdate();
                });
            }
            default -> throw new IllegalStateException("Unexpected value: " + readWrite);
        }
    }

    private void endOnceByRemovingThreadLocalsAndUnlocking() {
        final var txnInfo = this.getTransactionInfoOrNull();
        if (txnInfo != null) {
            switch (txnInfo.mode) {
                case READ -> txnInfo.activeChain.removeReader(txnInfo.transactionID);
                case WRITE -> {
                    txnInfo.activeChain.discardGraphForWriting();
                    writeSemaphore.release();
                }
                default -> throw new IllegalStateException("Unexpected value: " + txnInfo.mode);
            }
            this.threadLocalTxnInfo.remove();
            triggerBackgroundUpdate();
        }
    }

    private void onBeginWriteTrySwitchActiveAndStale() {
        while(!active.hasNothingToMergeAndNoDeltasToApply()
                && (stale.hasNothingToMergeAndNoDeltasToApply()
                || stale.hasNoReader()
                || active.getDeltaChainLength() >= this.maxChainLength)) {
            synchronized (syncActiveAndStaleSwitching) {
                if (stale.hasNoReader()) {
                    stale.mergeAndApplyDeltas();
                }
                if (stale.hasNothingToMergeAndNoDeltasToApply()) {
                    final var tmp = active;
                    active = stale;
                    stale = tmp;
                }
            }
        }
    }
    private void backgroundUpdate() {
        boolean didSomething = true;
        while (!isClosed && didSomething) {
            didSomething = false;
            try {
                synchronized (syncActiveAndStaleSwitching) {
                    if (!active.hasNothingToMergeAndNoDeltasToApply() && stale.hasNoReader()) {
                        if (!stale.hasNothingToMergeAndNoDeltasToApply()) {
                            stale.mergeAndApplyDeltas();
                        }
                        final var tmp = active;
                        active = stale;
                        stale = tmp;
                        didSomething = true;
                    }
                    if(stale.isReadyToMerge() && !stale.hasNothingToMergeAndNoDeltasToApply()) {
                        stale.mergeAndApplyDeltas();
                        didSomething = true;
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Exception in background update loop.", e);
                throw e;
            }
        }
    }

    @Override
    public ReadWrite transactionMode() {
        final var txnInfo = this.getTransactionInfoOrNull();
        if (txnInfo == null) {
            return null;
        }
        return txnInfo.mode;
    }

    @Override
    public TxnType transactionType() {
        final var txnInfo = this.getTransactionInfoOrNull();
        if (txnInfo == null) {
            return null;
        }
        return txnInfo.type;
    }

    private TransactionInfo getTransactionInfoOrNull() {
        final var txnInfo = this.threadLocalTxnInfo.get();
        if (txnInfo == null)
            return null;

        if(!txnInfo.isAlive.get()) {
            this.threadLocalTxnInfo.remove();
            return null;
        }
        return txnInfo;
    }

    @Override
    public boolean isInTransaction() {
        return getTransactionInfoOrNull() != null;
    }

    @Override
    public boolean promote(Promote txnType) {
        if (!writeSemaphore.tryAcquire()) {
            return false;
        }
        try {
            final var txnInfo = this.getTransactionInfoOrNull();
            if (txnInfo == null) {
                writeSemaphore.release();
                throw new JenaTransactionException(NOT_IN_A_TRANSACTION);
            }
            // if we are promoting to isolated, we need to check that the data hasn't changed
            if (txnType == Promote.ISOLATED
                    && txnInfo.version != dataVersion.get()) {
                writeSemaphore.release();
                return false;
            }
            transactionCoordinator.unregisterCurrentThread();
            txnInfo.activeChain.removeReader(txnInfo.transactionID);
            final GraphChain activeChain;
            final TransactionInfo info;
            synchronized (syncActiveAndStaleSwitching) {
                activeChain = this.active;
                info = new TransactionInfo(txnInfo.transactionID, new AtomicBoolean(true), txnInfo.type, ReadWrite.WRITE, activeChain.prepareGraphForWriting(), activeChain, dataVersion.get());
            }
            this.threadLocalTxnInfo.set(info);
            transactionCoordinator.registerCurrentThread(() -> {
                info.isAlive.set(false);
                activeChain.discardGraphForWriting();
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
        final var txnInfo = this.getTransactionInfoOrNull();
        if (txnInfo == null) {
            throw new JenaTransactionException(NOT_IN_A_TRANSACTION);
        }
        try {
            transactionCoordinator.unregisterCurrentThread();
            switch (transactionMode()) {
                case READ -> {
                    txnInfo.activeChain.removeReader(txnInfo.transactionID);
                    triggerBackgroundUpdate();
                }
                case WRITE -> {
                    final var delta = (DeltaGraph) txnInfo.graph;
                    if (delta.hasChanges()) {
                        synchronized (syncActiveAndStaleSwitching) {
                            if(active == txnInfo.activeChain) {
                                txnInfo.activeChain.linkGraphForWritingToChain();
                                stale.queueDelta(delta);
                            } else { //if the active graph has changed, we have to rebase the delta
                                if(stale.getDataVersion() != active.getDataVersion()) {
                                    // this should never happen, because the active graph should not have changed
                                    throw new JenaTransactionException("Rebase not possible: new active graph has already been changed by another transaction.");
                                }
                                stale.discardGraphForWriting();
                                stale.queueDelta(delta);
                                active.rebaseAndLinkDeltaForWritingToChain(delta);
                            }
                            dataVersion.incrementAndGet(); // increment the data version to signal that the data has changed
                        }
                    } else {
                        txnInfo.activeChain.discardGraphForWriting();
                    }
                    triggerBackgroundUpdate();
                    writeSemaphore.release();
                }
                default -> throw new JenaTransactionException("Unknown transaction mode: " + transactionMode());
            }
            this.threadLocalTxnInfo.remove();
        }
        catch (Exception e) {
            endOnceByRemovingThreadLocalsAndUnlocking();
            throw e;
        }
    }

    @Override
    public void abort() {
        final var txnInfo = this.getTransactionInfoOrNull();
        if (txnInfo == null) {
            return;
        }
        try {
            transactionCoordinator.unregisterCurrentThread();
            switch (transactionMode()) {
                case READ -> {
                    txnInfo.activeChain.removeReader(txnInfo.transactionID);
                    triggerBackgroundUpdate();
                }
                case WRITE -> {
                    txnInfo.activeChain.discardGraphForWriting();
                    triggerBackgroundUpdate();
                    writeSemaphore.release();
                }
                default -> throw new JenaTransactionException("Unknown transaction mode: " + transactionMode());
            }
            this.threadLocalTxnInfo.remove();
        } catch (Exception e) {
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
        try {
            final var txnInfo = this.getTransactionInfoOrNull();
            if (txnInfo != null) {
                try {
                    txnInfo.graph.close();
                } finally {
                    this.threadLocalTxnInfo.remove();
                }
            }
        } finally {
            try {
                transactionCoordinator.close();
            } catch (Exception e) {
                LOGGER.error("Exception while closing transaction coordinator.", e);
            } finally {
                this.isClosed = true;
            }
        }
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
        final var txnInfo = this.getTransactionInfoOrNull();
        if (txnInfo != null) {
            return txnInfo.graph.isClosed();
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
}

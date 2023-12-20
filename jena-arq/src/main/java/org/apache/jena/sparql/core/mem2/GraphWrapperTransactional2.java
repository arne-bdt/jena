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

import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class GraphWrapperTransactional2 implements Graph, Transactional {

    private static final String ERROR_MSG_FAILED_TO_ACQUIRE_WRITE_SEMAPHORE_WITHIN_X_MS = "Failed to acquire write semaphore within %s ms.";

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphWrapperTransactional2.class);

    /**
     * This lock is used to ensure that only one thread can write to the graph at a time.
     * A Semaphore is used instead of a ReentrantLock because it allows the lock to be released
     * in a different thread than the one that acquired it.
     */
    private final Semaphore writeSemaphore = new Semaphore(1);

    private record TransactionInfo(UUID transactionID, AtomicBoolean isAlive, TxnType type, ReadWrite mode, Graph graph, GraphChain activeChain, long version) {}
    private final ThreadLocal<TransactionInfo> txnInfo = new ThreadLocal<>();
    private final AtomicLong dataVersion = new AtomicLong(0);
    private final TransactionCoordinator transactionCoordinator;
    private final Thread backgroundThread;
    private GraphChain active;
    private GraphChain stale;

    private volatile boolean isClosed = false;

    private final Object syncBackgroundUpdateLoop = new Object();

    private final Object syncActiveAndStaleSwitching = new Object();

    private static final int DEFAULT_MAX_CHAIN_LENGTH = 3;

    private final int maxChainLength;

    public GraphWrapperTransactional2(final int maxChainLength) {
        this(GraphMem2Fast::new, maxChainLength, new TransactionCoordinatorImpl());
    }

    public GraphWrapperTransactional2() {
        this(GraphMem2Fast::new);
    }

    public GraphWrapperTransactional2(final Supplier<Graph> graphFactory) {
        this(graphFactory, DEFAULT_MAX_CHAIN_LENGTH, new TransactionCoordinatorImpl());
    }
    public GraphWrapperTransactional2(final Supplier<Graph> graphFactory, final int maxChainLength, final TransactionCoordinator transactionCoordinator) {
        this.maxChainLength = maxChainLength;
        this.active = new GraphChainImpl(graphFactory.get());
        this.stale = new GraphChainImpl(graphFactory.get());
        this.transactionCoordinator = transactionCoordinator;
        this.backgroundThread = new Thread(() -> backgroundUpdateLoop());
        this.backgroundThread.start();
    }

    public GraphWrapperTransactional2(final Graph graphToWrap, final Supplier<Graph> graphFactory) {
        this(graphToWrap, graphFactory, DEFAULT_MAX_CHAIN_LENGTH);
    }
    public GraphWrapperTransactional2(final Graph graphToWrap, final Supplier<Graph> graphFactory, final int maxChainLength) {
        this.maxChainLength = maxChainLength;
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
        this.transactionCoordinator = new TransactionCoordinatorImpl();
        this.backgroundThread = new Thread(this::backgroundUpdateLoop);
        this.backgroundThread.start();
    }

    private Graph getGraphForCurrentTransaction() {
        final var txnInfo = this.getTransactionInfoOrNull();
        if (txnInfo == null) {
            throw new JenaTransactionException("Not in a transaction.");
        }
        transactionCoordinator.refreshTimeoutForCurrentThread();
        return txnInfo.graph;
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
        }
        final var transactionId = UUID.randomUUID();
        final TransactionInfo info;
        switch (readWrite) {
            case READ -> {
                final GraphChain activeChain = this.active;
                info = new TransactionInfo(transactionId, new AtomicBoolean(true), txnType, readWrite, activeChain.getLastCommittedAndAddReader(transactionId), activeChain, dataVersion.get());
                txnInfo.set(info);
                transactionCoordinator.registerCurrentThread(() -> {
                    info.isAlive.set(false);
                    activeChain.removeReader(transactionId);
                    synchronized (syncBackgroundUpdateLoop) {
                        syncBackgroundUpdateLoop.notifyAll();
                    }
                });
            }
            case WRITE -> {
                onBeginWriteTrySwitchActiveAndStale();
                final GraphChain activeChain = this.active;
                info = new TransactionInfo(transactionId, new AtomicBoolean(true), txnType, readWrite, activeChain.prepareGraphForWriting(), activeChain, dataVersion.get());
                txnInfo.set(info);
                transactionCoordinator.registerCurrentThread(() -> {
                    info.isAlive.set(false);
                    synchronized (syncActiveAndStaleSwitching) {
                        activeChain.discardGraphForWriting();
                        writeSemaphore.release();
                    }
                    synchronized (syncBackgroundUpdateLoop) {
                        syncBackgroundUpdateLoop.notifyAll();
                    }
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
            this.txnInfo.remove();
        }
    }

    private void onBeginWriteTrySwitchActiveAndStale() {
        while(!active.hasNothingToMergeAndNoDeltasToApply()
                && (stale.hasNothingToMergeAndNoDeltasToApply()
                    || !stale.hasReader()
                    || active.getDeltaChainLength() >= this.maxChainLength)) {
            synchronized (syncActiveAndStaleSwitching) {
                if (!stale.hasReader()) {
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

    private void backgroundUpdateLoop() {
        while (!isClosed) {
            try {
                synchronized (syncActiveAndStaleSwitching) {
                    if (!active.hasNothingToMergeAndNoDeltasToApply() && !stale.hasReader()) {
                        if (!stale.hasNothingToMergeAndNoDeltasToApply()) {
                            stale.mergeAndApplyDeltas();
                        }
                        final var tmp = active;
                        active = stale;
                        stale = tmp;
                    }
                    if(!stale.hasReader() && !stale.hasNothingToMergeAndNoDeltasToApply()) {
                        stale.mergeAndApplyDeltas();
                    }
                }
                final var activeChain = this.active;
                final var staleChain = this.stale;
                synchronized (syncBackgroundUpdateLoop) {
                    if(activeChain.hasNothingToMergeAndNoDeltasToApply()
                            && staleChain.hasNothingToMergeAndNoDeltasToApply()) {
                        syncBackgroundUpdateLoop.wait();
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Exception in background update loop.", e);
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
        final var txnInfo = this.txnInfo.get();
        if (txnInfo == null)
            return null;

        if(!txnInfo.isAlive.get()) {
            this.txnInfo.remove();
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
            // if we are promoting to isolated, we need to check that the data hasn't changed
            if (txnType == Promote.ISOLATED
                    && txnInfo.version != dataVersion.get()) {
                writeSemaphore.release();
                return false;
            }
            transactionCoordinator.unregisterCurrentThread();
            txnInfo.activeChain.removeReader(txnInfo.transactionID);
            final var activeChain = this.active;
            final var info = new TransactionInfo(txnInfo.transactionID, new AtomicBoolean(true), txnInfo.type, ReadWrite.WRITE, activeChain.prepareGraphForWriting(), activeChain, dataVersion.get());
            this.txnInfo.set(info);
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
            throw new JenaTransactionException("Not in a transaction.");
        }
        try {
            transactionCoordinator.unregisterCurrentThread();
            switch (transactionMode()) {
                case READ -> {
                    txnInfo.activeChain.removeReader(txnInfo.transactionID);
                    synchronized (syncBackgroundUpdateLoop) {
                        syncBackgroundUpdateLoop.notifyAll();
                    }
                }
                case WRITE -> {
                    final var delta = (FastDeltaGraph) txnInfo.graph;
                    var hasChanges = delta.hasChanges();
                    if (hasChanges) {
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
                                //printf: Rebased
//                                System.out.println("Rebased");
                            }
                            dataVersion.incrementAndGet(); // increment the data version to signal that the data has changed
                        }
                    } else {
                        txnInfo.activeChain.discardGraphForWriting();
                    }
                    synchronized (syncBackgroundUpdateLoop) {
                        syncBackgroundUpdateLoop.notifyAll();
                    }
                    writeSemaphore.release();
                }
                default -> {
                    throw new JenaTransactionException("Unknown transaction mode: " + transactionMode());
                }
            }
            this.txnInfo.remove();
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
            throw new JenaTransactionException("Not in a transaction.");
        }
        try {
            transactionCoordinator.unregisterCurrentThread();
            switch (transactionMode()) {
                case READ -> {
                    txnInfo.activeChain.removeReader(txnInfo.transactionID);
                    synchronized (syncBackgroundUpdateLoop) {
                        syncBackgroundUpdateLoop.notifyAll();
                    }
                }
                case WRITE -> {
                    txnInfo.activeChain.discardGraphForWriting();
                    synchronized (syncBackgroundUpdateLoop) {
                        syncBackgroundUpdateLoop.notifyAll();
                    }
                    writeSemaphore.release();
                }
                default -> {
                    throw new JenaTransactionException("Unknown transaction mode: " + transactionMode());
                }
            }
            this.txnInfo.remove();
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
                    this.txnInfo.remove();
                }
            }
        } finally {
            try {
                transactionCoordinator.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                this.isClosed = true;
                try {
                    backgroundThread.join(200);
                } catch (InterruptedException e) {
                    backgroundThread.interrupt();
                    throw new RuntimeException(e);
                }
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

    public void printDeltaChainLengths() {
        //printf: Active (instance #) has delta chain length of (length). Stale (instance #) has delta chain length of (length).
        System.out.printf("Active (instance %s) has delta chain length of %s. Stale (instance %s) has delta chain length of %s.%n",
                active.getInstanceId(), active.getDeltaChainLength(), stale.getInstanceId(), stale.getDeltaChainLength());
    }
}

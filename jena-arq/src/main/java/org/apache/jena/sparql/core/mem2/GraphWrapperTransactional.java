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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class GraphWrapperTransactional implements Graph, Transactional {

    private static final int MAX_DELTA_CHAIN_LENGTH = 100;

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphWrapperTransactional.class);

    private final TransactionCoordinator transactionCoordinator;
    private final ConcurrentLinkedQueue<FastDeltaGraph> deltasToApplyToStaleGraph = new ConcurrentLinkedQueue<>();
    private final ReentrantLock lockForUpdatingStaleGraph = new java.util.concurrent.locks.ReentrantLock();

    private final ReentrantLock lockForBeginTransaction = new java.util.concurrent.locks.ReentrantLock();
    private final ForkJoinPool forkJoinPool;
    private GraphWrapperChainingDeltasTransactional staleGraph;
    private GraphWrapperChainingDeltasTransactional activeGraph;
    private final ThreadLocal<GraphWrapperChainingDeltasTransactional> txnGraph = new ThreadLocal<>();
    private final AtomicBoolean delayedUpdateHasBeenScheduled = new AtomicBoolean(false);

    int getNumberOfDeltasToApplyToStaleGraph() {
        return deltasToApplyToStaleGraph.size();
    }

    int getActiveGraphLengthOfDeltaChain() {
        return activeGraph.getLengthOfDeltaChain();
    }

    int getStaleGraphLengthOfDeltaChain() {
        return staleGraph.getLengthOfDeltaChain();
    }

    public GraphWrapperTransactional(final TransactionCoordinator transactionCoordinator) {
        this(transactionCoordinator, GraphMem2Fast::new);
    }

    public GraphWrapperTransactional(final TransactionCoordinator transactionCoordinator, final Supplier<Graph> graphFactory) {
        this(transactionCoordinator, graphFactory, ForkJoinPool.commonPool());
    }

    public GraphWrapperTransactional(final TransactionCoordinator transactionCoordinator, final Graph graphToWrap, final Supplier<Graph> graphFactory) {
        this(transactionCoordinator, graphToWrap, graphFactory, ForkJoinPool.commonPool());
    }

    public GraphWrapperTransactional(final TransactionCoordinator transactionCoordinator,
                                     final Supplier<Graph> graphFactory, final ForkJoinPool forkJoinPool) {
        this.transactionCoordinator = transactionCoordinator;
        this.staleGraph = new GraphWrapperChainingDeltasTransactional(graphFactory, transactionCoordinator,
                deltasToApplyToStaleGraph::add);
        this.activeGraph = new GraphWrapperChainingDeltasTransactional(graphFactory, transactionCoordinator,
                deltasToApplyToStaleGraph::add);
        this.forkJoinPool = forkJoinPool;
    }

    public GraphWrapperTransactional(final TransactionCoordinator transactionCoordinator,
                                     final Graph graphToWrap, final Supplier<Graph> graphFactory,
                                     final ForkJoinPool forkJoinPool) {
        this.transactionCoordinator = transactionCoordinator;
        this.staleGraph = new GraphWrapperChainingDeltasTransactional(graphToWrap, graphFactory,
                transactionCoordinator, deltasToApplyToStaleGraph::add);
        this.activeGraph = new GraphWrapperChainingDeltasTransactional(graphToWrap, graphFactory,
                transactionCoordinator, deltasToApplyToStaleGraph::add);
        this.forkJoinPool = forkJoinPool;
    }

    public TransactionCoordinator getTransactionCoordinator() {
        return transactionCoordinator;
    }

    private GraphWrapperChainingDeltasTransactional getGraphForCurrentTransaction() {
        final var graph = this.txnGraph.get();
        if (graph == null) {
            throw new JenaTransactionException("Not in a transaction.");
        }
        return graph;
    }

    @Override
    public void begin(TxnType txnType) {
        try {
            lockForBeginTransaction.lock();
            switchStaleAndActiveIfNeededAndPossible();
            txnGraph.set(activeGraph);
            activeGraph.begin(txnType);
        } finally {
            lockForBeginTransaction.unlock();
        }
    }

    private void switchStaleAndActiveIfNeededAndPossible() {
        if (activeGraph.getLengthOfDeltaChain() > 0          // only if the active graph has at least one delta
                && !staleGraph.hasOpenReadTransactions()     // only if there are no open read transactions on the stale graph
                && !activeGraph.hasOpenWriteTransaction()) { // and there is no active write transaction
            try {
                lockForUpdatingStaleGraph.lock();            // wait for the stale graph to be updated
                // check again, because it could have changed in the meantime
                if (this.deltasToApplyToStaleGraph.isEmpty() //check if all deltas have been applied to the stale graph
                        && activeGraph.getLengthOfDeltaChain() > 0
                        && staleGraph.getLengthOfDeltaChain() == 0 // only if the stale graph has no deltas
                        && !staleGraph.hasOpenReadTransactions()
                        && !activeGraph.hasOpenWriteTransaction()) {
                    final var tmp = staleGraph;
                    staleGraph = activeGraph;
                    activeGraph = tmp;
                    forkJoinPool.execute(this::updateStaleGraphIfPossible); // update the stale graph in the background
                }
            } finally {
                lockForUpdatingStaleGraph.unlock();
            }
        }
    }

    @Override
    public ReadWrite transactionMode() {
        return getGraphForCurrentTransaction().transactionMode();
    }

    @Override
    public TxnType transactionType() {
        return getGraphForCurrentTransaction().transactionType();
    }

    @Override
    public boolean isInTransaction() {
        return getGraphForCurrentTransaction().isInTransaction();
    }

    @Override
    public boolean promote(Promote txnType) {
        return getGraphForCurrentTransaction().promote(txnType);
    }

    @Override
    public void commit() {
        try {
            getGraphForCurrentTransaction().commit();
            if (!this.deltasToApplyToStaleGraph.isEmpty()) {
                if (getGraphForCurrentTransaction().getLengthOfDeltaChain() > MAX_DELTA_CHAIN_LENGTH) {
                    // if there are many consecutive write transactions, they might always be faster than the async call
                    // to update the stale graph. That way, they might block the update for a long time.
                    // In this case, we call the update synchronously to avoid this problem.
                    this.updateStaleGraphIfPossible();
                }
                forkJoinPool.execute(this::updateStaleGraphIfPossible);
            }
        } finally {
            txnGraph.remove();
        }
    }

    private void updateStaleGraphIfPossible() {
        if (!staleGraph.hasOpenReadTransactions()) {
            lockForUpdatingStaleGraph.lock();
            try {
                staleGraph.mergeDeltas();
                while (!deltasToApplyToStaleGraph.isEmpty()) {
                    final var delta = deltasToApplyToStaleGraph.peek();
                    staleGraph.executeDirectlyOnWrappedGraph(stale -> {
                        delta.getDeletions().forEachRemaining(stale::delete);
                        delta.getAdditions().forEachRemaining(stale::add);
                    });
                    deltasToApplyToStaleGraph.poll();
                }
            } catch (Exception exception) {
                LOGGER.error("Error while updating stale graph.", exception);
            } finally {
                lockForUpdatingStaleGraph.unlock();
            }
        } else {
            // While there are still open read transactions, we wait a bit and try again.
            // There are no new read transactions possible, so we will eventually succeed.
            // The only caller of this method is the commit method. This is the only place
            // where we add deltas to the queue and that here is work to do.
            if (delayedUpdateHasBeenScheduled.compareAndSet(false, true)) {
                CompletableFuture.delayedExecutor(
                                transactionCoordinator.getStaleTransactionRemovalTimerIntervalMs(),
                                java.util.concurrent.TimeUnit.MILLISECONDS, forkJoinPool)
                        .execute(this::delayedUpdateStaleGraphIfPossible);
            }
        }
    }

    private void delayedUpdateStaleGraphIfPossible() {
        delayedUpdateHasBeenScheduled.set(false);
        updateStaleGraphIfPossible();
    }

    @Override
    public void abort() {
        try {
            getGraphForCurrentTransaction().abort();
        } finally {
            txnGraph.remove();
        }
    }

    @Override
    public void end() {
        try {
            getGraphForCurrentTransaction().end();
        } finally {
            txnGraph.remove();
        }
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
        activeGraph.close();
        staleGraph.close();
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
        return activeGraph.isClosed();
    }
}

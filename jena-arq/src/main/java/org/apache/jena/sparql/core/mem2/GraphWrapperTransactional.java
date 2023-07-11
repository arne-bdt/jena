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
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class GraphWrapperTransactional implements Graph, Transactional {

    private static final int MAX_DELTA_CHAIN_LENGTH = 100;

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphWrapperTransactional.class);

    private final TransactionCoordinator transactionCoordinator;
    private final ConcurrentLinkedQueue<FastDeltaGraph> deltasToApplyToStaleGraph = new ConcurrentLinkedQueue<>();
    private final Semaphore semaphoreForUpdatingStaleGraph = new Semaphore(1);

    private final ReentrantLock lockForBeginTransaction = new java.util.concurrent.locks.ReentrantLock();
    private final ForkJoinPool forkJoinPool;
    private volatile GraphWrapperChainingDeltasTransactional staleGraph;
    private volatile GraphWrapperChainingDeltasTransactional activeGraph;

    private final AtomicBoolean delayedUpdateHasBeenScheduled = new AtomicBoolean(false);

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

    @Override
    public void begin(TxnType txnType) {
        switchStaleAndActiveIfNeededAndPossible();
        activeGraph.begin(txnType);
    }

    private void switchStaleAndActiveIfNeededAndPossible() {
        if (activeGraph.getLengthOfDeltaChain() > 0          // only if the active graph has at least one delta
                && !staleGraph.hasOpenReadTransactions()     // only if there are no open read transactions on the stale graph
                && !activeGraph.hasOpenWriteTransaction()) { // and there is no active write transaction
            try {
                semaphoreForUpdatingStaleGraph.acquire();            // wait for the stale graph to be updated
                // check again, because it could have changed in the meantime
                if (this.deltasToApplyToStaleGraph.isEmpty() //check if all deltas have been applied to the stale graph
                        && activeGraph.getLengthOfDeltaChain() > 0
                        && !staleGraph.hasOpenReadTransactions()
                        && !activeGraph.hasOpenWriteTransaction()) {
                    final var tmp = staleGraph;
                    staleGraph = activeGraph;
                    activeGraph = tmp;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                semaphoreForUpdatingStaleGraph.release();
            }
        }
    }

    @Override
    public ReadWrite transactionMode() {
        return activeGraph.transactionMode();
    }

    @Override
    public TxnType transactionType() {
        return activeGraph.transactionType();
    }

    @Override
    public boolean isInTransaction() {
        return activeGraph.isInTransaction();
    }

    @Override
    public boolean promote(Promote txnType) {
        return activeGraph.promote(txnType);
    }

    @Override
    public void commit() {
        activeGraph.commit();
        if (!this.deltasToApplyToStaleGraph.isEmpty()) {
            if (activeGraph.getLengthOfDeltaChain() > MAX_DELTA_CHAIN_LENGTH) {
                // if there are many consecutive write transactions, they might always be faster than the async call
                // to update the stale graph. That way, they might block the update for a long time.
                // In this case, we call the update synchronously to avoid this problem.
                this.updateStaleGraphIfPossible();
            }
            forkJoinPool.execute(this::updateStaleGraphIfPossible);
        }
    }

    private void updateStaleGraphIfPossible() {
        if (!staleGraph.hasOpenReadTransactions()) {
            try {
                semaphoreForUpdatingStaleGraph.acquire();
                while (!deltasToApplyToStaleGraph.isEmpty()) {
                    final var delta = deltasToApplyToStaleGraph.peek();
                    staleGraph.mergeDeltasAndExecuteDirectlyOnWrappedGraph(stale -> {
                        delta.getDeletions().forEachRemaining(stale::delete);
                        delta.getAdditions().forEachRemaining(stale::add);
                    });
                    deltasToApplyToStaleGraph.poll();
                }
            } catch (Throwable throwable) {
                LOGGER.error("Error while updating stale graph.", throwable);
            } finally {
                semaphoreForUpdatingStaleGraph.release();
            }
        } else if (!delayedUpdateHasBeenScheduled.get()) { // avoid scheduling multiple delayed updates
            // While there are still open read transactions, we wait a bit and try again.
            // There are no new read transactions possible, so we will eventually succeed.
            // The only caller of this method is the commit method. This is the only place
            // where we add deltas to the queue and that here is work to do.
            delayedUpdateHasBeenScheduled.set(true);
            CompletableFuture.delayedExecutor(
                            transactionCoordinator.getStaleTransactionRemovalTimerIntervalMs(),
                            java.util.concurrent.TimeUnit.MILLISECONDS, forkJoinPool)
                    .execute(this::delayedUpdateStaleGraphIfPossible);
        }
    }

    private void delayedUpdateStaleGraphIfPossible() {
        delayedUpdateHasBeenScheduled.set(false);
        updateStaleGraphIfPossible();
    }

    @Override
    public void abort() {
        activeGraph.abort();
    }

    @Override
    public void end() {
        activeGraph.end();
    }

    @Override
    public boolean dependsOn(Graph other) {
        return activeGraph.dependsOn(other);
    }

    @Override
    public TransactionHandler getTransactionHandler() {
        return null; // TODO
    }

    @Override
    public Capabilities getCapabilities() {
        return activeGraph.getCapabilities();
    }

    @Override
    public GraphEventManager getEventManager() {
        return activeGraph.getEventManager();
    }

    @Override
    public PrefixMapping getPrefixMapping() {
        return activeGraph.getPrefixMapping();
    }

    @Override
    public void add(Triple t) throws AddDeniedException {
        activeGraph.add(t);
    }

    @Override
    public void add(Node s, Node p, Node o) throws AddDeniedException {
        activeGraph.add(s, p, o);
    }

    @Override
    public void delete(Triple t) throws DeleteDeniedException {
        activeGraph.delete(t);
    }

    @Override
    public void delete(Node s, Node p, Node o) throws DeleteDeniedException {
        activeGraph.delete(s, p, o);
    }

    @Override
    public ExtendedIterator<Triple> find(Triple m) {
        return activeGraph.find(m);
    }

    @Override
    public ExtendedIterator<Triple> find(Node s, Node p, Node o) {
        return activeGraph.find(s, p, o);
    }

    @Override
    public Stream<Triple> stream(Node s, Node p, Node o) {
        return activeGraph.stream(s, p, o);
    }

    @Override
    public Stream<Triple> stream() {
        return activeGraph.stream();
    }

    @Override
    public ExtendedIterator<Triple> find() {
        return activeGraph.find();
    }

    @Override
    public boolean isIsomorphicWith(Graph g) {
        return activeGraph.isIsomorphicWith(g);
    }

    @Override
    public boolean contains(Node s, Node p, Node o) {
        return activeGraph.contains(s, p, o);
    }

    @Override
    public boolean contains(Triple t) {
        return activeGraph.contains(t);
    }

    @Override
    public void clear() {
        activeGraph.clear();
    }

    @Override
    public void remove(Node s, Node p, Node o) {
        activeGraph.remove(s, p, o);
    }

    @Override
    public void close() {
        activeGraph.close();
        staleGraph.close();
    }

    @Override
    public boolean isEmpty() {
        return activeGraph.isEmpty();
    }

    @Override
    public int size() {
        return activeGraph.size();
    }

    @Override
    public boolean isClosed() {
        return activeGraph.isClosed();
    }
}

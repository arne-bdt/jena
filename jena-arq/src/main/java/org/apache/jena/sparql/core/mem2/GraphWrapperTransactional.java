package org.apache.jena.sparql.core.mem2;

import org.apache.jena.graph.*;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class GraphWrapperTransactional implements Graph, Transactional {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphWrapperTransactional.class);

    final static TransactionCoordinator transactionCoordinator = null;
    private final ConcurrentLinkedQueue<FastDeltaGraph> deltasToApplyToStaleGraph = new ConcurrentLinkedQueue<>();
    private final ReentrantLock lockForUpdatingStaleGraph = new java.util.concurrent.locks.ReentrantLock();

    private final ReentrantLock lockForBeginTransaction = new java.util.concurrent.locks.ReentrantLock();
    private final Supplier<Graph> graphFactory;
    private volatile GraphWrapperChainingDeltasTransactional staleGraph;
    private volatile GraphWrapperChainingDeltasTransactional activeGraph;

    private final AtomicBoolean activeGraphHasAtLeastOneDelta = new AtomicBoolean(false);

    private final ForkJoinPool forkJoinPool;

    public GraphWrapperTransactional(final Supplier<Graph> graphFactory) {
        this(graphFactory, ForkJoinPool.commonPool());
    }

    public GraphWrapperTransactional(final Graph graphToWrap, final Supplier<Graph> graphFactory) {
        this(graphToWrap, graphFactory, ForkJoinPool.commonPool());
    }

    public GraphWrapperTransactional(final Supplier<Graph> graphFactory, final ForkJoinPool forkJoinPool) {
        this.graphFactory = graphFactory;
        this.staleGraph = new GraphWrapperChainingDeltasTransactional(graphFactory, transactionCoordinator,
                forkJoinPool, deltasToApplyToStaleGraph::add);
        this.activeGraph = new GraphWrapperChainingDeltasTransactional(graphFactory, transactionCoordinator,
                forkJoinPool, deltasToApplyToStaleGraph::add);
        this.forkJoinPool = forkJoinPool;
    }

    public GraphWrapperTransactional(final Graph graphToWrap, final Supplier<Graph> graphFactory,
                                     final ForkJoinPool forkJoinPool) {
        this.graphFactory = graphFactory;
        this.staleGraph = new GraphWrapperChainingDeltasTransactional(graphToWrap, graphFactory,
                transactionCoordinator, forkJoinPool, deltasToApplyToStaleGraph::add);
        this.activeGraph = new GraphWrapperChainingDeltasTransactional(graphToWrap, graphFactory,
                transactionCoordinator, forkJoinPool, deltasToApplyToStaleGraph::add);
        this.forkJoinPool = forkJoinPool;
    }

    @Override
    public void begin(TxnType txnType) {
        try {
            lockForBeginTransaction.lock();
            switchStaleAndActiveIfNeededAndPossible();
            activeGraph.begin(txnType);
        } finally {
            lockForBeginTransaction.unlock();
        }
    }

    private void switchStaleAndActiveIfNeededAndPossible() {
        if (activeGraphHasAtLeastOneDelta.get()          // only if the active graph has at least one delta
                && lockForUpdatingStaleGraph.tryLock()) {     // and the stale graph is not currently updated
            try {
                if (activeGraphHasAtLeastOneDelta.get() // check again, because it could have changed in the meantime
                        && !staleGraph.hasOpenReadTransactions() // only if there are no open read transactions on the stale graph
                        && !transactionCoordinator.hasActiveWriteTransaction()) { // and there is no active write transaction
                    final var tmp = staleGraph;
                    staleGraph = activeGraph;
                    activeGraph = tmp;
                    activeGraphHasAtLeastOneDelta.set(false);
                }
            } finally {
                lockForUpdatingStaleGraph.unlock();
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
            activeGraphHasAtLeastOneDelta.set(true);
            forkJoinPool.execute(this::updateStaleGraphIfPossible);
        }
    }

    private void updateStaleGraphIfPossible() {
        if (!deltasToApplyToStaleGraph.isEmpty()) {
            if (!staleGraph.hasOpenReadTransactions()) {
                lockForUpdatingStaleGraph.lock();
                try {
                    while (!deltasToApplyToStaleGraph.isEmpty()) {
                        final var delta = deltasToApplyToStaleGraph.peek();
                        staleGraph.executeDirectlyOnWrappedGraph(stale -> {
                            delta.getDeletions().forEachRemaining(stale::delete);
                            delta.getAdditions().forEachRemaining(stale::add);
                        });
                        deltasToApplyToStaleGraph.poll();
                    }
                } catch (Throwable throwable) {
                    LOGGER.error("Error while updating stale graph.", throwable);
                } finally {
                    lockForUpdatingStaleGraph.unlock();
                }
            } else {
                // While there are still open read transactions, we wait a bit and try again.
                // There are no new read transactions possible, so we will eventually succeed.
                // The only caller of this method is the commit method. This is the only place
                // where we add deltas to the queue and that here is work to do.
                CompletableFuture.delayedExecutor(
                                transactionCoordinator.getStaleTransactionRemovalTimerIntervalMs(),
                                java.util.concurrent.TimeUnit.MILLISECONDS, forkJoinPool)
                        .execute(this::updateStaleGraphIfPossible);
            }
        }
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

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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class GraphWrapperTransactional implements Graph, Transactional {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphWrapperTransactional.class);

    private static final int DEFAULT_TRANSACTION_TIMEOUT_MS = 30000;

    private static final int KEEP_INFO_ABOUT_TRANSACTION_TIMEOUT_FOR_X_TIMES_THE_TIMEOUT = 5;

    private static final int DEFAULT_STAlE_TRANSACTION_REMOVAL_SCHEDULER_INTERVAL_MS = 5000;

    /**
     * This lock is used to ensure that only one thread can write to the graph at a time.
     * A Semaphore is used instead of a ReentrantLock because it allows the lock to be released
     * in a different thread than the one that acquired it.
     */
    private final Semaphore writeSemaphore = new Semaphore(1);
    private final ReentrantLock lockForStaleGraph = new java.util.concurrent.locks.ReentrantLock();
    private final AtomicLong dataVersion = new AtomicLong(0);
    private final Supplier<Graph> graphFactory;
    private volatile Graph staleGraph;
    private volatile Graph activeGraph;
    private volatile GraphReadOnlyWrapper lastCommittedGraph;
    private volatile boolean activeGraphHasAtLeastOneDelta = false;
    private final ConcurrentLinkedQueue<FastDeltaGraph> deltasToApplyToStaleGraph = new ConcurrentLinkedQueue<>();
    private final ThreadLocal<Boolean> txnInTransaction = ThreadLocal.withInitial(() -> Boolean.FALSE);
    private final ThreadLocal<TxnType> txnType = new ThreadLocal();
    private final ThreadLocal<ReadWrite> txnMode = new ThreadLocal();
    private final ThreadLocal<Graph> txnGraph = new ThreadLocal<>();
    private final ThreadLocal<Long> txnReadTransactionVersion = new ThreadLocal<>();

    private final int transactionTimeoutMs;

    private final int staleTransactionRemovalSchedulerIntervalMs;

    private final ForkJoinPool forkJoinPool;

    public GraphWrapperTransactional(final Supplier<Graph> graphFactory) {
        this(graphFactory, DEFAULT_TRANSACTION_TIMEOUT_MS, ForkJoinPool.commonPool());
    }

    public GraphWrapperTransactional(final Graph graphToWrap, final Supplier<Graph> graphFactory) {
        this(graphToWrap, graphFactory, DEFAULT_TRANSACTION_TIMEOUT_MS, ForkJoinPool.commonPool());
    }

    public GraphWrapperTransactional(final Graph graphToWrap, final Supplier<Graph> graphFactory, final int transactionTimeoutMs, final ForkJoinPool forkJoinPool) {
        this(graphToWrap, graphFactory, transactionTimeoutMs, forkJoinPool, DEFAULT_STAlE_TRANSACTION_REMOVAL_SCHEDULER_INTERVAL_MS);
    }

    public GraphWrapperTransactional(final Supplier<Graph> graphFactory, final int transactionTimeoutMs, final ForkJoinPool forkJoinPool) {
        this(graphFactory, transactionTimeoutMs, forkJoinPool, DEFAULT_STAlE_TRANSACTION_REMOVAL_SCHEDULER_INTERVAL_MS);
    }

    public GraphWrapperTransactional(final Supplier<Graph> graphFactory, final int transactionTimeoutMs, final ForkJoinPool forkJoinPool, final int staleTransactionRemovalSchedulerIntervalMs) {
        this.graphFactory = graphFactory;
        this.staleGraph = graphFactory.get();
        this.activeGraph = graphFactory.get();
        this.lastCommittedGraph = new GraphReadOnlyWrapper(activeGraph);
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.staleTransactionRemovalSchedulerIntervalMs = staleTransactionRemovalSchedulerIntervalMs;
        this.forkJoinPool = forkJoinPool;
    }

    public GraphWrapperTransactional(final Graph graphToWrap, final Supplier<Graph> graphFactory, final int transactionTimeoutMs, final ForkJoinPool forkJoinPool, final int staleTransactionRemovalSchedulerIntervalMs) {
        this.graphFactory = graphFactory;
        this.staleGraph = graphToWrap;
        this.activeGraph = graphFactory.get();
        graphToWrap.find().forEachRemaining(this.activeGraph::add);
        this.lastCommittedGraph = new GraphReadOnlyWrapper(activeGraph);
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.staleTransactionRemovalSchedulerIntervalMs = staleTransactionRemovalSchedulerIntervalMs;
        this.forkJoinPool = forkJoinPool;
    }

    private Graph getGraphForCurrentTransaction() {
        final var txnGraph = this.txnGraph.get();
        if (txnGraph == null) {
            // TODO: Check if the transaction may have timed out.
            throw new JenaTransactionException("Not in a transaction");
        }
        return txnGraph;
    }

    @Override
    public void begin(TxnType txnType) {
        if (isInTransaction())
            new JenaTransactionException("Already in a transaction");
        ReadWrite readWrite = TxnType.convert(txnType);
        if (readWrite == ReadWrite.WRITE) {
            try {
                final var timeoutMs = transactionTimeoutMs + staleTransactionRemovalSchedulerIntervalMs;
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
                break;
            case WRITE:
                activeGraph = new FastDeltaGraph(activeGraph);
                txnGraph.set(activeGraph);
                break;
        }
    }

    private void switchStaleAndActiveIfNeededAndPossible() {
        if (activeGraphHasAtLeastOneDelta) {
            lockForStaleGraph.lock();
            try {
                if (activeGraphHasAtLeastOneDelta) {
                    final var tmp = staleGraph;
                    staleGraph = activeGraph;
                    activeGraph = tmp;
                    activeGraphHasAtLeastOneDelta = true;
                }
            } finally {
                lockForStaleGraph.unlock();
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
        // if we are promoting to isolated, we need to check that the data hasn't changed
        if (txnType == Promote.ISOLATED
                && txnReadTransactionVersion.get() != dataVersion.get()) {
            writeSemaphore.release();
            return false;
        }
        txnMode.set(ReadWrite.WRITE);
        activeGraph = new FastDeltaGraph(activeGraph);
        txnGraph.set(activeGraph);
        return true;
    }

    @Override
    public void commit() {
        if (isTransactionMode(ReadWrite.WRITE)) {
            final var delta = (FastDeltaGraph) activeGraph;
            if (delta.hasChanges()) {
                lastCommittedGraph = new GraphReadOnlyWrapper(activeGraph);
                dataVersion.getAndIncrement();
                deltasToApplyToStaleGraph.add(delta);
                activeGraphHasAtLeastOneDelta = true;
                forkJoinPool.execute(this::updateStaleGraph);
            } else {
                activeGraph = delta.getBase();
            }
        }
        endOnceByRemovingThreadLocalsAndUnlocking();
    }

    private void updateStaleGraph() {
        if (!deltasToApplyToStaleGraph.isEmpty()) {
            lockForStaleGraph.lock();
            try {
                while (!deltasToApplyToStaleGraph.isEmpty()) {
                    final var delta = deltasToApplyToStaleGraph.poll();
                    delta.getDeletions().forEachRemaining(staleGraph::delete);
                    delta.getAdditions().forEachRemaining(staleGraph::add);
                }
            } finally {
                lockForStaleGraph.unlock();
            }
        }
    }

    @Override
    public void abort() {
        if (isTransactionMode(ReadWrite.WRITE)) {
            activeGraph = ((FastDeltaGraph) activeGraph).getBase();
        }
        endOnceByRemovingThreadLocalsAndUnlocking();
    }

    @Override
    public void end() {
        if (isTransactionMode(ReadWrite.WRITE)) {
            abort();
            new JenaTransactionException("Write transaction - no commit or abort before end()");
        }
        endOnceByRemovingThreadLocalsAndUnlocking();
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
        getGraphForCurrentTransaction().close();
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
        return getGraphForCurrentTransaction().isClosed();
    }
}
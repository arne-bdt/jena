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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class GraphWrapperTransactional implements Graph, Transactional {

    private final ReentrantLock writeLock = new ReentrantLock();
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

    public GraphWrapperTransactional(final Supplier<Graph> graphFactory) {
        this.graphFactory = graphFactory;
        this.staleGraph = graphFactory.get();
        this.activeGraph = graphFactory.get();
        this.lastCommittedGraph = new GraphReadOnlyWrapper(activeGraph);
    }

    public GraphWrapperTransactional(final Graph graphToWrap, final Supplier<Graph> graphFactory) {
        this.graphFactory = graphFactory;
        this.staleGraph = graphToWrap;
        this.activeGraph = graphFactory.get();
        graphToWrap.find().forEachRemaining(this.activeGraph::add);
        this.lastCommittedGraph = new GraphReadOnlyWrapper(activeGraph);
    }

    @Override
    public void begin(TxnType txnType) {
        if (isInTransaction())
            new JenaTransactionException("Already in a transaction");
        ReadWrite readWrite = TxnType.convert(txnType);
        if (readWrite == ReadWrite.WRITE) {
            writeLock.lock();
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
        if (!writeLock.tryLock()) {
            return false;
        }
        // if we are promoting to isolated, we need to check that the data hasn't changed
        if (txnType == Promote.ISOLATED
                && txnReadTransactionVersion.get() != dataVersion.get()) {
            writeLock.unlock();
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
                new Thread(this::updateStaleGraph).start();
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
            txnGraph.remove();
            txnMode.remove();
            txnType.remove();
            txnInTransaction.remove();
            txnReadTransactionVersion.remove();
            if (writeLock.isHeldByCurrentThread()) {
                writeLock.unlock();
            }
        }
    }


    @Override
    public boolean dependsOn(Graph other) {
        return txnGraph.get().dependsOn(other);
    }

    @Override
    public TransactionHandler getTransactionHandler() {
        return null; // TODO
    }

    @Override
    public Capabilities getCapabilities() {
        return txnGraph.get().getCapabilities();
    }

    @Override
    public GraphEventManager getEventManager() {
        return txnGraph.get().getEventManager();
    }

    @Override
    public PrefixMapping getPrefixMapping() {
        return txnGraph.get().getPrefixMapping();
    }

    @Override
    public void add(Triple t) throws AddDeniedException {
        txnGraph.get().add(t);
    }

    @Override
    public void add(Node s, Node p, Node o) throws AddDeniedException {
        txnGraph.get().add(s, p, o);
    }

    @Override
    public void delete(Triple t) throws DeleteDeniedException {
        txnGraph.get().delete(t);
    }

    @Override
    public void delete(Node s, Node p, Node o) throws DeleteDeniedException {
        txnGraph.get().delete(s, p, o);
    }

    @Override
    public ExtendedIterator<Triple> find(Triple m) {
        return txnGraph.get().find(m);
    }

    @Override
    public ExtendedIterator<Triple> find(Node s, Node p, Node o) {
        return txnGraph.get().find(s, p, o);
    }

    @Override
    public Stream<Triple> stream(Node s, Node p, Node o) {
        return txnGraph.get().stream(s, p, o);
    }

    @Override
    public Stream<Triple> stream() {
        return txnGraph.get().stream();
    }

    @Override
    public ExtendedIterator<Triple> find() {
        return txnGraph.get().find();
    }

    @Override
    public boolean isIsomorphicWith(Graph g) {
        return txnGraph.get().isIsomorphicWith(g);
    }

    @Override
    public boolean contains(Node s, Node p, Node o) {
        return txnGraph.get().contains(s, p, o);
    }

    @Override
    public boolean contains(Triple t) {
        return txnGraph.get().contains(t);
    }

    @Override
    public void clear() {
        txnGraph.get().clear();
    }

    @Override
    public void remove(Node s, Node p, Node o) {
        txnGraph.get().remove(s, p, o);
    }

    @Override
    public void close() {
        txnGraph.get().close();
    }

    @Override
    public boolean isEmpty() {
        return txnGraph.get().isEmpty();
    }

    @Override
    public int size() {
        return txnGraph.get().size();
    }

    @Override
    public boolean isClosed() {
        return txnGraph.get().isClosed();
    }
}

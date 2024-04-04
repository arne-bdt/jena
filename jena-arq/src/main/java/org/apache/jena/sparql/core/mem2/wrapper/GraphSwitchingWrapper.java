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

package org.apache.jena.sparql.core.mem2.wrapper;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.graph.*;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.shared.AddDeniedException;
import org.apache.jena.shared.DeleteDeniedException;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * This class is used to wrap a graph and make it read-only.
 */
public class GraphSwitchingWrapper implements Graph, Transactional {

    public static final String NOT_IN_A_TRANSACTION = "Not in a transaction.";

    private record TransactionInfo(TxnType type, Graph graph, UUID graphId) {}

    private final ThreadLocal<TransactionInfo> threadLocalTxnInfo = new ThreadLocal<>();

    private final Object syncWrappedGraph = new Object();
    private Graph wrappedGraph;
    private UUID wrappedGraphId;

    private final Supplier<Graph> graphFactory;

    private ConcurrentMap<UUID, Pair<AtomicInteger, Graph>> graphsInUse = new ConcurrentHashMap<>();

    public GraphSwitchingWrapper(Supplier<Graph> graphFactory) {
        this.graphFactory = graphFactory;
        this.wrappedGraph = graphFactory.get();
        this.wrappedGraphId = UUID.randomUUID();
        this.graphsInUse.put(wrappedGraphId, Pair.of(new AtomicInteger(0), wrappedGraph));
    }

    public Graph createNewGraphUsingFactory() {
        return graphFactory.get();
    }

    public void switchWrappedGraph(Graph newGraph) {
        if(!wrappedGraph.getClass().equals(newGraph.getClass())) {
            throw new IllegalArgumentException("The new graph must be of the same type as the old one.");
        }
        synchronized (syncWrappedGraph) {
            if(graphsInUse.get(wrappedGraphId).getKey().get() == 0) {
                graphsInUse.remove(wrappedGraphId);
                wrappedGraph.close();
            }
            wrappedGraph = newGraph;
            wrappedGraphId = UUID.randomUUID();
            graphsInUse.put(wrappedGraphId, Pair.of(new AtomicInteger(0), wrappedGraph));
        }
    }

    private Graph getGraphForCurrentTransaction() {
        final var transactionInfo = threadLocalTxnInfo.get();
        if (transactionInfo == null) {
            throw new JenaTransactionException(NOT_IN_A_TRANSACTION);
        }
        return transactionInfo.graph;
    }


    @SuppressWarnings("java:S2142")
    @Override
    public void begin(TxnType txnType) {
        final TransactionInfo ti;
        synchronized (syncWrappedGraph) {
            ti = new TransactionInfo(txnType, wrappedGraph, wrappedGraphId);
            graphsInUse.get(wrappedGraphId).getKey().incrementAndGet();
        }
        threadLocalTxnInfo.set(ti);
        if (ti.graph instanceof Transactional transactional) {
            try {
                transactional.begin(txnType);
            } catch (Exception e) {
                threadLocalTxnInfo.remove();
                removeGraphFromUse(ti.graphId);
                throw e;
            }
        }
    }

    private void removeGraphFromUse(UUID graphId) {
        synchronized (syncWrappedGraph) {
            var graphInUse = graphsInUse.get(graphId);
            var usages = graphInUse.getKey().decrementAndGet();
            if(!wrappedGraphId.equals(graphId)  //if the currently wrapped graph is not the one that is being removed
                    && usages < 1) {            //and there are no more usages of the graph
                graphsInUse.remove(graphId);
                graphInUse.getValue().close();
            }
        }
    }

    @Override
    public ReadWrite transactionMode() {
        return TxnType.convert(transactionType());
    }

    @Override
    public TxnType transactionType() {
        final var txnInfo = this.threadLocalTxnInfo.get();
        if (txnInfo == null) {
            return null;
        }
        return txnInfo.type;
    }

    @Override
    public boolean isInTransaction() {
        return threadLocalTxnInfo.get() != null;
    }

    @Override
    public boolean promote(Promote txnType) {
        final var txnInfo = this.threadLocalTxnInfo.get();
        if (txnInfo == null) {
            throw new JenaTransactionException(NOT_IN_A_TRANSACTION);
        }
        if (txnInfo.graph instanceof Transactional transactional) {
            return transactional.promote(txnType);
        }
        return false;
    }

    @Override
    public void commit() {
        final var txnInfo = this.threadLocalTxnInfo.get();
        if (txnInfo == null) {
            throw new JenaTransactionException(NOT_IN_A_TRANSACTION);
        }
        try
        {
            if (txnInfo.graph instanceof Transactional transactional) {
                transactional.commit();
            }
        } finally {
            this.threadLocalTxnInfo.remove();
            removeGraphFromUse(txnInfo.graphId);
        }
    }

    @Override
    public void abort() {
        final var txnInfo = this.threadLocalTxnInfo.get();
        if (txnInfo == null) {
            return;
        }
        try
        {
            if (txnInfo.graph instanceof Transactional transactional) {
                transactional.abort();
            }
        } finally {
            this.threadLocalTxnInfo.remove();
            if(txnInfo != null) {
                removeGraphFromUse(txnInfo.graphId);
            }
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
        var exceptions = new ArrayList<Exception>();
        graphsInUse.values()
                .forEach(p -> {
                    try {
                        p.getValue().close();
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                });
        if (!exceptions.isEmpty()) {
            final var e = new RuntimeException("Exceptions occurred while closing graphs.");
            exceptions.forEach(e::addSuppressed);
            throw e;
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
        final var txnInfo = this.threadLocalTxnInfo.get();
        if (txnInfo != null) {
            return txnInfo.graph.isClosed();
        }
        return true;
    }
}

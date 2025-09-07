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

package org.apache.jena.cimxml.sparql.core;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapStd;
import org.apache.jena.sparql.core.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A {@link DatasetGraph} that holds a set of named graphs and an optional default graph.
 * <p>
 * This implementation provides "best effort" transactions; it only provides MRPlusSW locking.
 * So all graphs that are transactional must support MRPlusSW locking.
 * <p>
 * Only the transactional graphs are included in the transaction.
 */
public class LinkedCimDatasetGraph extends DatasetGraphCollection implements CimDatasetGraph {

    public LinkedCimDatasetGraph() {
        super();
    }

    public LinkedCimDatasetGraph(Graph defaultGraph) {
        this();
        addGraph(Quad.defaultGraphIRI, defaultGraph);
    }

    public static class LinkedDatasetTransactionException extends RuntimeException {

        private final Collection<Exception> exceptions;

        public Collection<Exception> getExceptions() {
            return exceptions;
        }

        public LinkedDatasetTransactionException(String message, Collection<Exception> exceptions) {
            super(message);
            this.exceptions = exceptions;
        }
    }

    private final ConcurrentMap<Node, Graph> graphs = new ConcurrentHashMap<>();
    private final ConcurrentMap<Node, Transactional> transactionalGraphs = new ConcurrentHashMap<>();

    private final TransactionalLock txn = TransactionalLock.createMRPlusSW();

    public Collection<Graph> getGraphs() {
        return graphs.values();
    }

    @Override
    public Iterator<Node> listGraphNodes() {
        return graphs.keySet().iterator();
    }

    protected final PrefixMap prefixes = new PrefixMapStd();

    @Override
    public PrefixMap prefixes() {
        return prefixes;
    }

    @Override
    public boolean supportsTransactions() {
        return true;
    }

    @Override
    public Graph getDefaultGraph() {
        return graphs.getOrDefault(Quad.defaultGraphIRI, Graph.emptyGraph);
    }

    @Override
    public Graph getGraph(Node graphNode) {
        return graphs.get(graphNode);
    }

    @Override
    public void addGraph(Node graphName, Graph graph) {
        graphs.put(graphName, graph);
        if(graph instanceof Transactional transactional) {
            transactionalGraphs.put(graphName, transactional);
        }
    }

    @Override
    public void removeGraph(Node graphName) {
        graphs.remove(graphName);
        transactionalGraphs.remove(graphName);
    }

    @Override
    public void begin(TxnType type) {
        txn.begin(type);
        var openedTransactions = new ArrayList<Transactional>();
        try {
            for (Transactional transactional : transactionalGraphs.values()) {
                transactional.begin(type);
                openedTransactions.add(transactional);
            }
        } catch (Exception e) {
            txn.abort();
            for (Transactional transactional : openedTransactions) {
                transactional.abort();
            }
            throw e;
        }
    }

    @Override
    public boolean promote(Promote mode) {
        return false;
    }

    @Override
    public void commit() {
        txn.commit();
        var failedCommits = new ArrayList<Exception>();
        for (Transactional transactional : transactionalGraphs.values()) {
            try {
                transactional.commit();
            } catch (Exception e) {
                failedCommits.add(e);
            }
        }
        if (!failedCommits.isEmpty()) {
            //Exception with message "Failed to commit transactions on x graphs"
            throw new LinkedDatasetTransactionException("Failed to commit transactions on " + failedCommits.size() + " graphs", failedCommits);
        }
    }

    @Override
    public void abort() {
        txn.abort();
        var failedAborts = new ArrayList<Exception>();
        for (Transactional transactional : transactionalGraphs.values()) {
            try {
                transactional.abort();
            } catch (Exception e) {
                failedAborts.add(e);
            }
        }
        if (!failedAborts.isEmpty()) {
            //Exception with message "Failed to abort transactions on x graphs"
            throw new LinkedDatasetTransactionException("Failed to abort transactions on " + failedAborts.size() + " graphs", failedAborts);
        }
    }

    @Override
    public void end() {
        txn.end();
        var failedEnds = new ArrayList<Exception>();
        for (Transactional transactional : transactionalGraphs.values()) {
            try {
                transactional.end();
            } catch (Exception e) {
                failedEnds.add(e);
            }
        }
        if (!failedEnds.isEmpty()) {
            //Exception with message "Failed to end transaction on x graphs"
            throw new LinkedDatasetTransactionException("Failed to end transaction on " + failedEnds.size() + " graphs", failedEnds);
        }
    }

    @Override
    public ReadWrite transactionMode() {
        return txn.transactionMode();
    }

    @Override
    public TxnType transactionType() {
        return txn.transactionType();
    }

    @Override
    public boolean isInTransaction() {
        return txn.isInTransaction();
    }

}

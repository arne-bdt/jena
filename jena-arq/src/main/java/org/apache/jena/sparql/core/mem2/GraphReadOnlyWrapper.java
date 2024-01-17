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
import org.apache.jena.graph.impl.SimpleTransactionHandler;
import org.apache.jena.shared.AddDeniedException;
import org.apache.jena.shared.DeleteDeniedException;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.stream.Stream;

/**
 * This class is used to wrap a graph and make it read-only.
 */
public class GraphReadOnlyWrapper implements Graph {

    private static final String ERROR_MSG_GRAPH_IS_READ_ONLY = "Graph is read-only";

    private final Graph wrappedGraph;
    private TransactionHandler transactionHandler = null;
    private Capabilities capabilities = null;

    public GraphReadOnlyWrapper(Graph graph) {
        this.wrappedGraph = graph;
    }

    @Override
    public boolean dependsOn(Graph other) {
        return wrappedGraph.dependsOn(other);
    }

    @Override
    public TransactionHandler getTransactionHandler() {
        if (transactionHandler == null)
            transactionHandler = new SimpleTransactionHandler();

        return transactionHandler;
    }

    @Override
    public Capabilities getCapabilities() {
        if (capabilities == null) {
            capabilities = new Capabilities() {
                @Override
                public boolean sizeAccurate() {
                    return wrappedGraph.getCapabilities().sizeAccurate();
                }

                @Override
                public boolean addAllowed() {
                    return false;
                }

                @Override
                public boolean deleteAllowed() {
                    return false;
                }

                @Override
                public boolean handlesLiteralTyping() {
                    return wrappedGraph.getCapabilities().handlesLiteralTyping();
                }
            };
        }
        return capabilities;
    }

    /**
     * Read-only graphs do not have event managers.
     */
    @Override
    public GraphEventManager getEventManager() {
        return null;
    }

    @Override
    public PrefixMapping getPrefixMapping() {
        return wrappedGraph.getPrefixMapping();
    }

    @Override
    public void add(Triple t) throws AddDeniedException {
        throw new AddDeniedException(ERROR_MSG_GRAPH_IS_READ_ONLY);
    }

    @Override
    public void add(Node s, Node p, Node o) throws AddDeniedException {
        throw new AddDeniedException(ERROR_MSG_GRAPH_IS_READ_ONLY);
    }

    @Override
    public void delete(Triple t) throws DeleteDeniedException {
        throw new DeleteDeniedException(ERROR_MSG_GRAPH_IS_READ_ONLY);
    }

    @Override
    public void delete(Node s, Node p, Node o) throws DeleteDeniedException {
        throw new DeleteDeniedException(ERROR_MSG_GRAPH_IS_READ_ONLY);
    }

    @Override
    public ExtendedIterator<Triple> find(Triple m) {
        return wrappedGraph.find(m);
    }

    @Override
    public ExtendedIterator<Triple> find(Node s, Node p, Node o) {
        return wrappedGraph.find(s, p, o);
    }

    @Override
    public Stream<Triple> stream(Node s, Node p, Node o) {
        return wrappedGraph.stream(s, p, o);
    }

    @Override
    public Stream<Triple> stream() {
        return wrappedGraph.stream();
    }

    @Override
    public ExtendedIterator<Triple> find() {
        return wrappedGraph.find();
    }

    @Override
    public boolean isIsomorphicWith(Graph g) {
        return wrappedGraph.isIsomorphicWith(g);
    }

    @Override
    public boolean contains(Node s, Node p, Node o) {
        return wrappedGraph.contains(s, p, o);
    }

    @Override
    public boolean contains(Triple t) {
        return wrappedGraph.contains(t);
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException(ERROR_MSG_GRAPH_IS_READ_ONLY);
    }

    @Override
    public void remove(Node s, Node p, Node o) {
        throw new UnsupportedOperationException(ERROR_MSG_GRAPH_IS_READ_ONLY);
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public boolean isEmpty() {
        return wrappedGraph.isEmpty();
    }

    @Override
    public int size() {
        return wrappedGraph.size();
    }

    @Override
    public boolean isClosed() {
        return wrappedGraph.isClosed();
    }
}

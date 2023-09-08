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

import org.apache.jena.graph.Graph;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

public class GraphDeltaTransactionManagerImpl implements GraphDeltaTransactionManager {

    private Graph lastCommittedGraph;

    private FastDeltaGraph deltaGraphOfCurrentTransaction = null;

    private int deltaChainLength = 0;

    private final ConcurrentLinkedQueue<Graph> readers = new ConcurrentLinkedQueue<>();

    public GraphDeltaTransactionManagerImpl(final Supplier<Graph> graphFactory) {
        lastCommittedGraph = graphFactory.get();
    }

    public GraphDeltaTransactionManagerImpl(final Supplier<Graph> graphFactory, final Graph graphToWrap) {
        lastCommittedGraph = graphFactory.get();
        graphToWrap.find().forEachRemaining(lastCommittedGraph::add);
    }

    private static Graph mergeDeltas(Graph graph) {
        if (graph instanceof FastDeltaGraph delta) {
            var base = mergeDeltas(delta.getBase());
            delta.getDeletions().forEachRemaining(base::delete);
            delta.getAdditions().forEachRemaining(base::add);
            return base;
        } else {
            return graph;
        }
    }

    @Override
    public boolean isTransactionOpen() {
        return deltaGraphOfCurrentTransaction != null;
    }

    @Override
    public boolean hasUnmergedDeltas() {
        return deltaChainLength != 0;
    }

    @Override
    public boolean hasReader() {
        return !readers.isEmpty();
    }

    @Override
    public boolean isReadyToMerge() {
        return !hasReader() && !isTransactionOpen();
    }

    @Override
    public boolean isReadyToApplyDeltas() {
        return !hasUnmergedDeltas() && !hasReader() && !isTransactionOpen();
    }

    @Override
    public Graph getLastCommittedGraphToRead() {
        readers.add(lastCommittedGraph);
        return lastCommittedGraph;
    }

    @Override
    public void releaseGraphFromRead(Graph graph) {
        readers.remove(graph);
    }

    @Override
    public FastDeltaGraph beginTransaction() {
        if (isTransactionOpen())
            throw new IllegalStateException("There is already a transaction in progress");
        deltaGraphOfCurrentTransaction = new FastDeltaGraph(lastCommittedGraph);
        return deltaGraphOfCurrentTransaction;
    }

    @Override
    public void commit() {
        if (!isTransactionOpen())
            throw new IllegalStateException("There is no transaction in progress");
        if (deltaGraphOfCurrentTransaction.hasChanges()) {
            lastCommittedGraph = deltaGraphOfCurrentTransaction;
            deltaChainLength++;
        }
        deltaGraphOfCurrentTransaction = null;
    }

    @Override
    public void rollback() {
        if (!isTransactionOpen())
            throw new IllegalStateException("There is no transaction in progress");
        deltaGraphOfCurrentTransaction = null;
    }

    @Override
    public int getDeltaChainLength() {
        return deltaChainLength;
    }

    @Override
    public void mergeDeltaChain() {
        if (!this.isReadyToMerge())
            throw new IllegalStateException("Not ready to merge");

        mergeDeltas(lastCommittedGraph);
        deltaChainLength = 0;
    }

    @Override
    public void applyDeltas(Queue<FastDeltaGraph> deltas) {
        if (!isReadyToApplyDeltas())
            throw new IllegalStateException("Not ready to apply deltas");

        while (!deltas.isEmpty()) {
            var delta = deltas.poll();
            delta.getDeletions().forEachRemaining(lastCommittedGraph::delete);
            delta.getAdditions().forEachRemaining(lastCommittedGraph::add);
        }
    }
}

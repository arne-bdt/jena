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
import org.apache.jena.sparql.core.mem2.wrapper.GraphReadOnlyWrapper;

import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * This class is used to manage a chain of graphs.
 * It has a last committed graph, which can itself be a delta graph.
 * <p>
 * New write transactions are based on the last committed graph
 * and are not linked to the chain until they are committed.
 * The graph for writing is the delta graph of the write transaction.
 * </p>
 * <p>
 * New deltas are queued and later actively applied to the last committed graph.
 * </p>
 * <p>
 * Merging of the delta chain is done by recursively merging all deltas into the base graph of the last committed graph.
 * </p>
 */
public class GraphChainImpl implements GraphChain {

    private final ConcurrentLinkedQueue<FastDeltaGraph> deltasToApply = new ConcurrentLinkedQueue<>();

    /**
     * Queues a delta to be applied to the last committed graph.
     * @param deltaGraph the delta to apply
     */
    public void queueDelta(FastDeltaGraph deltaGraph) {
        deltasToApply.add(deltaGraph);
    }

    private Graph lastCommittedGraph;

    private FastDeltaGraph deltaGraphOfWriteTransaction = null;

    private final AtomicInteger deltaChainLength = new AtomicInteger(0);

    private final ConcurrentSkipListSet<UUID> reader = new ConcurrentSkipListSet<>();

    public String getInstanceId() {
        return instanceId;
    }

    private final String instanceId;

    private final AtomicLong dataVersion = new AtomicLong(0);

    private final Supplier<Graph> graphFactory;

    public long getDataVersion() {
        return dataVersion.get();
    }

    public GraphChainImpl(final Graph base, final Supplier<Graph> graphFactory) {
        lastCommittedGraph = base;
        this.graphFactory = graphFactory;
        this.instanceId = UUID.randomUUID().toString();
    }

    public GraphChainImpl(final Supplier<Graph> graphFactory) {
        this(graphFactory.get(), graphFactory);
    }

    private static Graph mergeDeltas(Graph graph) {
        if (graph instanceof FastDeltaGraph delta) {
            final var base = mergeDeltas(delta.getBase());
            // first add, then delete --> this may use more memory but should be much faster, as it avoids unnecessary
            // destruction and recreation of the internal data structures
            delta.getAdditions().forEachRemaining(base::add);
            delta.getDeletions().forEachRemaining(base::delete);
            return base;
        } else {
            return graph;
        }
    }

    @Override
    public boolean hasGraphForWriting() {
        return deltaGraphOfWriteTransaction != null;
    }

    @Override
    public boolean hasNoUnmergedDeltas() {
        return deltaChainLength.get() == 0;
    }

    @Override
    public boolean hasNoReader() {
        return reader.isEmpty();
    }

    @Override
    public boolean isReadyToMerge() {
        return hasNoReader() && !hasGraphForWriting();
    }

    @Override
    public boolean isReadyToApplyDeltas() {
        return hasNoUnmergedDeltas() && hasNoReader() && !hasGraphForWriting();
    }

    @Override
    public GraphReadOnlyWrapper getLastCommittedAndAddReader(final UUID readerId) {
        if(!reader.add(readerId))
            throw new IllegalStateException("Reader already exists");
        return new GraphReadOnlyWrapper(lastCommittedGraph);
    }

    @Override
    public void removeReader(final UUID readerId) {
        reader.remove(readerId); /*this may occur more than once*/
    }

    /**
     * Creates a new delta graph for writing.
     * The last committed graph is used as base graph.
     * The delta graph is not linked to the chain.
     * @return the delta graph
     */
    @Override
    public FastDeltaGraph prepareGraphForWriting() {
        if (hasGraphForWriting())
            throw new IllegalStateException("There is already a transaction in progress");
        deltaGraphOfWriteTransaction = new FastDeltaGraph(lastCommittedGraph, graphFactory);
        return deltaGraphOfWriteTransaction;
    }

    /**
     * Links the delta graph for writing to the chain.
     */
    @Override
    public void linkGraphForWritingToChain() {
        if (!hasGraphForWriting())
            throw new IllegalStateException("There is no transaction in progress");
        lastCommittedGraph = deltaGraphOfWriteTransaction;
        deltaChainLength.incrementAndGet();
        deltaGraphOfWriteTransaction = null;
        dataVersion.getAndIncrement();
    }

    @Override
    public void rebaseAndLinkDeltaForWritingToChain(FastDeltaGraph deltaGraph) {
        lastCommittedGraph = new FastDeltaGraph(lastCommittedGraph, deltaGraph);
        deltaChainLength.incrementAndGet();
        deltaGraphOfWriteTransaction = null;
        dataVersion.getAndIncrement();
    }

    @Override
    public void discardGraphForWriting() {
        deltaGraphOfWriteTransaction = null;
    }

    @Override
    public int getDeltaChainLength() {
        return deltaChainLength.get();
    }

    @Override
    public int getDeltaQueueLength() {
        return deltasToApply.size();
    }

    /**
     * If the last committed graph is a delta graph, this method will merge all deltas into the base graph.
     */
    @Override
    public void mergeDeltaChain() {
        if (!this.isReadyToMerge())
            throw new IllegalStateException("Not ready to merge");

        if(deltaChainLength.get() > 0) {
            lastCommittedGraph = mergeDeltas(lastCommittedGraph);
            deltaChainLength.set(0);
        }
    }

    /**
     * Applies all queued deltas to the last committed graph.
     * The additions and deletions are executed as Graph#add and Graph#delete on the last committed graph.
     */
    @Override
    public void applyQueuedDeltas() {
        if (!isReadyToApplyDeltas())
            throw new IllegalStateException("Not ready to apply deltas");

        while (!deltasToApply.isEmpty()) {
            var delta = deltasToApply.poll();
            // first add, then delete --> this may use more memory but should be much faster, as it avoids unnecessary
            // destruction and recreation of the internal data structures
            delta.getAdditions().forEachRemaining(lastCommittedGraph::add);
            delta.getDeletions().forEachRemaining(lastCommittedGraph::delete);
            dataVersion.getAndIncrement();
        }
    }
}

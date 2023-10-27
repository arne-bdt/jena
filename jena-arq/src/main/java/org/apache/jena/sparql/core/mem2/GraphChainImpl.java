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

import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class GraphChainImpl implements GraphChain {

    private final ConcurrentLinkedQueue<FastDeltaGraph> deltasToApply = new ConcurrentLinkedQueue<>();

    public void queueDelta(FastDeltaGraph deltaGraph) {
        deltasToApply.add(deltaGraph);
    }

    private volatile Graph lastCommittedGraph;

    private volatile FastDeltaGraph deltaGraphOfCurrentTransaction = null;

    private volatile int deltaChainLength = 0;

    private final AtomicInteger readerCounter = new AtomicInteger(0);

    public String getInstanceId() {
        return instanceId;
    }

    private final String instanceId;

    public GraphChainImpl(final Graph base) {
        lastCommittedGraph = base;
        this.instanceId = UUID.randomUUID().toString();
    }

    private static Graph mergeDeltas(Graph graph) {
        if (graph instanceof FastDeltaGraph delta) {
            var base = mergeDeltas(delta.getBase());
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
        return deltaGraphOfCurrentTransaction != null;
    }

    @Override
    public boolean hasUnmergedDeltas() {
        return deltaChainLength != 0;
    }

    @Override
    public boolean hasReader() {
        return readerCounter.get() != 0;
    }

    @Override
    public boolean isReadyToMerge() {
        return !hasReader() && !hasGraphForWriting();
    }

    @Override
    public boolean isReadyToApplyDeltas() {
        return !hasUnmergedDeltas() && !hasReader() && !hasGraphForWriting();
    }

    @Override
    public GraphReadOnlyWrapper getLastCommittedAndIncReaderCounter() {
        readerCounter.incrementAndGet();
        return new GraphReadOnlyWrapper(lastCommittedGraph);
    }

    @Override
    public void decrementReaderCounter() {
        if (readerCounter.decrementAndGet() < 0)
            throw new IllegalStateException("Reader counter is negative");
    }

    @Override
    public FastDeltaGraph prepareGraphForWriting() {
        if (hasGraphForWriting())
            throw new IllegalStateException("There is already a transaction in progress");
        deltaGraphOfCurrentTransaction = new FastDeltaGraph(lastCommittedGraph);
        return deltaGraphOfCurrentTransaction;
    }

    @Override
    public void linkGraphForWritingToChain() {
        if (!hasGraphForWriting())
            throw new IllegalStateException("There is no transaction in progress");
        lastCommittedGraph = deltaGraphOfCurrentTransaction;
        deltaChainLength++;
        deltaGraphOfCurrentTransaction = null;
    }

    @Override
    public void discardGraphForWriting() {
        if (!hasGraphForWriting())
            throw new IllegalStateException("There is no transaction in progress");
        deltaGraphOfCurrentTransaction = null;
    }

    @Override
    public int getDeltaChainLength() {
        return deltaChainLength;
    }

    @Override
    public int getDeltaQueueLength() {
        return deltasToApply.size();
    }

    @Override
    public void mergeDeltaChain() {
        if (!this.isReadyToMerge())
            throw new IllegalStateException("Not ready to merge");

        if(deltaChainLength > 0) {
            lastCommittedGraph = mergeDeltas(lastCommittedGraph);
            deltaChainLength = 0;
        }
//        //println: Instance #: Merged delta chain.
//        System.out.println("Instance " + instanceId + ": Merged delta chain.");
    }

    @Override
    public void applyQueuedDeltas() {
        if (!isReadyToApplyDeltas())
            throw new IllegalStateException("Not ready to apply deltas");

        final var deltasSize = deltasToApply.size();
        while (!deltasToApply.isEmpty()) {
            var delta = deltasToApply.poll();
            // first add, then delete --> this may use more memory but should be much faster, as it avoids unnecessary
            // destruction and recreation of the internal data structures
            delta.getAdditions().forEachRemaining(lastCommittedGraph::add);
            delta.getDeletions().forEachRemaining(lastCommittedGraph::delete);
        }
//        if(deltasSize > 0) {
//            //println: Instance #: Applied deltas.
//            System.out.println("Instance " + instanceId + ": Applied " + deltasSize + " deltas.");
//        }
    }
}

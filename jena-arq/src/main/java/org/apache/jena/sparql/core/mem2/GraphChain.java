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

import org.apache.jena.sparql.core.mem2.wrapper.GraphReadOnlyWrapper;

import java.util.UUID;

/**
 * This interface is used to manage a chain of graphs.
 * It has a last committed graph, which can itself be a delta graph.
 * <p>
*  New write transactions are based on the last committed graph
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
public interface GraphChain {

    long getDataVersion();

    String getInstanceId();

    boolean hasGraphForWriting();

    boolean hasNoUnmergedDeltas();

    boolean hasNoReader();

    default boolean hasNothingToMergeAndNoDeltasToApply() {
        return hasNoUnmergedDeltas() && getDeltaQueueLength() == 0;
    }

    boolean isReadyToMerge();

    boolean isReadyToApplyDeltas();

    GraphReadOnlyWrapper getLastCommittedAndAddReader(UUID readerId);

    void removeReader(final UUID readerId);

    DeltaGraph prepareGraphForWriting();

    void linkGraphForWritingToChain();

    void rebaseAndLinkDeltaForWritingToChain(DeltaGraph deltaGraph);

    void discardGraphForWriting();

    int getDeltaChainLength();

    int getDeltaQueueLength();

    void mergeDeltaChain();

    void applyQueuedDeltas();

    void queueDelta(DeltaGraph deltaGraph);

    default void mergeAndApplyDeltas() {
        mergeDeltaChain();
        applyQueuedDeltas();
    }
}

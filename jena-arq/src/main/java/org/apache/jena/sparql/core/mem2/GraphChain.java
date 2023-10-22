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

import java.util.Queue;

public interface GraphChain {

    String getInstanceId();

    boolean hasGraphForWriting();

    boolean hasUnmergedDeltas();

    boolean hasReader();

    boolean isReadyToMerge();

    boolean isReadyToApplyDeltas();

    GraphReadOnlyWrapper getLastCommittedAndIncReaderCounter();

    void decrementReaderCounter();

    FastDeltaGraph prepareGraphForWriting();

    void linkGraphForWritingToChain();

    void discardGraphForWriting();

    int getDeltaChainLength();

    void mergeDeltaChain();

    void applyDeltas(Queue<FastDeltaGraph> deltas);
}

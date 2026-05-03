/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *   SPDX-License-Identifier: Apache-2.0
 */

package org.apache.jena.sparql.core.mem;

import java.util.stream.Stream;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.IndexingStrategy;
import org.apache.jena.mem2.store.TripleStore;
import org.apache.jena.mem2.store.roaring.RoaringTripleStore;
import org.apache.jena.util.iterator.ExtendedIterator;

/**
 * An immutable, read-only view of a transactional indexed-set graph.
 * Snapshots are the unit of isolation served to readers: a reader holds
 * a single {@code Snapshot} reference for the whole life of its
 * transaction and the JVM keeps the underlying store alive via that
 * reachability chain.
 * <p>
 * Phase A backs each snapshot with an independent {@link TripleStore}.
 * Snapshots are produced either by {@link #empty(IndexingStrategy)} (at
 * graph construction), by {@link GraphMemIndexedSetTxnWorkingCopy#seal()}
 * (after a successful write commit), or by {@link #copyOf(GraphMemIndexedSetTxnSnapshot)}
 * for graph-level {@code copy()}.
 */
final class GraphMemIndexedSetTxnSnapshot implements GraphMemIndexedSetTxnReadOps {

    final TripleStore store;
    final IndexingStrategy strategy;

    GraphMemIndexedSetTxnSnapshot(TripleStore store, IndexingStrategy strategy) {
        this.store = store;
        this.strategy = strategy;
    }

    static GraphMemIndexedSetTxnSnapshot empty(IndexingStrategy strategy) {
        return new GraphMemIndexedSetTxnSnapshot(new RoaringTripleStore(strategy), strategy);
    }

    /** Deep-copy a snapshot to obtain an independent one for {@code Graph.copy()}. */
    static GraphMemIndexedSetTxnSnapshot copyOf(GraphMemIndexedSetTxnSnapshot s) {
        return new GraphMemIndexedSetTxnSnapshot(s.store.copy(), s.strategy);
    }

    @Override public boolean contains(Triple m)              { return store.contains(m); }
    @Override public int size()                              { return store.countTriples(); }
    @Override public boolean isEmpty()                       { return store.isEmpty(); }
    @Override public ExtendedIterator<Triple> find(Triple m) { return store.find(m); }
    @Override public Stream<Triple> stream()                 { return store.stream(); }
    @Override public Stream<Triple> stream(Triple m)         { return store.stream(m); }
}

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
import org.apache.jena.util.iterator.ExtendedIterator;

/**
 * Writer-private mutable view of a transactional indexed-set graph.
 * Created at {@code begin(WRITE)} or successful {@code promote()} via
 * {@link #from(GraphMemIndexedSetTxnSnapshot)}; sealed into a published
 * {@link GraphMemIndexedSetTxnSnapshot} at {@code commit()}.
 * <p>
 * Phase A allocates a fully independent {@link TripleStore} on creation
 * by {@link TripleStore#copy()}, so mutations against this working copy
 * cannot affect any concurrently-held snapshot. Phase B will replace
 * this with the per-structure copy-on-write scheme from the
 * implementation plan.
 * <h2>Phase A correctness invariants</h2>
 * <ul>
 *     <li>The working copy's {@code store} is unique to the writer:
 *     it is created by {@code TripleStore.copy()} and never aliased
 *     to any published snapshot.</li>
 *     <li>The published snapshot's {@code store} is never mutated; the
 *     writer publishes by replacing the volatile {@code published}
 *     reference, not by mutating it.</li>
 *     <li>{@link #seal()} hands the writer's freshly-mutated store to
 *     the new snapshot exactly once per write transaction.</li>
 *     <li>Concurrent readers retain their snapshot through their stack;
 *     the JVM keeps the snapshot's store alive for as long as needed.</li>
 * </ul>
 */
final class GraphMemIndexedSetTxnWorkingCopy implements GraphMemIndexedSetTxnReadOps {

    final TripleStore store;
    final IndexingStrategy strategy;

    private GraphMemIndexedSetTxnWorkingCopy(TripleStore store, IndexingStrategy strategy) {
        this.store = store;
        this.strategy = strategy;
    }

    /**
     * Phase A: deep-copy the published store. The resulting working
     * copy shares nothing with {@code published} and is safe to mutate
     * while readers continue to see {@code published}.
     */
    static GraphMemIndexedSetTxnWorkingCopy from(GraphMemIndexedSetTxnSnapshot published) {
        return new GraphMemIndexedSetTxnWorkingCopy(published.store.copy(), published.strategy);
    }

    void add(Triple t)       { store.add(t); }
    void remove(Triple t)    { store.remove(t); }
    void clear()             { store.clear(); }

    /** Convert this working copy into the next published snapshot. */
    GraphMemIndexedSetTxnSnapshot seal() {
        return new GraphMemIndexedSetTxnSnapshot(store, strategy);
    }

    @Override public boolean contains(Triple m)              { return store.contains(m); }
    @Override public int size()                              { return store.countTriples(); }
    @Override public boolean isEmpty()                       { return store.isEmpty(); }
    @Override public ExtendedIterator<Triple> find(Triple m) { return store.find(m); }
    @Override public Stream<Triple> stream()                 { return store.stream(); }
    @Override public Stream<Triple> stream(Triple m)         { return store.stream(m); }
}

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

package org.apache.jena.mem.store.cow.strategies;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.store.cow.CowWriteTxn;
import org.apache.jena.mem.store.cow.TxnTripleSet;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.stream.Stream;

/**
 * COW counterpart of
 * {@link org.apache.jena.mem.store.strategies.MinimalStoreStrategy}: never
 * builds an index but still answers pattern-match operations — by
 * linearly filtering the canonical {@link TxnTripleSet}. Useful when the
 * dataset is small or memory is more precious than match-time
 * performance.
 *
 * <p>Holds only a borrowed reference to the enclosing store's triples;
 * there is no per-strategy mutable state. {@link #fork} therefore just
 * rebinds the reference to the new store's triples.
 */
public final class CowMinimalStoreStrategy implements CowStoreStrategy {

    private final TxnTripleSet triples;

    public CowMinimalStoreStrategy(TxnTripleSet triples) {
        this.triples = triples;
    }

    @Override
    public CowStoreStrategy fork(CowWriteTxn newWriteTxn) {
        return new CowMinimalStoreStrategy(newWriteTxn.getTriples());
    }

    @Override public void addToIndex(Triple t, int i)    { /* no bitmaps */ }
    @Override public void removeFromIndex(Triple t, int i) { /* no bitmaps */ }
    @Override public void clearIndex()                   { /* no bitmaps */ }

    // No index: every partial pattern is answered by a uniform linear scan
    // against Triple#matches. The six-method split mirrors CowStoreStrategy;
    // each rebuilds the match triple and delegates to the shared scan
    // helpers below.

    @Override public boolean containsSubAnyAny(Node s) { return containsMatch(Triple.create(s, Node.ANY, Node.ANY)); }
    @Override public boolean containsAnyPreAny(Node p) { return containsMatch(Triple.create(Node.ANY, p, Node.ANY)); }
    @Override public boolean containsAnyAnyObj(Node o) { return containsMatch(Triple.create(Node.ANY, Node.ANY, o)); }
    @Override public boolean containsSubPreAny(Node s, Node p) { return containsMatch(Triple.create(s, p, Node.ANY)); }
    @Override public boolean containsSubAnyObj(Node s, Node o) { return containsMatch(Triple.create(s, Node.ANY, o)); }
    @Override public boolean containsAnyPreObj(Node p, Node o) { return containsMatch(Triple.create(Node.ANY, p, o)); }

    @Override public Stream<Triple> streamSubAnyAny(Node s) { return streamMatch(Triple.create(s, Node.ANY, Node.ANY)); }
    @Override public Stream<Triple> streamAnyPreAny(Node p) { return streamMatch(Triple.create(Node.ANY, p, Node.ANY)); }
    @Override public Stream<Triple> streamAnyAnyObj(Node o) { return streamMatch(Triple.create(Node.ANY, Node.ANY, o)); }
    @Override public Stream<Triple> streamSubPreAny(Node s, Node p) { return streamMatch(Triple.create(s, p, Node.ANY)); }
    @Override public Stream<Triple> streamSubAnyObj(Node s, Node o) { return streamMatch(Triple.create(s, Node.ANY, o)); }
    @Override public Stream<Triple> streamAnyPreObj(Node p, Node o) { return streamMatch(Triple.create(Node.ANY, p, o)); }

    @Override public ExtendedIterator<Triple> findSubAnyAny(Node s) { return findMatch(Triple.create(s, Node.ANY, Node.ANY)); }
    @Override public ExtendedIterator<Triple> findAnyPreAny(Node p) { return findMatch(Triple.create(Node.ANY, p, Node.ANY)); }
    @Override public ExtendedIterator<Triple> findAnyAnyObj(Node o) { return findMatch(Triple.create(Node.ANY, Node.ANY, o)); }
    @Override public ExtendedIterator<Triple> findSubPreAny(Node s, Node p) { return findMatch(Triple.create(s, p, Node.ANY)); }
    @Override public ExtendedIterator<Triple> findSubAnyObj(Node s, Node o) { return findMatch(Triple.create(s, Node.ANY, o)); }
    @Override public ExtendedIterator<Triple> findAnyPreObj(Node p, Node o) { return findMatch(Triple.create(Node.ANY, p, o)); }

    private boolean containsMatch(Triple tripleMatch) {
        return triples.anyMatch(tripleMatch::matches);
    }

    private Stream<Triple> streamMatch(Triple tripleMatch) {
        return triples.keyStream().filter(tripleMatch::matches);
    }

    private ExtendedIterator<Triple> findMatch(Triple tripleMatch) {
        return triples.keyIterator().filterKeep(tripleMatch::matches);
    }
}

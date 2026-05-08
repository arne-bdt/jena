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

import org.apache.jena.graph.Triple;
import org.apache.jena.mem.pattern.MatchPattern;
import org.apache.jena.mem.store.cow.CowIndexedSetTripleStore;
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
    public CowStoreStrategy fork(CowIndexedSetTripleStore newStore) {
        return new CowMinimalStoreStrategy(newStore.getTriples());
    }

    @Override public void addToIndex(Triple t, int i)    { /* no bitmaps */ }
    @Override public void removeFromIndex(Triple t, int i) { /* no bitmaps */ }
    @Override public void clearIndex()                   { /* no bitmaps */ }

    // The {@code pattern} parameter on the three match methods below is
    // part of the {@link CowStoreStrategy} contract but unused here: a
    // linear scan against {@code Triple#matches} is uniform across all
    // partial patterns, so no per-pattern dispatch is needed.

    @Override
    public boolean containsMatch(Triple tripleMatch, MatchPattern pattern) {
        return triples.anyMatch(tripleMatch::matches);
    }

    @Override
    public Stream<Triple> streamMatch(Triple tripleMatch, MatchPattern pattern) {
        return triples.keyStream().filter(tripleMatch::matches);
    }

    @Override
    public ExtendedIterator<Triple> findMatch(Triple tripleMatch, MatchPattern pattern) {
        return triples.keyIterator().filterKeep(tripleMatch::matches);
    }
}

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
import org.apache.jena.mem.store.cow.CowWriteTxn;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.stream.Stream;

/**
 * COW counterpart of
 * {@link org.apache.jena.mem.store.strategies.ManualStoreStrategy}: never
 * builds an index automatically. Add/remove/clear are no-ops on the
 * index side; pattern-match operations throw
 * {@link UnsupportedOperationException} until the user explicitly
 * initialises the index (typically via
 * {@code CowIndexedSetTripleStore.initializeIndex()} /
 * {@code initializeIndexParallel()}, which atomically swap this strategy
 * for a {@link CowEagerStoreStrategy}).
 *
 * <p>Stateless: the same instance is shared across all forks; there is
 * nothing to copy.
 */
public final class CowManualStoreStrategy implements CowStoreStrategy {

    /** Singleton instance — the strategy carries no state. */
    public static final CowManualStoreStrategy INSTANCE = new CowManualStoreStrategy();

    private static final String NOT_INITIALIZED =
            "Index has not been initialised yet. Call initializeIndex() (or initializeIndexParallel()) before issuing pattern lookups.";

    private CowManualStoreStrategy() {}

    @Override public CowStoreStrategy fork(CowWriteTxn newWriteTxn) { return this; }

    @Override public void addToIndex(Triple t, int i)    { /* no index */ }
    @Override public void removeFromIndex(Triple t, int i) { /* no index */ }
    @Override public void clearIndex()                   { /* no index */ }

    @Override public boolean throwsOnUninitializedLookup() { return true; }

    @Override
    public boolean containsMatch(Triple match, MatchPattern pattern) {
        throw new UnsupportedOperationException(NOT_INITIALIZED);
    }

    @Override
    public Stream<Triple> streamMatch(Triple match, MatchPattern pattern) {
        throw new UnsupportedOperationException(NOT_INITIALIZED);
    }

    @Override
    public ExtendedIterator<Triple> findMatch(Triple match, MatchPattern pattern) {
        throw new UnsupportedOperationException(NOT_INITIALIZED);
    }
}

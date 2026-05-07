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

package org.apache.jena.mem.store.cow;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem.IndexingStrategy;
import org.apache.jena.mem.store.TripleStore;
import org.apache.jena.mem.store.indexed.IndexedSetTripleStore;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.stream.Stream;

/**
 * Copy-on-write {@link TripleStore} intended to back a transactional graph
 * (see {@code GraphMemIndexedSetCowTxn}). "Cow" stands for <i>copy on
 * write</i>.
 * <p>
 * In addition to the standard {@link TripleStore} contract, this class
 * exposes {@link #forkForWrite()}, which returns a writable copy that is
 * intended to <b>replace</b> the original (the original must be treated as
 * frozen after the fork). This is a strictly stronger contract than
 * {@link #copy()} (whose copies are independently mutable) and is the hook
 * Phase B uses to make {@code begin(WRITE)} cheap: once the underlying
 * collections support array sharing, {@link #forkForWrite()} will allocate
 * only writer-private bookkeeping rather than a full deep copy.
 *
 * <h2>Phase B status</h2>
 * This class is currently a thin wrapper over {@link IndexedSetTripleStore}.
 * {@link #forkForWrite()} performs a full deep copy of the inner store, so
 * the cost model of a write transaction is the same as the baseline
 * {@link IndexedSetTripleStore}. The architectural seam is in place so the
 * inner state can later be replaced with copy-on-write transactional
 * collections without changing any caller, including
 * {@code GraphMemIndexedSetCowTxn}.
 *
 * <h2>Discipline</h2>
 * After {@link #forkForWrite()} returns, callers must not call any mutator
 * on the original store. Doing so will be safe today (deep copies are
 * independent) but will break correctness once shared-array COW is wired in.
 * The transactional graph that owns these stores enforces the discipline by
 * assigning the published reference exactly once per commit and never
 * mutating it thereafter.
 */
public class CowIndexedSetTripleStore implements TripleStore {

    private final TripleStore inner;

    /** Creates an empty store using {@link IndexingStrategy#EAGER}. */
    public CowIndexedSetTripleStore() {
        this(IndexingStrategy.EAGER);
    }

    /** Creates an empty store using the given indexing strategy. */
    public CowIndexedSetTripleStore(IndexingStrategy indexingStrategy) {
        this(new IndexedSetTripleStore(indexingStrategy));
    }

    /**
     * Internal constructor wrapping a freshly created or freshly forked
     * inner store. Kept package-private so callers go through
     * {@link #forkForWrite()} or {@link #copy()}.
     */
    CowIndexedSetTripleStore(TripleStore inner) {
        this.inner = inner;
    }

    /**
     * Fork this store for a write transaction.
     * <p>
     * The returned store is independent for the purpose of mutation and is
     * intended to <b>replace</b> the original after commit. The caller must
     * not call any mutator on this (the original) store after this method
     * returns; otherwise correctness will be broken once the inner state is
     * upgraded to copy-on-write internals.
     * <p>
     * The contract is stronger than {@link #copy()}, which permits
     * independent mutation of both stores. It exists as a separate named
     * method specifically so the call site signals "I am about to fork for
     * a write transaction, the original is frozen from here on", which the
     * future COW implementation will rely on.
     */
    public CowIndexedSetTripleStore forkForWrite() {
        // Phase B initial scaffolding: full deep copy. Will be replaced
        // by writer-private bookkeeping copy + shared-array sharing once
        // the transactional collection types are in place.
        return new CowIndexedSetTripleStore(inner.copy());
    }

    @Override
    public CowIndexedSetTripleStore copy() {
        // Standard Copyable: both stores are independently mutable.
        return new CowIndexedSetTripleStore(inner.copy());
    }

    // ----- TripleStore: pure delegation -----

    @Override public void add(Triple t)              { inner.add(t); }
    @Override public void remove(Triple t)           { inner.remove(t); }
    @Override public void clear()                    { inner.clear(); }
    @Override public int countTriples()              { return inner.countTriples(); }
    @Override public boolean isEmpty()               { return inner.isEmpty(); }
    @Override public boolean contains(Triple m)      { return inner.contains(m); }
    @Override public Stream<Triple> stream()         { return inner.stream(); }
    @Override public Stream<Triple> stream(Triple m) { return inner.stream(m); }
    @Override public ExtendedIterator<Triple> find(Triple m) { return inner.find(m); }
}

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

package org.apache.jena.mem.store.mvcc;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.stream.Stream;

/**
 * A lock-free, version-pinned read view of an {@link MvccTripleStore}. Captured by
 * {@link MvccTripleStore#openReadView()} with a single volatile read; every read
 * operation filters the shared store by this view's fixed version. The view is
 * registered with the version control for vacuum tracking and <em>must</em> be
 * {@link #close() closed} exactly once to deregister.
 * <p>
 * The view's read operations are thread-safe and may be invoked from a thread
 * other than the one that opened it (e.g. for parallel cross-graph reads).
 */
public final class MvccReadView {

    private static final Triple ANY = Triple.create(Node.ANY, Node.ANY, Node.ANY);

    private final MvccTripleStore store;
    private final MvccTripleStore.Gen gen;
    private final boolean registered;
    private boolean closed = false;

    MvccReadView(MvccTripleStore store, MvccTripleStore.Gen gen, boolean registered) {
        this.store = store;
        this.gen = gen;
        this.registered = registered;
    }

    /** @return the version this view is pinned at. */
    public long version() {
        return gen.version();
    }

    /** @return {@code true} iff some triple matches the pattern at this version. */
    public boolean contains(Triple match) {
        return store.contains(gen, gen.version(), match);
    }

    /** @return an iterator over triples matching the pattern at this version. */
    public ExtendedIterator<Triple> find(Triple match) {
        return store.find(gen, gen.version(), match);
    }

    /** @return a stream over triples matching the pattern at this version. */
    public Stream<Triple> stream(Triple match) {
        return store.stream(gen, gen.version(), match);
    }

    /** @return a stream over every triple visible at this version. */
    public Stream<Triple> stream() {
        return store.stream(gen, gen.version(), ANY);
    }

    /** @return the number of triples visible at this version. */
    public int count() {
        return store.countLive(gen);
    }

    /** @return {@code true} iff no triples are visible at this version. */
    public boolean isEmpty() {
        return gen.liveCount() == 0;
    }

    /** Deregister this reader from vacuum tracking. Idempotent; a no-op for a
     * transient (unregistered) view. */
    public void close() {
        if (registered && !closed) {
            closed = true;
            store.versionControl().deregisterReader(gen.version());
        }
    }
}

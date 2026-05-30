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
import org.apache.jena.mem.store.mvcc.strategies.MvccStoreStrategy;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * The writer's working state for one write transaction. Changes are buffered in an
 * overlay (a set of added triples and a set of removed triples) layered over the
 * committed generation captured at begin; nothing touches the shared store until
 * {@link MvccTripleStore#commit(MvccWriteTxn)} applies the overlay. Consequently
 * {@code abort} is a no-op (just drop this object).
 * <p>
 * The overlay maintains two invariants, which keep commit application correct and
 * avoid spurious duplicate slots when a committed triple is deleted and re-added
 * within one transaction:
 * <ul>
 *   <li>{@link #added} holds only triples that are <em>not</em> committed-live;</li>
 *   <li>{@link #removed} holds only triples that <em>are</em> committed-live.</li>
 * </ul>
 * Single-threaded: used only by the writer thread under the store's writer lock.
 */
public final class MvccWriteTxn {

    private static final Triple ANY = Triple.create(Node.ANY, Node.ANY, Node.ANY);

    private final MvccTripleStore store;
    private final long version;
    private final MvccTripleStore.Gen committedGen;

    private final Set<Triple> added = new HashSet<>();
    private final Set<Triple> removed = new HashSet<>();

    MvccWriteTxn(MvccTripleStore store, long version, MvccTripleStore.Gen committedGen) {
        this.store = store;
        this.version = version;
        this.committedGen = committedGen;
    }

    long version() {
        return version;
    }

    Set<Triple> added() {
        return added;
    }

    Set<Triple> removed() {
        return removed;
    }

    /** @return whether this transaction has any net change to publish. */
    public boolean hasChanges() {
        return !added.isEmpty() || !removed.isEmpty();
    }

    // ---- Mutations -----------------------------------------------------------

    /** Add a triple (idempotent against the committed + overlay state). */
    public void add(Triple t) {
        if (store.committedContains(t)) {
            // Already committed-live: cancel any pending delete; otherwise no change.
            removed.remove(t);
        } else {
            removed.remove(t);
            added.add(t);
        }
    }

    /** Remove a triple (idempotent against the committed + overlay state). */
    public void remove(Triple t) {
        if (store.committedContains(t)) {
            added.remove(t);
            removed.add(t);
        } else {
            // Committed-absent: just cancel any pending add.
            added.remove(t);
        }
    }

    // ---- Reads (committed view + overlay) ------------------------------------

    /** @return {@code true} iff some triple matches the pattern in this txn's view. */
    public boolean contains(Triple match) {
        if (isConcrete(match)) {
            return added.contains(match)
                    || (store.committedContains(match) && !removed.contains(match));
        }
        return find(match).hasNext();
    }

    /** @return an iterator over triples matching the pattern in this txn's view. */
    public ExtendedIterator<Triple> find(Triple match) {
        final ExtendedIterator<Triple> base =
                store.find(committedGen, committedGen.version(), match)
                        .filterDrop(removed::contains);
        if (added.isEmpty()) {
            return base;
        }
        final List<Triple> extra = new ArrayList<>();
        for (Triple t : added) {
            if (MvccTripleStore.matches(match, t)) {
                extra.add(t);
            }
        }
        return extra.isEmpty() ? base : base.andThen(WrappedIterator.create(extra.iterator()));
    }

    /** @return a stream over triples matching the pattern in this txn's view. */
    public Stream<Triple> stream(Triple match) {
        return org.apache.jena.atlas.iterator.Iter.asStream(find(match));
    }

    /** @return a stream over every triple visible in this txn's view. */
    public Stream<Triple> stream() {
        return stream(ANY);
    }

    /** @return the number of triples visible in this txn's view. */
    public int count() {
        return committedGen.liveCount() - removed.size() + added.size();
    }

    /** @return {@code true} iff this txn's view is empty. */
    public boolean isEmpty() {
        return count() == 0;
    }

    private static boolean isConcrete(Triple match) {
        return MvccStoreStrategy.isConcrete(match.getSubject())
                && MvccStoreStrategy.isConcrete(match.getPredicate())
                && MvccStoreStrategy.isConcrete(match.getObject());
    }
}

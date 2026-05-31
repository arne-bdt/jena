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

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.GraphMemIndexedSet;
import org.apache.jena.mem.store.indexed.TripleSet;
import org.apache.jena.mem.store.mvcc.strategies.MvccStoreStrategy;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.stream.Stream;

/**
 * The writer's working state for one write transaction. Changes are buffered in an
 * overlay layered over the committed generation captured at begin; nothing touches
 * the shared store until {@link MvccTripleStore#commit(MvccWriteTxn)} applies the
 * overlay. Consequently {@code abort} is a no-op (just drop this object).
 * <p>
 * The added side is an <em>indexed</em> {@link GraphMemIndexedSet} rather than a
 * plain set, so a read-your-writes {@link #find} narrows through the overlay's
 * subject/predicate/object index — O(matches) — instead of linearly scanning every
 * triple added so far in the transaction. Without that index a transaction that adds
 * {@code B} triples while probing each one (the typical read-modify-write loop) costs
 * O(B&sup2;); the index restores it to O(B). Matching is term-based (RDF-term
 * equality), identical to {@link MvccTripleStore}'s committed view, so layering the
 * two is exact.
 * <p>
 * The removed side is a {@link TripleSet} (the same
 * {@link org.apache.jena.mem.collection.FastHashSet} the committed store uses for its
 * dedup): it is probed via {@code containsKey} for the committed base's
 * {@code filterDrop} and iterated in full at commit, both of which it does faster
 * than a {@link java.util.HashSet}. The whole overlay is thus the
 * base-plus-indexed-additions-minus-deletions shape of a delta graph.
 * <p>
 * The overlay maintains two invariants, which keep commit application correct and
 * avoid spurious duplicate slots when a committed triple is deleted and re-added
 * within one transaction:
 * <ul>
 *   <li>{@link #added} holds only triples that are <em>not</em> committed-live;</li>
 *   <li>{@link #removed} holds only triples that <em>are</em> committed-live.</li>
 * </ul>
 * Single-threaded: used only by the writer thread under the store's writer lock, so
 * the non-thread-safe overlay structures need no synchronisation.
 */
public final class MvccWriteTxn {

    private static final Triple ANY = Triple.create(Node.ANY, Node.ANY, Node.ANY);

    private final MvccTripleStore store;
    private final long version;
    private final MvccTripleStore.Gen committedGen;

    private final Graph added = new GraphMemIndexedSet();
    private final TripleSet removed = new TripleSet();

    MvccWriteTxn(MvccTripleStore store, long version, MvccTripleStore.Gen committedGen) {
        this.store = store;
        this.version = version;
        this.committedGen = committedGen;
    }

    /** @return the version this transaction will commit at. */
    public long version() {
        return version;
    }

    Graph added() {
        return added;
    }

    TripleSet removed() {
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
            removed.tryRemove(t);
        } else {
            removed.tryRemove(t);
            added.add(t);
        }
    }

    /** Remove a triple (idempotent against the committed + overlay state). */
    public void remove(Triple t) {
        if (store.committedContains(t)) {
            added.delete(t);
            removed.tryAdd(t);
        } else {
            // Committed-absent: just cancel any pending add.
            added.delete(t);
        }
    }

    // ---- Reads (committed view + overlay) ------------------------------------

    /** @return {@code true} iff some triple matches the pattern in this txn's view. */
    public boolean contains(Triple match) {
        if (isConcrete(match)) {
            return added.contains(match)
                    || (store.committedContains(match) && !removed.containsKey(match));
        }
        return find(match).hasNext();
    }

    /** @return an iterator over triples matching the pattern in this txn's view. */
    public ExtendedIterator<Triple> find(Triple match) {
        final ExtendedIterator<Triple> base =
                store.find(committedGen, committedGen.version(), match)
                        .filterDrop(removed::containsKey);
        // The overlay never holds committed-live triples (see invariants), so it can
        // never duplicate a triple yielded by base; its find() narrows through the
        // overlay's own index rather than scanning every added triple.
        return added.isEmpty() ? base : base.andThen(added.find(match));
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

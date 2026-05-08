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
import org.apache.jena.mem.pattern.MatchPattern;
import org.apache.jena.mem.pattern.PatternClassifier;
import org.apache.jena.mem.store.cow.CowIndexedSetTripleStore;
import org.apache.jena.mem.store.cow.TxnNodesToIndices;
import org.apache.jena.mem.store.cow.TxnTripleSet;
import org.apache.jena.mem.store.indexed.IndexList;
import org.apache.jena.mem.store.indexed.IndexListIterator;
import org.apache.jena.mem.store.indexed.IndexListSpliterator;
import org.apache.jena.mem.store.indexed.IndexListsIterator;
import org.apache.jena.mem.store.indexed.IndexListsSpliterator;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NullIterator;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * COW counterpart of
 * {@link org.apache.jena.mem.store.indexed.EagerStoreStrategy}: maintains
 * a complete subject/predicate/object index over the canonical
 * {@link TxnTripleSet} at all times, plus three reverse-index {@code int[]}s
 * giving each triple's position inside its component {@link IndexList}
 * (for {@code O(1)} removal).
 *
 * <h2>Sharing model</h2>
 * The three node-keyed spines ({@link TxnNodesToIndices}) follow the
 * standard COW spine layout: their {@code keys}/{@code hashCodes}/
 * {@code values} are shared with the source, their
 * {@code positions}/{@code deleted} are forked. The three reverse-index
 * arrays are <i>not</i> shared — the writer mutates them at arbitrary alive
 * triple-indices on removal (swap-with-last bookkeeping), so they are
 * cloned per fork.
 * <p>
 * {@link IndexList}s held in the spines' {@code values[]} arrays are
 * shared until first mutation; {@link #ensureWritableList} performs the
 * clone-on-first-touch by comparing each list's {@link IndexList#getOwnerId()
 * ownership id} against this strategy's own {@link #ownerId}, then
 * replacing the spine slot via {@link TxnNodesToIndices#put} (which is
 * the COW tombstone-and-append).
 *
 * <h2>Why the per-list owner id</h2>
 * The earlier design used an {@code IdentityHashMap}-backed
 * {@code Set<IndexList>} per strategy to track writer-owned lists. The
 * per-list {@code long ownerId} replaces it: a single field load + int
 * compare in the hot path replaces a hash-table lookup, and there's no
 * per-fork allocation at all (ownership is intrinsic to the list). For
 * graphs with many distinct nodes this is the difference between O(N)
 * map entries per fork and a single long field per list, paid once.
 */
public final class CowEagerStoreStrategy implements CowStoreStrategy {

    private static final String UNSUPPORTED_PATTERN = "Unsupported pattern classifier: %s";

    /**
     * Counter used to stamp each strategy with a globally unique
     * {@link #ownerId}. {@code AtomicLong} so concurrent strategy
     * construction (e.g. multiple readers race-building eager strategies
     * for a LAZY published view) gets distinct ids. The 64-bit space is
     * effectively inexhaustible for any realistic process lifetime.
     */
    private static final AtomicLong NEXT_OWNER_ID = new AtomicLong();

    /** Canonical triples. Borrowed reference to the enclosing store's triples. */
    private final TxnTripleSet triples;

    private final TxnNodesToIndices subjectIndex;
    private final TxnNodesToIndices predicateIndex;
    private final TxnNodesToIndices objectIndex;

    private int[] sReverseIndices;
    private int[] pReverseIndices;
    private int[] oReverseIndices;

    /**
     * Unique ownership id stamped onto every {@link IndexList} this
     * strategy creates or clones. {@link #ensureWritableList} uses
     * {@code list.getOwnerId() == this.ownerId} as the writer-owned
     * predicate.
     */
    private final long ownerId = NEXT_OWNER_ID.incrementAndGet();

    // -------------------------------------------------------------------

    /**
     * Build an empty eager index over the given triple set, then index
     * any triples already present (sequentially). Installs a grow hook so
     * the reverse-index arrays grow in lock-step with {@code triples.keys[]}.
     *
     * @param triples the canonical triple set
     */
    public CowEagerStoreStrategy(TxnTripleSet triples) {
        this(triples, false, true);
    }

    /**
     * Build an empty eager index over the given triple set, then index
     * any triples already present. Installs the grow hook.
     *
     * @param triples  the canonical triple set
     * @param parallel if {@code true}, populate the three indices
     *                 concurrently (used by the LAZY-parallel auto-build
     *                 and by {@link CowIndexedSetTripleStore#initializeIndexParallel})
     */
    public CowEagerStoreStrategy(TxnTripleSet triples, boolean parallel) {
        this(triples, parallel, true);
    }

    /**
     * Full constructor.
     *
     * @param triples         the canonical triple set
     * @param parallel        if {@code true}, populate the three indices
     *                        concurrently
     * @param installGrowHook whether to register the keys-grow callback on
     *                        {@code triples}. Pass {@code false} when the
     *                        strategy is being built against a published
     *                        snapshot (e.g. a LAZY → EAGER race-build on a
     *                        snapshot held by multiple readers): the
     *                        snapshot's keys never grow, so the hook would
     *                        never fire, and concurrent installations
     *                        would otherwise race on the shared
     *                        {@code TxnTripleSet}'s hook field.
     *                        {@link #addToIndex} resizes the reverse-index
     *                        arrays on-demand if the keys array grows
     *                        without the hook (only relevant on a writer's
     *                        working copy that took this code path).
     */
    public CowEagerStoreStrategy(TxnTripleSet triples, boolean parallel, boolean installGrowHook) {
        this.triples = triples;
        this.subjectIndex = new TxnNodesToIndices();
        this.predicateIndex = new TxnNodesToIndices();
        this.objectIndex = new TxnNodesToIndices();
        final int len = triples.getInternalKeysLength();
        this.sReverseIndices = new int[len];
        this.pReverseIndices = new int[len];
        this.oReverseIndices = new int[len];
        if (installGrowHook) {
            triples.setOnKeysGrowHook(this::onTriplesKeysGrew);
        }
        if (triples.size() > 0) {
            if (parallel) {
                indexAllParallel();
            } else {
                indexAllSequential();
            }
        }
    }

    /**
     * Fork constructor — used by {@link #fork(CowIndexedSetTripleStore)}.
     * Forks each spine and clones each reverse-index array. The new
     * strategy is stamped with a fresh {@link #ownerId}, so every
     * {@link IndexList} inherited from the source is treated as
     * "not writer-owned" until {@link #ensureWritableList} clones it on
     * first mutation.
     */
    private CowEagerStoreStrategy(TxnTripleSet newTriples, CowEagerStoreStrategy source) {
        this.triples = newTriples;
        this.subjectIndex = source.subjectIndex.fork();
        this.predicateIndex = source.predicateIndex.fork();
        this.objectIndex = source.objectIndex.fork();
        this.sReverseIndices = source.sReverseIndices.clone();
        this.pReverseIndices = source.pReverseIndices.clone();
        this.oReverseIndices = source.oReverseIndices.clone();
        // Hook routes grow events to THIS strategy's reverse-index arrays.
        newTriples.setOnKeysGrowHook(this::onTriplesKeysGrew);
    }

    @Override
    public CowStoreStrategy fork(CowIndexedSetTripleStore newStore) {
        return new CowEagerStoreStrategy(newStore.getTriples(), this);
    }

    @Override
    public boolean isIndexInitialized() {
        return true;
    }

    // ----- Index maintenance -----------------------------------------

    @Override
    public void addToIndex(Triple t, int index) {
        ensureReverseIndicesCapacity(index);
        sReverseIndices[index] = ensureWritableList(subjectIndex, t.getSubject()).add(index);
        pReverseIndices[index] = ensureWritableList(predicateIndex, t.getPredicate()).add(index);
        oReverseIndices[index] = ensureWritableList(objectIndex, t.getObject()).add(index);
    }

    /**
     * Defensively resize the three reverse-index arrays so they cover
     * {@code index}. Normally the keys-grow hook (if installed) does this
     * eagerly when {@code triples.keys[]} grows; this fallback covers the
     * case where the strategy was constructed without the hook (see the
     * {@code installGrowHook} parameter on the constructor).
     */
    private void ensureReverseIndicesCapacity(int index) {
        if (index < sReverseIndices.length) return;
        final int newLength = Math.max(triples.getInternalKeysLength(), index + 1);
        sReverseIndices = Arrays.copyOf(sReverseIndices, newLength);
        pReverseIndices = Arrays.copyOf(pReverseIndices, newLength);
        oReverseIndices = Arrays.copyOf(oReverseIndices, newLength);
    }

    @Override
    public void removeFromIndex(Triple t, int index) {
        removeFromComponent(subjectIndex, t.getSubject(), index, sReverseIndices);
        removeFromComponent(predicateIndex, t.getPredicate(), index, pReverseIndices);
        removeFromComponent(objectIndex, t.getObject(), index, oReverseIndices);
    }

    @Override
    public void clearIndex() {
        subjectIndex.clear();
        predicateIndex.clear();
        objectIndex.clear();
        final int len = triples.getInternalKeysLength();
        sReverseIndices = new int[len];
        pReverseIndices = new int[len];
        oReverseIndices = new int[len];
        // Ownership stamp on this strategy is unchanged — any list
        // created from here on still belongs to this strategy.
    }

    private void removeFromComponent(TxnNodesToIndices spine, Node node, int idx, int[] reverse) {
        final IndexList list = ensureWritableList(spine, node);
        final int oldPos = reverse[idx];
        final int switched = list.removeAt(oldPos);
        if (list.isEmpty()) {
            spine.removeUnchecked(node);
        } else if (switched != -1) {
            reverse[switched] = oldPos;
        }
    }

    /**
     * Return the {@link IndexList} stored at {@code spine.get(node)},
     * cloned and re-installed if it's still shared with the snapshot.
     * Ownership is determined by {@link IndexList#getOwnerId()}: if the
     * list's stamped id matches this strategy's {@link #ownerId} the
     * list is already writer-owned and can be mutated in place;
     * otherwise it is cloned, the clone is stamped, and the spine slot
     * is updated via {@link TxnNodesToIndices#put} (the COW
     * tombstone-and-append, so any open snapshot still resolves the key
     * to the original list through its own probe table).
     */
    private IndexList ensureWritableList(TxnNodesToIndices spine, Node node) {
        IndexList list = spine.get(node);
        if (list == null) {
            list = new IndexList();
            list.setOwnerId(ownerId);
            spine.put(node, list);
            return list;
        }
        if (list.getOwnerId() == ownerId) {
            return list;                        // already writer-owned
        }
        // Shared with snapshot — clone before mutation.
        final IndexList forked = list.clone();
        forked.setOwnerId(ownerId);
        spine.put(node, forked);                // tombstone-and-append on the spine
        return forked;
    }

    /** Resize the reverse-index arrays whenever {@code triples.keys[]} grows. */
    private void onTriplesKeysGrew(int newKeysLength) {
        sReverseIndices = Arrays.copyOf(sReverseIndices, newKeysLength);
        pReverseIndices = Arrays.copyOf(pReverseIndices, newKeysLength);
        oReverseIndices = Arrays.copyOf(oReverseIndices, newKeysLength);
    }

    /** Sequentially populate all three indices from {@link #triples}. */
    private void indexAllSequential() {
        triples.forEachKey(this::addToIndex);
    }

    /**
     * Populate the three indices in parallel: each runs on its own thread,
     * iterating the triple set independently. The three indices touch
     * disjoint state, so no synchronisation is needed.
     */
    private void indexAllParallel() {
        final var futureObjects = CompletableFuture.runAsync(
                () -> triples.forEachKey((t, i) -> addOIndex(t.getObject(), i)));
        final var futureSubjects = CompletableFuture.runAsync(
                () -> triples.forEachKey((t, i) -> addSIndex(t.getSubject(), i)));
        triples.forEachKey((t, i) -> addPIndex(t.getPredicate(), i));
        CompletableFuture.allOf(futureObjects, futureSubjects).join();
    }

    private void addSIndex(Node subject, int index) {
        sReverseIndices[index] = ensureWritableList(subjectIndex, subject).add(index);
    }
    private void addPIndex(Node predicate, int index) {
        pReverseIndices[index] = ensureWritableList(predicateIndex, predicate).add(index);
    }
    private void addOIndex(Node object, int index) {
        oReverseIndices[index] = ensureWritableList(objectIndex, object).add(index);
    }

    // ----- Pattern match ---------------------------------------------

    @Override
    public boolean containsMatch(Triple match, MatchPattern pattern) {
        return switch (pattern) {
            case SUB_ANY_ANY -> subjectIndex.containsKey(match.getSubject());
            case ANY_PRE_ANY -> predicateIndex.containsKey(match.getPredicate());
            case ANY_ANY_OBJ -> objectIndex.containsKey(match.getObject());
            case SUB_PRE_ANY -> bothIndexed(subjectIndex, match.getSubject(), sReverseIndices,
                                            predicateIndex, match.getPredicate(), pReverseIndices);
            case ANY_PRE_OBJ -> bothIndexed(predicateIndex, match.getPredicate(), pReverseIndices,
                                            objectIndex, match.getObject(), oReverseIndices);
            case SUB_ANY_OBJ -> bothIndexed(subjectIndex, match.getSubject(), sReverseIndices,
                                            objectIndex, match.getObject(), oReverseIndices);
            default -> throw new IllegalStateException(String.format(UNSUPPORTED_PATTERN, pattern));
        };
    }

    private static boolean bothIndexed(TxnNodesToIndices a, Node aKey, int[] aReverse,
                                       TxnNodesToIndices b, Node bKey, int[] bReverse) {
        final IndexList aList = a.get(aKey);
        if (aList == null) return false;
        final IndexList bList = b.get(bKey);
        if (bList == null) return false;
        return IndexList.intersects(aList, aReverse, bList, bReverse);
    }

    @Override
    public Stream<Triple> streamMatch(Triple match, MatchPattern pattern) {
        return switch (pattern) {
            case SUB_ANY_ANY -> singleListStream(subjectIndex, match.getSubject());
            case ANY_PRE_ANY -> singleListStream(predicateIndex, match.getPredicate());
            case ANY_ANY_OBJ -> singleListStream(objectIndex, match.getObject());
            case SUB_PRE_ANY -> intersectStream(subjectIndex, match.getSubject(), sReverseIndices,
                                                predicateIndex, match.getPredicate(), pReverseIndices);
            case ANY_PRE_OBJ -> intersectStream(predicateIndex, match.getPredicate(), pReverseIndices,
                                                objectIndex, match.getObject(), oReverseIndices);
            case SUB_ANY_OBJ -> intersectStream(subjectIndex, match.getSubject(), sReverseIndices,
                                                objectIndex, match.getObject(), oReverseIndices);
            default -> throw new IllegalStateException(String.format(UNSUPPORTED_PATTERN, pattern));
        };
    }

    private Stream<Triple> singleListStream(TxnNodesToIndices spine, Node key) {
        final IndexList list = spine.get(key);
        if (list == null) return Stream.empty();
        return StreamSupport.stream(new IndexListSpliterator(triples, list), false);
    }

    private Stream<Triple> intersectStream(TxnNodesToIndices a, Node aKey, int[] aReverse,
                                           TxnNodesToIndices b, Node bKey, int[] bReverse) {
        final IndexList aList = a.get(aKey);
        if (aList == null) return Stream.empty();
        final IndexList bList = b.get(bKey);
        if (bList == null) return Stream.empty();
        return StreamSupport.stream(
                new IndexListsSpliterator(triples, aList, aReverse, bList, bReverse), false);
    }

    @Override
    public ExtendedIterator<Triple> findMatch(Triple match, MatchPattern pattern) {
        return switch (pattern) {
            case SUB_ANY_ANY -> singleListFind(subjectIndex, match.getSubject());
            case ANY_PRE_ANY -> singleListFind(predicateIndex, match.getPredicate());
            case ANY_ANY_OBJ -> singleListFind(objectIndex, match.getObject());
            case SUB_PRE_ANY -> intersectFind(subjectIndex, match.getSubject(), sReverseIndices,
                                              predicateIndex, match.getPredicate(), pReverseIndices);
            case ANY_PRE_OBJ -> intersectFind(predicateIndex, match.getPredicate(), pReverseIndices,
                                              objectIndex, match.getObject(), oReverseIndices);
            case SUB_ANY_OBJ -> intersectFind(subjectIndex, match.getSubject(), sReverseIndices,
                                              objectIndex, match.getObject(), oReverseIndices);
            default -> throw new IllegalStateException(String.format(UNSUPPORTED_PATTERN, pattern));
        };
    }

    private ExtendedIterator<Triple> singleListFind(TxnNodesToIndices spine, Node key) {
        final IndexList list = spine.get(key);
        if (list == null) return NullIterator.instance();
        return new IndexListIterator(triples, list);
    }

    private ExtendedIterator<Triple> intersectFind(TxnNodesToIndices a, Node aKey, int[] aReverse,
                                                   TxnNodesToIndices b, Node bKey, int[] bReverse) {
        final IndexList aList = a.get(aKey);
        if (aList == null) return NullIterator.instance();
        final IndexList bList = b.get(bKey);
        if (bList == null) return NullIterator.instance();
        return new IndexListsIterator(triples, aList, aReverse, bList, bReverse);
    }
}

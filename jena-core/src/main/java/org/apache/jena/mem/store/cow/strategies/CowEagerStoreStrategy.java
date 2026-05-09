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
import org.apache.jena.mem.store.cow.CowWriteTxn;
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
 * {@code positions}/{@code deleted}/{@code valueOwnedByThisWriter} are
 * forked (or, in the case of the writer-owned bitmap, allocated fresh).
 * The three reverse-index arrays are <i>not</i> shared — the writer
 * mutates them at arbitrary alive triple-indices on removal
 * (swap-with-last bookkeeping), so they are cloned per fork.
 * <p>
 * {@link IndexList}s held in the spines' {@code values[]} arrays are
 * shared until first mutation; {@link #ensureWritableList} performs the
 * clone-on-first-touch by consulting the spine's
 * {@link TxnNodesToIndices#isValueOwnedByThisWriter(int) writer-owned}
 * bitmap (a per-slot, writer-private boolean). On a miss the strategy
 * clones the list and re-installs it via {@link TxnNodesToIndices#put}
 * (the COW tombstone-and-append), which automatically marks the new
 * slot writer-owned.
 *
 * <h2>Why spine-level ownership</h2>
 * Earlier designs tracked ownership either via an {@code IdentityHashMap
 * <IndexList>} per strategy or via a {@code long ownerId} field on
 * {@link IndexList} itself. The current scheme moves the bit onto the
 * spine's per-slot writer-private array, where ownership conceptually
 * belongs: the spine is the place that knows which slots it has freshly
 * written this transaction. The bit is set exclusively by the spine's
 * {@code put} path and never propagated across forks (a fresh fork has
 * placed nothing yet, so all bits start clear), so no cloning of the
 * ownership array is needed at fork time.
 */
public final class CowEagerStoreStrategy implements CowStoreStrategy {

    private static final String UNSUPPORTED_PATTERN = "Unsupported pattern classifier: %s";

    /** Canonical triples. Borrowed reference to the enclosing store's triples. */
    private final TxnTripleSet triples;

    private final TxnNodesToIndices subjectIndex;
    private final TxnNodesToIndices predicateIndex;
    private final TxnNodesToIndices objectIndex;

    private int[] sReverseIndices;
    private int[] pReverseIndices;
    private int[] oReverseIndices;

    // -------------------------------------------------------------------

    /**
     * Build an empty eager index over the given triple set, then index
     * any triples already present (sequentially).
     *
     * @param triples the canonical triple set
     */
    public CowEagerStoreStrategy(TxnTripleSet triples) {
        this(triples, false);
    }

    /**
     * Build an empty eager index over the given triple set, then index
     * any triples already present.
     *
     * @param triples  the canonical triple set
     * @param parallel if {@code true}, populate the three indices
     *                 concurrently (used by the LAZY-parallel auto-build
     *                 and by the writer-side initializeIndexParallel hook)
     */
    public CowEagerStoreStrategy(TxnTripleSet triples, boolean parallel) {
        this.triples = triples;
        this.subjectIndex = new TxnNodesToIndices();
        this.predicateIndex = new TxnNodesToIndices();
        this.objectIndex = new TxnNodesToIndices();
        final int len = triples.getInternalKeysLength();
        this.sReverseIndices = new int[len];
        this.pReverseIndices = new int[len];
        this.oReverseIndices = new int[len];
        if (triples.size() > 0) {
            if (parallel) {
                indexAllParallel();
            } else {
                indexAllSequential();
            }
        }
    }

    /**
     * Fork constructor — used by {@link #fork(CowWriteTxn)}. Forks each
     * spine and clones each reverse-index array. Every {@link IndexList}
     * inherited from the source is "not writer-owned" until
     * {@link #ensureWritableList} clones it on first mutation, because
     * the spines' {@code valueOwnedByThisWriter} bitmaps start fresh
     * (all-clear) on fork.
     */
    private CowEagerStoreStrategy(TxnTripleSet newTriples, CowEagerStoreStrategy source) {
        this.triples = newTriples;
        this.subjectIndex = source.subjectIndex.fork();
        this.predicateIndex = source.predicateIndex.fork();
        this.objectIndex = source.objectIndex.fork();
        this.sReverseIndices = source.sReverseIndices.clone();
        this.pReverseIndices = source.pReverseIndices.clone();
        this.oReverseIndices = source.oReverseIndices.clone();
    }

    @Override
    public CowStoreStrategy fork(CowWriteTxn newWriteTxn) {
        return new CowEagerStoreStrategy(newWriteTxn.getTriples(), this);
    }

    @Override
    public boolean isIndexInitialized() {
        return true;
    }

    // ----- Index maintenance -----------------------------------------

    @Override
    public void addToIndex(Triple t, int index) {
        // Resize the writer-private reverse-index arrays in lock-step
        // with the writer's keys[] capacity. This branch is rare (grows
        // are amortised O(log N)); on the common path it is a single
        // length compare. Replaces the earlier setOnKeysGrowHook +
        // ensureReverseIndicesCapacity machinery (no callback to install
        // because only the writer ever grows the keys array, and the
        // writer drives addToIndex itself).
        final int keysLen = triples.getInternalKeysLength();
        if (sReverseIndices.length < keysLen) {
            sReverseIndices = Arrays.copyOf(sReverseIndices, keysLen);
            pReverseIndices = Arrays.copyOf(pReverseIndices, keysLen);
            oReverseIndices = Arrays.copyOf(oReverseIndices, keysLen);
        }
        sReverseIndices[index] = ensureWritableList(subjectIndex, t.getSubject()).add(index);
        pReverseIndices[index] = ensureWritableList(predicateIndex, t.getPredicate()).add(index);
        oReverseIndices[index] = ensureWritableList(objectIndex, t.getObject()).add(index);
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
     * Ownership is read from the spine's per-slot
     * {@link TxnNodesToIndices#isValueOwnedByThisWriter writer-owned}
     * bitmap: a set bit means the slot's value was placed by this writer
     * (and is therefore safe to mutate in place); a clear bit means the
     * value is still shared with a snapshot and must be cloned. The
     * clone is re-installed via {@link TxnNodesToIndices#put} (the COW
     * tombstone-and-append), which automatically marks the new slot
     * writer-owned.
     */
    private IndexList ensureWritableList(TxnNodesToIndices spine, Node node) {
        final int eIndex = spine.indexOf(node);
        if (eIndex < 0) {
            final IndexList list = new IndexList();
            spine.put(node, list);              // marks new slot writer-owned
            return list;
        }
        if (spine.isValueOwnedByThisWriter(eIndex)) {
            return spine.getValueAt(eIndex);    // already writer-owned
        }
        // Shared with snapshot — clone before mutation.
        final IndexList forked = spine.getValueAt(eIndex).clone();
        spine.put(node, forked);                // tombstone-and-append on the spine
        return forked;
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

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
import java.util.function.Function;
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
 * <h2>Reverse-index growth</h2>
 * The three reverse-index arrays must stay sized at least as large as
 * {@code triples.getInternalKeysLength()} so any newly added triple's
 * stable index is in-range. They grow via a writer-private callback
 * installed on the triple set ({@link TxnTripleSet#setOnKeysGrowHook}):
 * when {@code triples.keys[]} grows, the hook fires and resizes the
 * three reverse-index arrays in lock-step. {@link #addToIndex} can
 * therefore index without a length check on the hot path.
 * <p>
 * The hook is installed only by writer-side construction paths (the
 * sequential and parallel fork constructors below, plus
 * {@link CowWriteTxn#initializeIndex()}, {@link CowWriteTxn#initializeIndexParallel()},
 * {@link CowWriteTxn#resetIndexStrategy()}, and the writer-side branch
 * of the LAZY auto-upgrade). Snapshot-side construction paths skip
 * the hook because the snapshot's triples never grow and concurrent
 * installs from multiple readers would race on the (plain) field.
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
     * any triples already present (sequentially). No grow hook
     * installed — caller must use the three-arg overload to opt in.
     */
    public CowEagerStoreStrategy(TxnTripleSet triples) {
        this(triples, false, false);
    }

    /**
     * Build an empty eager index over the given triple set, then index
     * any triples already present. No grow hook installed — caller must
     * use the three-arg overload to opt in.
     *
     * @param triples  the canonical triple set
     * @param parallel if {@code true}, populate the three indices
     *                 concurrently (used by the LAZY-parallel auto-build
     *                 and by the writer-side initializeIndexParallel hook)
     */
    public CowEagerStoreStrategy(TxnTripleSet triples, boolean parallel) {
        this(triples, parallel, false);
    }

    /**
     * Full constructor.
     *
     * @param triples         the canonical triple set
     * @param parallel        if {@code true}, populate the three indices
     *                        concurrently
     * @param installGrowHook whether to register the keys-grow callback
     *                        on {@code triples}. Pass {@code true} from
     *                        writer-side paths so the reverse-index
     *                        arrays grow in lock-step with
     *                        {@code triples.keys[]} and {@link #addToIndex}
     *                        can skip a length check on the hot path.
     *                        Pass {@code false} from snapshot-side paths
     *                        (most importantly the LAZY → EAGER
     *                        auto-upgrade race on a published snapshot,
     *                        where multiple readers would otherwise
     *                        race-write the (plain) hook field on the
     *                        shared triple set). A snapshot's keys
     *                        never grow, so the hook would never fire
     *                        anyway.
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
        if (!installGrowHook) {
            // Snapshot-side construction (e.g. the LAZY auto-upgrade
            // race on a published snapshot, or a brand-new empty
            // CowSnapshot configured EAGER). The build phase needs
            // the writer-owned bitmap so insertAt can stamp slots,
            // but the resulting strategy only ever serves reads, so
            // the bitmaps are released here.
            freeWriterOwnedBitmaps();
        }
    }

    /**
     * Fork constructor — used by {@link #fork(CowWriteTxn)}. Forks each
     * spine and clones each reverse-index array sequentially, and
     * installs the writer-side grow hook on the new triple set so the
     * reverse-index arrays grow in lock-step with future writes. Every
     * {@link IndexList} inherited from the source is "not writer-owned"
     * until {@link #ensureWritableList} clones it on first mutation,
     * because the spines' {@code valueOwnedByThisWriter} bitmaps start
     * fresh (all-clear) on fork.
     */
    private CowEagerStoreStrategy(TxnTripleSet newTriples, CowEagerStoreStrategy source) {
        this(newTriples,
                source.subjectIndex.fork(),
                source.predicateIndex.fork(),
                source.objectIndex.fork(),
                source.sReverseIndices.clone(),
                source.pReverseIndices.clone(),
                source.oReverseIndices.clone());
    }

    /**
     * Assemble-from-parts constructor used by {@link #prepareParallelFork}:
     * the caller has produced the six writer-private allocations on its
     * own threads (typically the common fork-join pool) and joined the
     * results before invoking this constructor. Installs the writer-
     * side grow hook on the new triple set. The fork-side ownership
     * invariants are exactly the same as for the sequential
     * {@linkplain #CowEagerStoreStrategy(TxnTripleSet, CowEagerStoreStrategy)
     * fork constructor} — every spine's writer-owned bitmap starts
     * all-clear, so every inherited {@link IndexList} is
     * clone-on-first-touch.
     */
    private CowEagerStoreStrategy(TxnTripleSet newTriples,
                                  TxnNodesToIndices subjectIndex,
                                  TxnNodesToIndices predicateIndex,
                                  TxnNodesToIndices objectIndex,
                                  int[] sReverseIndices,
                                  int[] pReverseIndices,
                                  int[] oReverseIndices) {
        this.triples = newTriples;
        this.subjectIndex = subjectIndex;
        this.predicateIndex = predicateIndex;
        this.objectIndex = objectIndex;
        this.sReverseIndices = sReverseIndices;
        this.pReverseIndices = pReverseIndices;
        this.oReverseIndices = oReverseIndices;
        // The fork is always a writer; install the grow hook so
        // addToIndex can skip the length check on the hot path.
        newTriples.setOnKeysGrowHook(this::onTriplesKeysGrew);
    }

    @Override
    public CowStoreStrategy fork(CowWriteTxn newWriteTxn) {
        return new CowEagerStoreStrategy(newWriteTxn.getTriples(), this);
    }

    /**
     * Two-phase parallel-fork dispatcher. <b>Phase 1</b>: dispatch the
     * three spine forks and three reverse-index clones to the common
     * fork-join pool and capture the futures in the returned
     * assembler. Returns immediately so the snapshot can launch its
     * own {@link TxnTripleSet#fork()} concurrently.
     * <p>
     * <b>Phase 2</b>: when the snapshot invokes the assembler with the
     * freshly forked write transaction, join the six futures and build
     * the strategy. By this point the snapshot has already joined its
     * own triples fork (it had to, to construct the write txn), so the
     * spine/reverse-array work has been fully overlapped with the
     * triples allocation — seven independent allocations in total.
     * <p>
     * Each {@code supplyAsync} task only reads from {@code this}
     * (whose state is by fork-time discipline frozen) and writes to a
     * fresh allocation, so no inter-task synchronisation is required.
     */
    @Override
    public Function<CowWriteTxn, CowStoreStrategy> prepareParallelFork() {
        final CompletableFuture<TxnNodesToIndices> fSubj =
                CompletableFuture.supplyAsync(this.subjectIndex::fork);
        final CompletableFuture<TxnNodesToIndices> fPred =
                CompletableFuture.supplyAsync(this.predicateIndex::fork);
        final CompletableFuture<TxnNodesToIndices> fObj =
                CompletableFuture.supplyAsync(this.objectIndex::fork);
        final CompletableFuture<int[]> fS =
                CompletableFuture.supplyAsync(this.sReverseIndices::clone);
        final CompletableFuture<int[]> fP =
                CompletableFuture.supplyAsync(this.pReverseIndices::clone);
        final CompletableFuture<int[]> fO =
                CompletableFuture.supplyAsync(this.oReverseIndices::clone);
        return newWriteTxn -> new CowEagerStoreStrategy(
                newWriteTxn.getTriples(),
                fSubj.join(), fPred.join(), fObj.join(),
                fS.join(),    fP.join(),    fO.join());
    }

    @Override
    public boolean isIndexInitialized() {
        return true;
    }

    // ----- Index maintenance -----------------------------------------

    @Override
    public void addToIndex(Triple t, int index) {
        // No length check on the hot path — the writer-side
        // setOnKeysGrowHook installed at fork time fires inside
        // triples.addAndGetIndex() *before* this method runs, so the
        // three reverse-index arrays are guaranteed to cover `index`.
        sReverseIndices[index] = ensureWritableList(subjectIndex, t.getSubject()).add(index);
        pReverseIndices[index] = ensureWritableList(predicateIndex, t.getPredicate()).add(index);
        oReverseIndices[index] = ensureWritableList(objectIndex, t.getObject()).add(index);
    }

    /** Resize the reverse-index arrays whenever {@code triples.keys[]} grows. */
    private void onTriplesKeysGrew(int newKeysLength) {
        sReverseIndices = Arrays.copyOf(sReverseIndices, newKeysLength);
        pReverseIndices = Arrays.copyOf(pReverseIndices, newKeysLength);
        oReverseIndices = Arrays.copyOf(oReverseIndices, newKeysLength);
    }

    /**
     * Release each spine's writer-only ownership bitmap. Called from
     * the snapshot-side branch of the constructor and from
     * {@link org.apache.jena.mem.store.cow.CowWriteTxn#freeze()} so
     * that snapshots don't carry the writer-only tracking arrays
     * past their useful life. Idempotent.
     */
    public void freeWriterOwnedBitmaps() {
        subjectIndex.freeWriterOwnedBitmap();
        predicateIndex.freeWriterOwnedBitmap();
        objectIndex.freeWriterOwnedBitmap();
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

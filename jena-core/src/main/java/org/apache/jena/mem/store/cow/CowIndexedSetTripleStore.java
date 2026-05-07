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

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.IndexingStrategy;
import org.apache.jena.mem.pattern.MatchPattern;
import org.apache.jena.mem.pattern.PatternClassifier;
import org.apache.jena.mem.store.TripleStore;
import org.apache.jena.mem.store.indexed.IndexList;
import org.apache.jena.mem.store.indexed.IndexListIterator;
import org.apache.jena.mem.store.indexed.IndexListSpliterator;
import org.apache.jena.mem.store.indexed.IndexListsIterator;
import org.apache.jena.mem.store.indexed.IndexListsSpliterator;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.NullIterator;
import org.apache.jena.util.iterator.SingletonIterator;

import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Copy-on-write {@link TripleStore} that backs the Phase B transactional
 * graph {@code GraphMemIndexedSetCowTxn}. "Cow" stands for <i>copy on
 * write</i>.
 *
 * <h2>Internal layout</h2>
 * Mirrors the structure of the baseline
 * {@link org.apache.jena.mem.store.indexed.IndexedSetTripleStore} with its
 * eager strategy:
 * <ul>
 *   <li>One {@link TxnTripleSet} of all triples; each triple has a stable
 *       integer index.
 *   <li>Three {@link TxnNodesToIndices} maps (subject, predicate, object)
 *       holding, per node, an {@link IndexList} of the triple indices that
 *       mention it in that slot.
 *   <li>Three parallel {@code int[]} reverse-index arrays giving, per
 *       triple slot, the position of that triple inside its
 *       subject/predicate/object {@link IndexList}; this is what makes
 *       removal {@code O(1)}.
 * </ul>
 *
 * <h2>Fork semantics</h2>
 * Phase B's cheap {@link #forkForWrite()} is what distinguishes this from
 * the deep-copy baseline:
 * <ul>
 *   <li>{@link TxnTripleSet} and {@link TxnNodesToIndices} are <b>forked</b>:
 *       their {@code keys[]}/{@code hashCodes[]}/{@code values[]} are shared
 *       with the source; their {@code positions[]}/{@code deleted[]} are
 *       per-fork copies.
 *   <li>The three reverse-index {@code int[]}s are <b>cloned</b>: the
 *       writer mutates them at arbitrary alive triple-indices on removal,
 *       so they cannot be shared with a snapshot.
 *   <li>The set of writer-owned {@link IndexList}s, {@link #myForks}, is
 *       fresh and empty: every list reachable through the spines is
 *       initially shared with the source, and is cloned on first touch.
 * </ul>
 * After {@link #forkForWrite()} returns, the source must be treated as
 * frozen — the transactional graph enforces this by writing
 * {@code published} exactly once per commit.
 *
 * <h2>Indexing strategy</h2>
 * Phase B currently supports only {@link IndexingStrategy#EAGER}. The eager
 * logic is inlined here rather than expressed through a strategy interface;
 * the abstraction is unnecessary for a single supported strategy.
 */
public class CowIndexedSetTripleStore implements TripleStore {

    private static final String UNSUPPORTED_PATTERN = "Unsupported pattern classifier: %s";

    /** The canonical set of triples; each entry has a stable integer index. */
    private final TxnTripleSet triples;

    /** Subject -> indices of triples whose subject matches. */
    private final TxnNodesToIndices subjectIndex;
    /** Predicate -> indices of triples whose predicate matches. */
    private final TxnNodesToIndices predicateIndex;
    /** Object -> indices of triples whose object matches. */
    private final TxnNodesToIndices objectIndex;

    /**
     * For triple index {@code t}, the position of {@code t} inside the
     * subject's {@link IndexList}. Same shape for predicate / object.
     * <p>
     * These arrays are <b>not shared</b> across forks — the writer mutates
     * them at arbitrary alive triple-indices when the swap-with-last on
     * removal moves another triple into the freed slot, so any snapshot
     * sharing them would see corrupted reverse positions.
     * <p>
     * Length is kept equal to {@code triples.getInternalKeysLength()} via
     * the grow hook below.
     */
    private int[] sReverseIndices;
    private int[] pReverseIndices;
    private int[] oReverseIndices;

    /**
     * Identity-tracked set of {@link IndexList}s this store owns
     * exclusively (i.e. has cloned during the current write transaction).
     * Used by {@link #ensureWritableList} to perform clone-on-first-touch.
     * <p>
     * After commit (when this store becomes the published snapshot for
     * future readers), the set is no longer consulted; future forks start
     * with a fresh empty {@code myForks} and re-clone on their first
     * touch of any list.
     */
    private final Set<IndexList> myForks =
            Collections.newSetFromMap(new IdentityHashMap<>());

    // -------------------------------------------------------------------

    /** Creates an empty store using {@link IndexingStrategy#EAGER}. */
    public CowIndexedSetTripleStore() {
        this(IndexingStrategy.EAGER);
    }

    /**
     * Creates an empty store using the given indexing strategy.
     * <p>
     * Phase B currently supports only {@link IndexingStrategy#EAGER}.
     */
    public CowIndexedSetTripleStore(IndexingStrategy indexingStrategy) {
        if (indexingStrategy != IndexingStrategy.EAGER) {
            throw new IllegalArgumentException(
                    "CowIndexedSetTripleStore currently supports only EAGER indexing; got " + indexingStrategy);
        }
        this.triples = new TxnTripleSet();
        this.subjectIndex = new TxnNodesToIndices();
        this.predicateIndex = new TxnNodesToIndices();
        this.objectIndex = new TxnNodesToIndices();
        final int len = triples.getInternalKeysLength();
        this.sReverseIndices = new int[len];
        this.pReverseIndices = new int[len];
        this.oReverseIndices = new int[len];
        // Wire the grow hook so the reverse-index arrays grow in lock-step
        // with the writer's keys[] array. The hook is writer-private — never
        // fires on a published snapshot.
        this.triples.setOnKeysGrowHook(this::onTriplesKeysGrew);
    }

    /**
     * Fork constructor — the cheap COW path. Shares spine arrays via
     * {@link TxnTripleSet#fork()} and {@link TxnNodesToIndices#fork()};
     * full-clones the three reverse-index arrays; starts {@link #myForks}
     * empty.
     */
    private CowIndexedSetTripleStore(CowIndexedSetTripleStore source) {
        this.triples = source.triples.fork();
        this.subjectIndex = source.subjectIndex.fork();
        this.predicateIndex = source.predicateIndex.fork();
        this.objectIndex = source.objectIndex.fork();
        this.sReverseIndices = source.sReverseIndices.clone();
        this.pReverseIndices = source.pReverseIndices.clone();
        this.oReverseIndices = source.oReverseIndices.clone();
        // Hook routes grow events to THIS instance's reverse-index arrays.
        this.triples.setOnKeysGrowHook(this::onTriplesKeysGrew);
    }

    /**
     * Fork this store for a write transaction. See class doc.
     */
    public CowIndexedSetTripleStore forkForWrite() {
        return new CowIndexedSetTripleStore(this);
    }

    @Override
    public CowIndexedSetTripleStore copy() {
        // Phase B's Copyable contract is satisfied via fork: the source is
        // treated as frozen after the call. Truly independent mutation of
        // both copies is not in the Phase B hot path.
        return forkForWrite();
    }

    // ----- Mutation ---------------------------------------------------

    @Override
    public void add(Triple t) {
        final int idx = triples.addAndGetIndex(t);
        if (idx < 0) {
            return;                                // already present
        }
        addToIndex(t, idx);
    }

    @Override
    public void remove(Triple t) {
        final int idx = triples.removeAndGetIndex(t);
        if (idx < 0) {
            return;                                // not present
        }
        removeFromIndex(t, idx);
    }

    @Override
    public void clear() {
        triples.clear();
        subjectIndex.clear();
        predicateIndex.clear();
        objectIndex.clear();
        final int len = triples.getInternalKeysLength();
        sReverseIndices = new int[len];
        pReverseIndices = new int[len];
        oReverseIndices = new int[len];
        myForks.clear();
        // Reinstall the grow hook — TxnTripleSet#clear allocates fresh
        // arrays, but the hook reference itself is preserved (it's a
        // setter-installed field). No-op below; documented for clarity.
    }

    private void addToIndex(Triple t, int idx) {
        sReverseIndices[idx] = ensureWritableList(subjectIndex, t.getSubject()).add(idx);
        pReverseIndices[idx] = ensureWritableList(predicateIndex, t.getPredicate()).add(idx);
        oReverseIndices[idx] = ensureWritableList(objectIndex, t.getObject()).add(idx);
    }

    private void removeFromIndex(Triple t, int idx) {
        removeFromComponent(subjectIndex, t.getSubject(), idx, sReverseIndices);
        removeFromComponent(predicateIndex, t.getPredicate(), idx, pReverseIndices);
        removeFromComponent(objectIndex, t.getObject(), idx, oReverseIndices);
    }

    /**
     * Remove triple-index {@code idx} from the {@link IndexList} stored at
     * {@code spine.get(node)}. Updates the reverse-index array to track
     * the swap-with-last move when applicable.
     */
    private void removeFromComponent(TxnNodesToIndices spine, Node node, int idx, int[] reverse) {
        final IndexList list = ensureWritableList(spine, node);
        final int oldPos = reverse[idx];
        final int switched = list.removeAt(oldPos);
        if (list.isEmpty()) {
            // Drop the spine entry entirely. The (now empty) list reference
            // remains in shared values[] until the next grow reaps it; the
            // writer's deleted[] keeps the snapshot's view intact.
            spine.removeUnchecked(node);
        } else if (switched != -1) {
            // The swap-with-last moved another triple-index into oldPos.
            reverse[switched] = oldPos;
        }
    }

    /**
     * Return the {@link IndexList} stored at {@code spine.get(node)},
     * cloned and re-installed if it's still shared with the snapshot.
     * The returned list is in {@link #myForks} and may be freely mutated.
     * <p>
     * Three cases:
     * <ol>
     *   <li><b>Absent</b>: create a fresh empty {@link IndexList}, install
     *       it via {@code spine.put}, register it as writer-owned.
     *   <li><b>Already in {@code myForks}</b>: return as-is (already cloned
     *       earlier in this transaction).
     *   <li><b>Shared</b>: clone, install via {@code spine.put} (which is
     *       a tombstone-and-append in {@link TxnNodesToIndices}), register
     *       the clone, return the clone.
     * </ol>
     */
    private IndexList ensureWritableList(TxnNodesToIndices spine, Node node) {
        IndexList list = spine.get(node);
        if (list == null) {
            list = new IndexList();
            spine.put(node, list);
            myForks.add(list);
            return list;
        }
        if (myForks.contains(list)) {
            return list;
        }
        // Shared with snapshot — clone before mutation.
        final IndexList forked = list.clone();
        spine.put(node, forked);                   // tombstone-and-append
        myForks.add(forked);
        return forked;
    }

    /** Grow the reverse-index arrays whenever the writer's triples.keys[] grows. */
    private void onTriplesKeysGrew(int newKeysLength) {
        sReverseIndices = Arrays.copyOf(sReverseIndices, newKeysLength);
        pReverseIndices = Arrays.copyOf(pReverseIndices, newKeysLength);
        oReverseIndices = Arrays.copyOf(oReverseIndices, newKeysLength);
    }

    // ----- Read -------------------------------------------------------

    @Override
    public int countTriples() {
        return triples.size();
    }

    @Override
    public boolean isEmpty() {
        return triples.isEmpty();
    }

    @Override
    public boolean contains(Triple match) {
        final MatchPattern pattern = PatternClassifier.classify(match);
        return switch (pattern) {
            case SUB_PRE_OBJ -> triples.containsKey(match);
            case ANY_ANY_ANY -> !isEmpty();
            case SUB_ANY_ANY -> subjectIndex.containsKey(match.getSubject());
            case ANY_PRE_ANY -> predicateIndex.containsKey(match.getPredicate());
            case ANY_ANY_OBJ -> objectIndex.containsKey(match.getObject());
            case SUB_PRE_ANY -> bothIndexed(subjectIndex, match.getSubject(), sReverseIndices,
                                            predicateIndex, match.getPredicate(), pReverseIndices);
            case ANY_PRE_OBJ -> bothIndexed(predicateIndex, match.getPredicate(), pReverseIndices,
                                            objectIndex, match.getObject(), oReverseIndices);
            case SUB_ANY_OBJ -> bothIndexed(subjectIndex, match.getSubject(), sReverseIndices,
                                            objectIndex, match.getObject(), oReverseIndices);
        };
    }

    private boolean bothIndexed(TxnNodesToIndices a, Node aKey, int[] aReverse,
                                TxnNodesToIndices b, Node bKey, int[] bReverse) {
        final IndexList aList = a.get(aKey);
        if (aList == null) return false;
        final IndexList bList = b.get(bKey);
        if (bList == null) return false;
        return IndexList.intersects(aList, aReverse, bList, bReverse);
    }

    @Override
    public Stream<Triple> stream() {
        return triples.keyStream();
    }

    @Override
    public Stream<Triple> stream(Triple match) {
        final MatchPattern pattern = PatternClassifier.classify(match);
        return switch (pattern) {
            case ANY_ANY_ANY -> stream();
            case SUB_PRE_OBJ -> triples.containsKey(match) ? Stream.of(match) : Stream.empty();
            case SUB_ANY_ANY -> singleListStream(subjectIndex, match.getSubject());
            case ANY_PRE_ANY -> singleListStream(predicateIndex, match.getPredicate());
            case ANY_ANY_OBJ -> singleListStream(objectIndex, match.getObject());
            case SUB_PRE_ANY -> intersectStream(subjectIndex, match.getSubject(), sReverseIndices,
                                                predicateIndex, match.getPredicate(), pReverseIndices);
            case ANY_PRE_OBJ -> intersectStream(predicateIndex, match.getPredicate(), pReverseIndices,
                                                objectIndex, match.getObject(), oReverseIndices);
            case SUB_ANY_OBJ -> intersectStream(subjectIndex, match.getSubject(), sReverseIndices,
                                                objectIndex, match.getObject(), oReverseIndices);
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
                new IndexListsSpliterator(triples, aList, aReverse, bList, bReverse),
                false);
    }

    @Override
    public ExtendedIterator<Triple> find(Triple match) {
        final MatchPattern pattern = PatternClassifier.classify(match);
        return switch (pattern) {
            case ANY_ANY_ANY -> triples.keyIterator();
            case SUB_PRE_OBJ -> triples.containsKey(match)
                    ? new SingletonIterator<>(match) : NiceIterator.emptyIterator();
            case SUB_ANY_ANY -> singleListFind(subjectIndex, match.getSubject());
            case ANY_PRE_ANY -> singleListFind(predicateIndex, match.getPredicate());
            case ANY_ANY_OBJ -> singleListFind(objectIndex, match.getObject());
            case SUB_PRE_ANY -> intersectFind(subjectIndex, match.getSubject(), sReverseIndices,
                                              predicateIndex, match.getPredicate(), pReverseIndices);
            case ANY_PRE_OBJ -> intersectFind(predicateIndex, match.getPredicate(), pReverseIndices,
                                              objectIndex, match.getObject(), oReverseIndices);
            case SUB_ANY_OBJ -> intersectFind(subjectIndex, match.getSubject(), sReverseIndices,
                                              objectIndex, match.getObject(), oReverseIndices);
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

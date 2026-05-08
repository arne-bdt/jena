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
import org.apache.jena.mem.pattern.MatchPattern;
import org.apache.jena.mem.pattern.PatternClassifier;
import org.apache.jena.mem.store.TripleStore;
import org.apache.jena.mem.store.cow.strategies.CowEagerStoreStrategy;
import org.apache.jena.mem.store.cow.strategies.CowLazyStoreStrategy;
import org.apache.jena.mem.store.cow.strategies.CowManualStoreStrategy;
import org.apache.jena.mem.store.cow.strategies.CowMinimalStoreStrategy;
import org.apache.jena.mem.store.cow.strategies.CowStoreStrategy;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.SingletonIterator;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * Copy-on-write {@link TripleStore} that backs the Phase B transactional
 * graph {@code GraphMemIndexedSetCowTxn}. "Cow" stands for <i>copy on
 * write</i>.
 *
 * <h2>Internal layout</h2>
 * <ul>
 *   <li>{@link TxnTripleSet} {@code triples}: the canonical triple set;
 *       each entry has a stable integer index.
 *   <li>{@link AtomicReference}{@code <}{@link CowStoreStrategy}{@code >}
 *       {@code strategy}: pluggable strategy controlling whether (and how)
 *       a subject/predicate/object index is maintained, and how partial-
 *       pattern lookups are evaluated. The reference is mutable so that:
 *       <ul>
 *         <li>{@code LAZY}/{@code LAZY_PARALLEL} can atomically install a
 *             freshly-built {@link CowEagerStoreStrategy} on first lookup
 *             (CAS, so concurrent readers race-build harmlessly);
 *         <li>{@link #initializeIndex} / {@link #initializeIndexParallel}
 *             can install an eager strategy from outside;
 *         <li>{@link #resetIndexStrategy} can revert to the original
 *             configured strategy.
 *       </ul>
 * </ul>
 *
 * <h2>Fork semantics</h2>
 * {@link #forkForWrite()} forks the triples (cheap COW) and asks the
 * current strategy to {@linkplain CowStoreStrategy#fork(CowIndexedSetTripleStore)
 * fork itself} into the new store. For non-eager strategies this is
 * essentially free; for eager it forks the spines and clones the three
 * reverse-index arrays. The new fork's eager strategy is stamped with a
 * fresh per-instance {@code ownerId}; any
 * {@link org.apache.jena.mem.store.indexed.IndexList} shared with the
 * source is cloned on first mutation by
 * {@link org.apache.jena.mem.store.cow.strategies.CowEagerStoreStrategy
 * #ensureWritableList} (which compares the list's stamped id against the
 * fork's id).
 *
 * <h2>Indexing strategies</h2>
 * All five baseline strategies are supported:
 * <ul>
 *   <li>{@link IndexingStrategy#EAGER}: index built immediately, kept
 *       up-to-date on every add/remove.
 *   <li>{@link IndexingStrategy#LAZY}: no index until first pattern
 *       lookup; sequential build then.
 *   <li>{@link IndexingStrategy#LAZY_PARALLEL}: same, but parallel build.
 *   <li>{@link IndexingStrategy#MANUAL}: never builds automatically;
 *       pattern lookups throw until {@link #initializeIndex} is called.
 *   <li>{@link IndexingStrategy#MINIMAL}: never builds; pattern lookups
 *       linearly scan the triple set.
 * </ul>
 */
public class CowIndexedSetTripleStore implements TripleStore {

    /** The canonical set of triples; each entry has a stable integer index. */
    private final TxnTripleSet triples;

    /** The configured initial strategy; {@link #resetIndexStrategy} reverts to this. */
    private final IndexingStrategy initialStrategy;

    /**
     * The current strategy for index maintenance and partial-pattern
     * lookups. Atomic to support a lock-free LAZY → EAGER first-lookup
     * transition on a published snapshot held by multiple readers.
     */
    private final AtomicReference<CowStoreStrategy> strategy;

    /**
     * True if the strategy reference has been mutated since this store
     * was constructed (or forked). Used by the transactional graph at
     * commit time to decide whether a write transaction's strategy
     * upgrade is worth publishing even if the triple set itself was not
     * mutated.
     * <p>
     * Set under the same path that mutates {@link #strategy}: a
     * successful {@link #tryInstallEagerStrategy} CAS,
     * {@link #initializeIndex}, {@link #initializeIndexParallel}, or
     * {@link #resetIndexStrategy}.
     */
    private volatile boolean strategyChanged = false;

    // -------------------------------------------------------------------

    /** Creates an empty store using {@link IndexingStrategy#EAGER}. */
    public CowIndexedSetTripleStore() {
        this(IndexingStrategy.EAGER);
    }

    /** Creates an empty store using the given indexing strategy. */
    public CowIndexedSetTripleStore(IndexingStrategy indexingStrategy) {
        this.triples = new TxnTripleSet();
        this.initialStrategy = indexingStrategy;
        this.strategy = new AtomicReference<>(buildInitialStrategy(indexingStrategy));
    }

    /** Build the strategy implementation for the given enum value. */
    private CowStoreStrategy buildInitialStrategy(IndexingStrategy s) {
        return switch (s) {
            case EAGER         -> new CowEagerStoreStrategy(triples, false);
            case LAZY          -> new CowLazyStoreStrategy(this, false);
            case LAZY_PARALLEL -> new CowLazyStoreStrategy(this, true);
            case MANUAL        -> CowManualStoreStrategy.INSTANCE;
            case MINIMAL       -> new CowMinimalStoreStrategy(triples);
        };
    }

    /** Fork constructor — see class doc. */
    private CowIndexedSetTripleStore(CowIndexedSetTripleStore source, Parts triplesParts) {
        this.triples = triplesParts.triples;
        this.initialStrategy = source.initialStrategy;
        // Read the source's strategy once and ask it to fork into us.
        // For LAZY: the new strategy is rebound to this store; the
        // source's installation status (still LAZY vs already upgraded
        // to EAGER) is reflected by which subclass `srcStrategy` is.
        final CowStoreStrategy srcStrategy = source.strategy.get();
        this.strategy = new AtomicReference<>(srcStrategy.fork(this));
    }

    /**
     * Bag used to feed the fork constructor with pre-allocated parts.
     * Lets the sequential and parallel fork paths share one constructor.
     */
    private record Parts(TxnTripleSet triples) {}

    // -------------------------------------------------------------------
    // Forking

    /** Cheap fork for a write transaction (sequential). */
    public CowIndexedSetTripleStore forkForWrite() {
        return new CowIndexedSetTripleStore(this, new Parts(this.triples.fork()));
    }

    /**
     * Parallel variant of {@link #forkForWrite()} for benchmarking.
     * <p>
     * For non-EAGER stores there is exactly one parallelisable allocation
     * here ({@code triples.fork}), so the parallel path is no faster than
     * the sequential one. For EAGER stores the strategy's own fork
     * (called from the fork constructor) does the bulk of the work and is
     * not parallelised here — keeping that allocation on the calling
     * thread guarantees the grow-hook installation order. The benchmark
     * value of this path is therefore in characterising the hand-off
     * cost; see the class doc.
     */
    public CowIndexedSetTripleStore forkForWriteParallel() {
        // The single allocation isn't worth a fork-join hop, but we keep
        // a CompletableFuture path here for parity with the prior API
        // shape so benchmarks comparing the two get a real CompletableFuture.
        CompletableFuture<TxnTripleSet> fTriples =
                CompletableFuture.supplyAsync(this.triples::fork);
        return new CowIndexedSetTripleStore(this, new Parts(fTriples.join()));
    }

    /**
     * {@inheritDoc}
     * <p>
     * <b>This is a fork, not a deep copy.</b> The source must not be
     * mutated after this call — only the returned instance is safe to
     * mutate. See the class Javadoc's "Fork semantics" section.
     */
    @Override
    public CowIndexedSetTripleStore copy() {
        return forkForWrite();
    }

    // -------------------------------------------------------------------
    // Strategy controls (writer-facing API)

    /** @return the indexing strategy this store was created with. */
    public IndexingStrategy getIndexingStrategy() {
        return initialStrategy;
    }

    /** @return whether the index is built and ready to serve pattern lookups. */
    public boolean isIndexInitialized() {
        return strategy.get().isIndexInitialized();
    }

    /**
     * Drop the current strategy and re-install a fresh one of the
     * configured {@link #getIndexingStrategy()} kind. Subsequent lookups
     * will follow the initial strategy's rules (e.g. lazy will
     * re-trigger an auto-build on the next lookup).
     * <p>
     * Intended to be called from a write transaction; mutates the
     * writer-private strategy slot.
     * <p>
     * Distinct from {@link CowStoreStrategy#clearIndex()}, which clears
     * a single strategy's internal state in place. This method
     * <i>replaces</i> the strategy reference instead.
     */
    public void resetIndexStrategy() {
        strategy.set(buildInitialStrategy(initialStrategy));
        strategyChanged = true;
    }

    /**
     * Build the eager index sequentially and install it as the current
     * strategy, regardless of how the store was originally configured.
     * After this call, {@link #isIndexInitialized()} is {@code true} and
     * pattern lookups go straight through the eager path.
     */
    public void initializeIndex() {
        strategy.set(new CowEagerStoreStrategy(triples, false));
        strategyChanged = true;
    }

    /**
     * Build the eager index in parallel and install it as the current
     * strategy. See {@link #initializeIndex()}.
     */
    public void initializeIndexParallel() {
        strategy.set(new CowEagerStoreStrategy(triples, true));
        strategyChanged = true;
    }

    /**
     * @return whether the strategy reference has been mutated since this
     * store was constructed or forked. Used by the transactional graph
     * to detect "the writer auto-built the index" and treat that as a
     * publish-worthy change at commit time even when no data was added
     * or removed.
     */
    public boolean wasStrategyChanged() {
        return strategyChanged;
    }

    // -------------------------------------------------------------------
    // Internal: lazy → eager atomic upgrade + accessors used by strategies

    /**
     * Accessor for use by strategy implementations (which need to hand
     * the same canonical triples to their fork() and to streaming
     * iterators).
     */
    public TxnTripleSet getTriples() {
        return triples;
    }

    /**
     * Atomic compare-and-set for the lazy → eager auto-upgrade.
     * Called from {@link CowLazyStoreStrategy} after it has built a
     * fresh eager strategy; if a concurrent caller has already won the
     * race (the strategy slot now holds a different value) this CAS
     * fails and the caller's eager build is GC'd.
     *
     * @param expected the lazy strategy that was current when the build
     *                 started; CAS only succeeds if the slot still holds
     *                 this exact reference
     * @param built    the freshly built eager strategy to install
     * @return true if the CAS succeeded
     */
    public boolean tryInstallEagerStrategy(CowLazyStoreStrategy expected,
                                           CowEagerStoreStrategy built) {
        if (strategy.compareAndSet(expected, built)) {
            strategyChanged = true;
            return true;
        }
        return false;
    }

    // -------------------------------------------------------------------
    // TripleStore: mutation

    @Override
    public void add(Triple t) {
        final int idx = triples.addAndGetIndex(t);
        if (idx < 0) return;                 // already present
        strategy.get().addToIndex(t, idx);
    }

    @Override
    public void remove(Triple t) {
        final int idx = triples.removeAndGetIndex(t);
        if (idx < 0) return;                 // not present
        strategy.get().removeFromIndex(t, idx);
    }

    @Override
    public void clear() {
        triples.clear();
        strategy.get().clearIndex();
    }

    // -------------------------------------------------------------------
    // TripleStore: read

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
            default -> strategy.get().containsMatch(match, pattern);
        };
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
            default -> strategy.get().streamMatch(match, pattern);
        };
    }

    @Override
    public ExtendedIterator<Triple> find(Triple match) {
        final MatchPattern pattern = PatternClassifier.classify(match);
        return switch (pattern) {
            case ANY_ANY_ANY -> triples.keyIterator();
            case SUB_PRE_OBJ -> triples.containsKey(match)
                    ? new SingletonIterator<>(match) : NiceIterator.emptyIterator();
            default -> strategy.get().findMatch(match, pattern);
        };
    }
}

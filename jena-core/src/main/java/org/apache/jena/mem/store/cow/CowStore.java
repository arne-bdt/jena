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
import org.apache.jena.mem.store.cow.strategies.CowEagerStoreStrategy;
import org.apache.jena.mem.store.cow.strategies.CowLazyStoreStrategy;
import org.apache.jena.mem.store.cow.strategies.CowManualStoreStrategy;
import org.apache.jena.mem.store.cow.strategies.CowMinimalStoreStrategy;
import org.apache.jena.mem.store.cow.strategies.CowStoreStrategy;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.SingletonIterator;

import java.util.stream.Stream;

/**
 * Common state and read-only operations shared by {@link CowSnapshot}
 * (the immutable view a graph publishes to readers) and
 * {@link CowWriteTxn} (the writer's working copy). The split exists so
 * the type system enforces the rule "snapshots are not mutated":
 * {@link CowSnapshot} simply does not expose {@code add} / {@code remove}
 * / {@code clear}.
 *
 * <h2>State held here</h2>
 * Both kinds carry the same internal layout:
 * <ul>
 *   <li>{@link #triples} — the canonical {@link TxnTripleSet} (each
 *       triple has a stable {@code int} index).
 *   <li>The current pluggable {@link CowStoreStrategy}. The slot
 *       lives in each subclass (not on this base) so the
 *       concurrency model can differ:
 *       <ul>
 *         <li>{@link CowSnapshot} stores it in a {@code volatile}
 *             field. Snapshots are read by any number of concurrent
 *             reader threads, and the LAZY → EAGER first-lookup
 *             auto-upgrade installs a freshly built eager strategy
 *             with a plain volatile write — any two equivalent
 *             eagers racing here are interchangeable, so
 *             last-writer-wins is sufficient.
 *         <li>{@link CowWriteTxn} stores it in a plain field — the
 *             slot is mutated only by the single writer thread (the
 *             graph's {@code writeLock} serialises writers), so no
 *             volatile read overhead is paid on the
 *             {@code add} / {@code remove} hot path.
 *       </ul>
 *   <li>{@link #initialStrategy} — the strategy enum value passed at
 *       construction; preserved across forks so a write-txn that
 *       {@code resetIndexStrategy()} reverts to the same kind, and so
 *       that {@code getIndexingStrategy()} returns a stable answer.
 * </ul>
 *
 * <h2>Lifecycle</h2>
 * A graph holds exactly one {@link CowSnapshot} at any time (the
 * "published" view). Beginning a write transaction calls
 * {@link CowSnapshot#forkForWrite()} or
 * {@link CowSnapshot#forkForWriteParallel()} to produce a private
 * {@link CowWriteTxn}; on commit, {@link CowWriteTxn#freeze()} hands
 * back a fresh {@link CowSnapshot} that the graph CASes into its
 * published slot.
 */
public abstract class CowStore {

    /** The canonical set of triples; each entry has a stable integer index. */
    protected final TxnTripleSet triples;

    /** The configured initial strategy; used to revert and to report. */
    protected final IndexingStrategy initialStrategy;

    // -------------------------------------------------------------------

    /**
     * Empty-store constructor.
     *
     * @param indexingStrategy the configured strategy enum
     */
    protected CowStore(IndexingStrategy indexingStrategy) {
        this.triples = new TxnTripleSet();
        this.initialStrategy = indexingStrategy;
    }

    /**
     * Internal constructor used by fork.
     */
    protected CowStore(IndexingStrategy initialStrategy, TxnTripleSet triples) {
        this.initialStrategy = initialStrategy;
        this.triples = triples;
    }

    /**
     * @return the current pluggable strategy. Subclasses provide the
     * concurrency model: {@link CowSnapshot} reads through an
     * {@link java.util.concurrent.atomic.AtomicReference} (volatile
     * semantics, supports the LAZY-upgrade race); {@link CowWriteTxn}
     * reads from a plain field (single-thread access).
     */
    protected abstract CowStoreStrategy currentStrategy();

    /**
     * Build the strategy implementation for the given enum value.
     * <p>
     * The {@code installGrowHook} flag controls whether the eager
     * strategy installs the writer-side keys-grow callback on the
     * triple set. Subclasses pass {@code true} from writer-side paths
     * ({@link CowWriteTxn}) so {@code addToIndex} can skip a length
     * check on the hot path; {@code false} from snapshot-side paths
     * ({@link CowSnapshot}'s initial empty construction, where the
     * triple set will never grow because snapshots are read-only) so
     * the (plain) hook field on the shared triple set is never
     * race-written.
     */
    protected final CowStoreStrategy buildInitialStrategy(IndexingStrategy s, boolean installGrowHook) {
        return switch (s) {
            case EAGER         -> new CowEagerStoreStrategy(triples, false, installGrowHook);
            case LAZY          -> new CowLazyStoreStrategy(this, false);
            case LAZY_PARALLEL -> new CowLazyStoreStrategy(this, true);
            case MANUAL        -> CowManualStoreStrategy.INSTANCE;
            case MINIMAL       -> new CowMinimalStoreStrategy(triples);
        };
    }

    // -------------------------------------------------------------------
    // Strategy / triples accessors used by strategy implementations

    /**
     * Accessor for use by strategy implementations (which need the same
     * canonical triples for fork() and for streaming iterators).
     */
    public final TxnTripleSet getTriples() {
        return triples;
    }

    /** @return the indexing strategy this store was created with. */
    public final IndexingStrategy getIndexingStrategy() {
        return initialStrategy;
    }

    /** @return whether the index is built and ready to serve lookups. */
    public final boolean isIndexInitialized() {
        return currentStrategy().isIndexInitialized();
    }

    /**
     * Install an eager strategy as the current strategy. Called from
     * the LAZY auto-upgrade path
     * ({@code CowLazyStoreStrategy.upgradeAndAnswer}).
     * <p>
     * No compare-and-set semantics: any two eager strategies racing to
     * install on a snapshot are <i>equivalent</i> (both are pure
     * functions of the same frozen triple set), so picking a "winner"
     * doesn't matter. The slot just gets the last writer's value and
     * subsequent reads return it; concurrent racers each return their
     * own freshly built eager to <i>their</i> caller, which is
     * consistent. A plain volatile write therefore suffices.
     * <p>
     * Subclass overrides:
     * <ul>
     *   <li>{@link CowSnapshot} writes its volatile field — published
     *       to every reader.
     *   <li>{@link CowWriteTxn} writes its plain field (single writer
     *       thread, no need for volatile) and flips
     *       {@code strategyChanged} so the graph's commit treats the
     *       upgrade as publish-worthy.
     * </ul>
     */
    public abstract void installEagerStrategy(CowEagerStoreStrategy built);

    // -------------------------------------------------------------------
    // Read API (shared by snapshot and write txn)

    /** @return the number of live triples. */
    public final int countTriples() {
        return triples.size();
    }

    /** @return {@code true} iff the store contains no triples. */
    public final boolean isEmpty() {
        return triples.isEmpty();
    }

    /** @return {@code true} iff some triple matches the given pattern. */
    public final boolean contains(Triple match) {
        final MatchPattern pattern = PatternClassifier.classify(match);
        return switch (pattern) {
            case SUB_PRE_OBJ -> triples.containsKey(match);
            case ANY_ANY_ANY -> !isEmpty();
            default -> currentStrategy().containsMatch(match, pattern);
        };
    }

    /** @return a stream over every triple in this store. */
    public final Stream<Triple> stream() {
        return triples.keyStream();
    }

    /** @return a stream over the triples matching the given pattern. */
    public final Stream<Triple> stream(Triple match) {
        final MatchPattern pattern = PatternClassifier.classify(match);
        return switch (pattern) {
            case ANY_ANY_ANY -> stream();
            case SUB_PRE_OBJ -> triples.containsKey(match) ? Stream.of(match) : Stream.empty();
            default -> currentStrategy().streamMatch(match, pattern);
        };
    }

    /** @return an iterator over the triples matching the given pattern. */
    public final ExtendedIterator<Triple> find(Triple match) {
        final MatchPattern pattern = PatternClassifier.classify(match);
        return switch (pattern) {
            case ANY_ANY_ANY -> triples.keyIterator();
            case SUB_PRE_OBJ -> triples.containsKey(match)
                    ? new SingletonIterator<>(match) : NiceIterator.emptyIterator();
            default -> currentStrategy().findMatch(match, pattern);
        };
    }
}

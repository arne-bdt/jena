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

import java.util.concurrent.atomic.AtomicReference;
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
 *   <li>{@link #strategy} — the current pluggable
 *       {@link CowStoreStrategy} that maintains the auxiliary index and
 *       answers partial-pattern lookups. The slot is atomic so the LAZY
 *       → EAGER first-lookup auto-upgrade can race-install a freshly
 *       built eager strategy without locking; this is safe even on a
 *       published snapshot because the build is a pure function of the
 *       (frozen) triple set.
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

    /**
     * The current pluggable strategy. Atomic to support the lock-free
     * LAZY → EAGER first-lookup auto-upgrade on a published snapshot
     * held by multiple concurrent readers.
     */
    protected final AtomicReference<CowStoreStrategy> strategy;

    // -------------------------------------------------------------------

    /**
     * Empty-store constructor.
     *
     * @param indexingStrategy the configured strategy enum
     * @param installGrowHook  whether the writer-side keys-grow hook
     *                         should be installed if the strategy is
     *                         eager. Subclasses pass their kind:
     *                         {@code true} for {@link CowWriteTxn},
     *                         {@code false} for {@link CowSnapshot}.
     */
    protected CowStore(IndexingStrategy indexingStrategy, boolean installGrowHook) {
        this.triples = new TxnTripleSet();
        this.initialStrategy = indexingStrategy;
        this.strategy = new AtomicReference<>(buildInitialStrategy(indexingStrategy, installGrowHook));
    }

    /**
     * Internal constructor used by fork — installs the given triples and
     * a fresh strategy slot whose initial value is supplied by the
     * caller (typically the source's strategy.fork(...) result).
     */
    protected CowStore(IndexingStrategy initialStrategy,
                       TxnTripleSet triples,
                       CowStoreStrategy strategy) {
        this.initialStrategy = initialStrategy;
        this.triples = triples;
        this.strategy = new AtomicReference<>(strategy);
    }

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
        return strategy.get().isIndexInitialized();
    }

    /**
     * Atomic compare-and-set for the lazy → eager auto-upgrade.
     * <p>
     * Called from {@link CowLazyStoreStrategy} after it has built a
     * fresh eager strategy. If a concurrent caller has already won the
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
        return strategy.compareAndSet(expected, built);
    }

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
            default -> strategy.get().containsMatch(match, pattern);
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
            default -> strategy.get().streamMatch(match, pattern);
        };
    }

    /** @return an iterator over the triples matching the given pattern. */
    public final ExtendedIterator<Triple> find(Triple match) {
        final MatchPattern pattern = PatternClassifier.classify(match);
        return switch (pattern) {
            case ANY_ANY_ANY -> triples.keyIterator();
            case SUB_PRE_OBJ -> triples.containsKey(match)
                    ? new SingletonIterator<>(match) : NiceIterator.emptyIterator();
            default -> strategy.get().findMatch(match, pattern);
        };
    }
}

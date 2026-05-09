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

import org.apache.jena.atlas.lib.Copyable;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.store.cow.collection.TxnFastHashSet;
import org.apache.jena.mem.store.indexed.IndexedTripleSource;

import java.util.function.IntConsumer;

/**
 * Copy-on-write twin of {@link org.apache.jena.mem.store.indexed.TripleSet}.
 * The canonical {@link Triple} collection inside the COW indexed store;
 * each triple has a stable {@code int} index that the strategy uses as
 * the key into its parallel reverse-index arrays.
 * <p>
 * Adds two pieces of API on top of {@link TxnFastHashSet}:
 * <ul>
 *   <li>{@link #setOnKeysGrowHook} — fires whenever {@code keys[]} grows,
 *       so dependent writer-private arrays (e.g. the eager strategy's
 *       {@code sReverseIndices} / {@code pReverseIndices} /
 *       {@code oReverseIndices}) can resize in lock-step. The hook is
 *       writer-private: under the snapshot/write-txn split
 *       ({@link CowSnapshot} vs {@link CowWriteTxn}) only a writer's
 *       triple set ever grows, so installing the hook is safe — at
 *       most one writer exists at a time. The hook is intentionally
 *       <i>not</i> installed by snapshot-side construction paths
 *       (e.g. the LAZY → EAGER auto-upgrade race on a published
 *       snapshot held by multiple readers); a snapshot's keys never
 *       grow, so the hook would never fire and the racey field write
 *       would be undefined.
 *   <li>{@link #getInternalKeysLength} — exposes the current capacity
 *       of the {@code keys} array, useful for callers that pre-size
 *       parallel arrays to match.
 * </ul>
 * Public surface intentionally matches {@code TripleSet} so the eager
 * strategy can be ported with minimal refactoring.
 */
public class TxnTripleSet
        extends TxnFastHashSet<Triple>
        implements Copyable<TxnTripleSet>, IndexedTripleSource {

    /**
     * Optional callback invoked by {@link #growKeysAndHashCodeArrays}
     * after the writer allocates a fresh {@code keys} array. Receives
     * the new {@code keys.length}. Plain field (not {@code volatile}):
     * only writer-side strategy construction paths install it, and
     * there is at most one writer at a time, so no race.
     */
    private IntConsumer onKeysGrowHook = null;

    public TxnTripleSet() {
        super();
    }

    /**
     * Fork constructor — see
     * {@link org.apache.jena.mem.store.cow.collection.TxnFastHashBase#TxnFastHashBase(
     * org.apache.jena.mem.store.cow.collection.TxnFastHashBase)}.
     * The grow hook is <i>not</i> propagated to the fork; the fork's
     * eager strategy installs its own (pointing at its own
     * reverse-index arrays).
     */
    private TxnTripleSet(final TxnTripleSet source) {
        super(source);
    }

    /**
     * Register a callback invoked after the writer's {@code keys}
     * array grows. The callback receives the new {@code keys.length}.
     * <p>
     * Discipline: only callable from writer-side construction paths
     * (the fork constructor of {@link CowWriteTxn}'s eager strategy,
     * the writer-only {@code initializeIndex}/{@code initializeIndexParallel},
     * and the writer-side branch of the LAZY auto-upgrade). Snapshot-
     * side construction paths must not install a hook because the
     * snapshot's triples never grow and concurrent installs from
     * multiple readers would race on this field.
     */
    public void setOnKeysGrowHook(IntConsumer onKeysGrowHook) {
        this.onKeysGrowHook = onKeysGrowHook;
    }

    @Override
    protected Triple[] newKeysArray(int size) {
        return new Triple[size];
    }

    @Override
    protected void growKeysAndHashCodeArrays() {
        super.growKeysAndHashCodeArrays();
        if (onKeysGrowHook != null) {
            onKeysGrowHook.accept(keys.length);
        }
    }

    /**
     * Cheap fork (the canonical Phase B path). Discipline: after this
     * returns, treat the source as frozen — only the fork is mutated.
     */
    public TxnTripleSet fork() {
        return new TxnTripleSet(this);
    }

    /**
     * {@inheritDoc}
     * <p>
     * <b>This is a fork, not a deep copy.</b> The source must not be
     * mutated after this call — only the returned instance is safe to
     * mutate. See {@link #fork()} and the class Javadoc.
     * <p>
     * If a future caller needs a truly independent deep copy, this is
     * the place to add it.
     */
    @Override
    public TxnTripleSet copy() {
        return fork();
    }

    /**
     * @return the current capacity of the {@code keys} array (one past
     * the largest index that may yet be allocated before a grow)
     */
    public int getInternalKeysLength() {
        return keys.length;
    }
}

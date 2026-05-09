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

package org.apache.jena.mem.store.cow.collection;

import org.apache.jena.mem.collection.JenaMapIndexed;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.Spliterator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * Copy-on-write twin of {@link org.apache.jena.mem.collection.FastHashMap}.
 * See {@link TxnFastHashBase} for the sharing and tombstone discipline.
 *
 * <h2>The update path: tombstone-and-append</h2>
 * The most subtle divergence from {@code FastHashMap} is how a put on an
 * <i>already-present</i> key is handled. The baseline simply overwrites
 * {@code values[entryIndex]}. The COW twin <b>cannot</b>: {@code values}
 * is shared with any open snapshot, and overwriting would change the
 * snapshot's view of the key's value.
 * <p>
 * Instead, an update <b>tombstones</b> the old slot via {@link #removeFrom}
 * (writer-private {@code deleted[oldEIndex] = true}, plus Algorithm-R on
 * the writer-private probe table) and then <b>appends</b> a new entry at
 * {@code keysPos++}. The snapshot's probe table still resolves the key to
 * the old slot, where it sees the old value through {@code values[]}; the
 * writer's probe table resolves to the new slot with the new value. Both
 * views remain self-consistent without any shared-array writes that could
 * cross the snapshot boundary.
 * <p>
 * Cost per update: one extra slot consumed (cleaned up at the next
 * {@code grow}); one extra Algorithm-R pass; same big-O as a remove + add.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public abstract class TxnFastHashMap<K, V> extends TxnFastHashBase<K> implements JenaMapIndexed<K, V> {

    /**
     * Parallel array to {@link #keys} holding the value for each entry.
     * Shared with any forked snapshot/working-copy, exactly like
     * {@code keys} and {@code hashCodes}. Tombstoned slots may still hold
     * the old value reference; liveness is checked via {@link #deleted}.
     */
    protected V[] values;

    /**
     * Per-slot, writer-private bitmap: {@code true} at index {@code i}
     * iff the value at {@code values[i]} was placed by <i>this writer</i>
     * (since fork). Lets the eager strategy decide whether the value
     * object stored at a slot is still shared with a snapshot (and must
     * be cloned-on-first-touch before in-place mutation) or is already
     * writer-owned.
     * <p>
     * Lifecycle:
     * <ul>
     *   <li>Allocated by the constructors (so the build phase of
     *       {@code CowEagerStoreStrategy.indexAllSequential/Parallel}
     *       can write through {@link #insertAt} without a hot-path
     *       null check).
     *   <li>Allocated fresh (all-clear) by the fork constructor — the
     *       new writer has, by construction, placed nothing yet.
     *   <li>Grown in lock-step with {@code values[]} by
     *       {@link #onKeysAndHashCodesGrown}; reset by {@link #clear()}.
     *   <li>Released by {@link #freeWriterOwnedBitmap()} once the
     *       containing spine is handed to a snapshot — snapshots don't
     *       write, so the bitmap is dead weight at that point. Called
     *       from {@link org.apache.jena.mem.store.cow.CowWriteTxn#freeze()}
     *       and from the snapshot-side branch of
     *       {@code CowEagerStoreStrategy}'s constructor (after the
     *       LAZY-upgrade build completes).
     * </ul>
     * Tombstoned slots' bits are never read (callers go through
     * {@link #findPosition}/{@link #containsKey} which skip dead
     * slots), so we don't bother clearing them on remove or update.
     */
    protected boolean[] valueOwnedByThisWriter;

    protected TxnFastHashMap(int initialSize) {
        super(initialSize);
        this.values = newValuesArray(keys.length);
        this.valueOwnedByThisWriter = new boolean[keys.length];
    }

    protected TxnFastHashMap() {
        super();
        this.values = newValuesArray(keys.length);
        this.valueOwnedByThisWriter = new boolean[keys.length];
    }

    /**
     * Fork constructor — see {@link TxnFastHashBase#TxnFastHashBase(TxnFastHashBase)}.
     * Shares {@code values} (in addition to {@code keys}/{@code hashCodes})
     * with the source. {@link #valueOwnedByThisWriter} is allocated fresh
     * (all-clear) — the new writer has, by construction, placed nothing
     * yet.
     */
    protected TxnFastHashMap(final TxnFastHashMap<K, V> source) {
        super(source);
        this.values = source.values;
        this.valueOwnedByThisWriter = new boolean[source.values.length];
    }

    /**
     * Release the writer-owned bitmap. Called once a spine has been
     * aliased into a {@link org.apache.jena.mem.store.cow.CowSnapshot}
     * — the snapshot doesn't write, so the bitmap is dead weight.
     * Reads do not consult the bitmap (it's a writer-only tracking
     * aid), so freeing it does not affect the snapshot.
     * <p>
     * If the spine is later re-forked for a new write transaction,
     * the {@linkplain #TxnFastHashMap(TxnFastHashMap) fork constructor}
     * allocates a fresh bitmap on the writer side — the source
     * snapshot's spine stays null.
     * <p>
     * Idempotent.
     */
    public void freeWriterOwnedBitmap() {
        valueOwnedByThisWriter = null;
    }

    /** Subclasses allocate their typed value array here. */
    protected abstract V[] newValuesArray(int size);

    @Override
    protected void onKeysAndHashCodesGrown(int oldLength) {
        // Grow values[] in lock-step with keys[]/hashCodes[]. Reap
        // tombstoned slots in the new array so dead V references can be
        // GC'd, mirroring the keys[] reap in
        // TxnFastHashBase#growKeysAndHashCodeArrays. The base class has
        // already installed the new keys/hashCodes/deleted arrays on the
        // instance, so we read this.keys.length / this.deleted directly.
        final V[] newValues = newValuesArray(this.keys.length);
        System.arraycopy(values, 0, newValues, 0, oldLength);
        for (int i = 0; i < keysPos; i++) {
            if (this.deleted[i]) {
                newValues[i] = null;
            }
        }
        this.values = newValues;
        // valueOwnedByThisWriter is writer-private. In writer-side
        // operation it's non-null and we preserve the bits across the
        // grow; the null guard handles the case where freeze has
        // already released it (snapshots don't grow so this path
        // shouldn't fire, but it costs nothing to be defensive).
        if (valueOwnedByThisWriter != null) {
            final boolean[] newOwned = new boolean[this.keys.length];
            System.arraycopy(valueOwnedByThisWriter, 0, newOwned, 0, oldLength);
            this.valueOwnedByThisWriter = newOwned;
        }
    }

    // Note: removeFrom is inherited unchanged from TxnFastHashBase. Unlike
    // FastHashMap, we deliberately do NOT null values[~positions[here]] —
    // values[] is shared with snapshots. Reaping happens later in
    // onKeysAndHashCodesGrown when the writer allocates fresh arrays.

    @Override
    public void clear() {
        super.clear();
        values = newValuesArray(keys.length);
        // clear() is reachable only via writer-side paths (CowWriteTxn
        // exposes it; CowSnapshot does not), so the bitmap is always
        // non-null here. Re-allocate to a fresh all-clear bitmap of
        // the new size.
        valueOwnedByThisWriter = new boolean[keys.length];
    }

    // ----- Insert / update --------------------------------------------

    @Override
    public boolean tryPut(K key, V value) {
        growPositionsArrayIfNeeded();
        final int hashCode = key.hashCode();
        final var pIndex = findPosition(key, hashCode);
        if (pIndex < 0) {
            insertAt(~pIndex, key, hashCode, value);
            return true;
        } else {
            updateExisting(pIndex, key, hashCode, value);
            return false;
        }
    }

    @Override
    public void put(K key, V value) {
        growPositionsArrayIfNeeded();
        final int hashCode = key.hashCode();
        final var pIndex = findPosition(key, hashCode);
        if (pIndex < 0) {
            insertAt(~pIndex, key, hashCode, value);
        } else {
            updateExisting(pIndex, key, hashCode, value);
        }
    }

    @Override
    public int putAndGetIndex(K key, V value) {
        growPositionsArrayIfNeeded();
        final int hashCode = key.hashCode();
        final var pIndex = findPosition(key, hashCode);
        if (pIndex < 0) {
            return insertAt(~pIndex, key, hashCode, value);
        } else {
            return updateExisting(pIndex, key, hashCode, value);
        }
    }

    /**
     * Insert a brand-new entry. {@code emptyPIndex} is a probe-table slot
     * known to be empty (i.e. {@code positions[emptyPIndex] == 0}).
     */
    private int insertAt(int emptyPIndex, K key, int hashCode, V value) {
        final var eIndex = getFreeKeyIndex();
        keys[eIndex] = key;
        hashCodes[eIndex] = hashCode;
        values[eIndex] = value;
        // Required: getFreeKeyIndex() may return a slot from the post-grow
        // freelist where deleted[i] is still true (carried over from the
        // old tombstone — growKeysAndHashCodeArrays arraycopies the old
        // deleted[] without clearing compacted bits).
        deleted[eIndex] = false;
        // This writer just placed the value; mark the slot writer-owned
        // so the eager strategy's clone-on-first-touch knows it can
        // mutate the value in place.
        valueOwnedByThisWriter[eIndex] = true;
        positions[emptyPIndex] = ~eIndex;
        return eIndex;
    }

    /**
     * Update an entry whose key is already present. Implements the COW
     * tombstone-and-append: tombstone the old slot (so any snapshot still
     * resolves to the old value via its own {@code positions}), then append
     * a fresh entry at {@code keysPos++} with the new value.
     * <p>
     * Returns the new entry index.
     */
    private int updateExisting(int pIndex, K key, int hashCode, V value) {
        // Tombstone the old slot. Note: removeFrom mutates only writer-
        // private state (deleted[oldEIndex]=true; Algorithm-R on positions).
        removeFrom(pIndex);

        // The probe table is now "key absent". Re-find the empty insertion
        // point along the (possibly shifted) probe chain.
        final var newPIndex = findPosition(key, hashCode);
        // findPosition must report absent here; if it ever didn't, we'd be
        // about to corrupt the table. The assert is silenced by the Java
        // compiler in release builds.
        assert newPIndex < 0 : "key unexpectedly present after removeFrom";
        return insertAt(~newPIndex, key, hashCode, value);
    }

    /**
     * @return the value at index {@code i}; bounds-and-liveness are not
     * checked. Caller must ensure {@code i} corresponds to a live slot.
     */
    public V getValueAt(int i) {
        return values[i];
    }

    /**
     * @return {@code true} iff the value at slot {@code eIndex} was placed
     * by <i>this writer</i> (since fork). Lets the COW eager strategy
     * decide whether the value object is still shared with a snapshot
     * (and must be cloned-on-first-touch before in-place mutation) or is
     * already writer-owned. See {@link #valueOwnedByThisWriter}.
     */
    public boolean isValueOwnedByThisWriter(int eIndex) {
        return valueOwnedByThisWriter[eIndex];
    }

    @Override
    public V get(K key) {
        var pIndex = findPosition(key, key.hashCode());
        if (pIndex < 0) {
            return null;
        } else {
            return values[~positions[pIndex]];
        }
    }

    @Override
    public V getOrDefault(K key, V defaultValue) {
        var pIndex = findPosition(key, key.hashCode());
        if (pIndex < 0) {
            return defaultValue;
        } else {
            return values[~positions[pIndex]];
        }
    }

    @Override
    public V computeIfAbsent(K key, Supplier<V> absentValueSupplier) {
        final int hashCode = key.hashCode();
        var pIndex = findPosition(key, hashCode);
        if (pIndex < 0) {
            // tryGrowPositionsArrayIfNeeded may resize positions[] and
            // therefore invalidate pIndex. If so, recompute the empty slot
            // along the new probe chain.
            if (tryGrowPositionsArrayIfNeeded()) {
                pIndex = ~findEmptySlotWithoutEqualityCheck(hashCode);
            }
            final var value = absentValueSupplier.get();
            insertAt(~pIndex, key, hashCode, value);
            return value;
        } else {
            return values[~positions[pIndex]];
        }
    }

    @Override
    public void compute(K key, UnaryOperator<V> valueProcessor) {
        final int hashCode = key.hashCode();
        var pIndex = findPosition(key, hashCode);
        if (pIndex < 0) {
            // No prior value.
            final var value = valueProcessor.apply(null);
            if (value == null)
                return;
            if (tryGrowPositionsArrayIfNeeded()) {
                pIndex = ~findEmptySlotWithoutEqualityCheck(hashCode);
            }
            insertAt(~pIndex, key, hashCode, value);
        } else {
            // Existing value. Tombstone-and-append on update;
            // tombstone-only on null result (i.e. remove).
            final var oldEIndex = ~positions[pIndex];
            final var newValue = valueProcessor.apply(values[oldEIndex]);
            if (newValue == null) {
                removeFrom(pIndex);
            } else {
                updateExisting(pIndex, key, hashCode, newValue);
            }
        }
    }

    // ----- Iteration over values --------------------------------------

    @Override
    public ExtendedIterator<V> valueIterator() {
        return new SparseTombstoneIterator<>(values, deleted, keysPos, this);
    }

    @Override
    public Spliterator<V> valueSpliterator() {
        return new SparseTombstoneSpliterator<>(values, deleted, keysPos, this);
    }
}

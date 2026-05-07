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

import org.apache.jena.mem.collection.JenaMapSetCommon;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.Spliterator;
import java.util.function.Predicate;

/**
 * Copy-on-write twin of {@link org.apache.jena.mem.collection.FastHashBase}.
 * The public surface and probe-table layout are intentionally identical so
 * the two can be read side by side; only the removal discipline and the
 * fork semantics differ.
 *
 * <h2>Sharing model</h2>
 * Each instance is either a <i>published snapshot</i> (treated as immutable
 * by the caller) or a <i>working copy</i> forked from a snapshot. Both kinds
 * are the same Java type; the discipline is enforced by the surrounding
 * transactional graph, not by the type system. After {@link #fork()}, the
 * caller must not mutate this (the source) instance.
 *
 * <p>Internal arrays split into <i>shared</i> and <i>writer-private</i>:
 * <ul>
 *   <li><b>Shared</b>: {@code keys}, {@code hashCodes}. Subclasses that hold
 *       an additional {@code values[]} (e.g. maps) keep it shared too.
 *   <li><b>Writer-private</b>: {@code positions} (probe table) and
 *       {@code deleted} (tombstone bitmap). The fork constructor copies
 *       these so the source's view stays stable even as the fork mutates.
 * </ul>
 *
 * <h2>Tombstone discipline</h2>
 * Liveness is encoded by an explicit {@link #deleted} bitmap, not by
 * {@code keys[i] == null}. The four invariants the writer must preserve:
 * <ol>
 *   <li><b>Append-only on shared spine arrays.</b> The writer never writes
 *       to {@code keys[i]} or {@code hashCodes[i]} for any {@code i < } the
 *       source snapshot's {@code keysPos}. New entries always go at
 *       {@code keysPos++} (a slot beyond every open snapshot's view).
 *   <li><b>No swap-with-last on shared arrays.</b> Removal does not permute
 *       elements in the dense array. Deletion only sets writer-private
 *       {@code deleted[i] = true} and runs Algorithm-R on the writer-private
 *       {@code positions} table.
 *   <li><b>Iteration skips by {@link #deleted}, never by null.</b> Because
 *       the writer never overwrites alive snapshot slots, a snapshot's
 *       {@code keys[i]} for a tombstoned slot still holds the (now-dead)
 *       reference; only {@code deleted[i]} marks it dead.
 *   <li><b>Freelist is reaped only at grow time.</b> Tombstoned slots are
 *       not reused mid-transaction (every insert appends at
 *       {@code keysPos++}). At a {@code grow} the writer allocates fresh
 *       arrays, copies the live slots, and (in the new arrays) nulls the
 *       tombstoned slots so their references can be GC'd. Old snapshots
 *       still point at the old arrays and are unaffected.
 * </ol>
 *
 * <p>This wastes one slot per remove until the next {@code grow}. The
 * trade-off is intentional: it eliminates an entire family of correctness
 * bugs that would otherwise require generation tagging or pin counting.
 *
 * @param <K> the type of the keys
 */
public abstract class TxnFastHashBase<K> implements JenaMapSetCommon<K> {

    /** Initial size of the {@link #positions} probe table. */
    protected static final int MINIMUM_HASHES_SIZE = 16;
    /** Initial size of the {@link #keys} / {@link #hashCodes} arrays. */
    protected static final int MINIMUM_ELEMENTS_SIZE = 8;

    // ---- Shared arrays (with any forked snapshot/working-copy) --------

    /**
     * Dense array of stored keys. Indices are stable: once a key is
     * inserted at index {@code i}, subsequent removes do <i>not</i> move
     * it; only mark it dead via {@link #deleted}. {@code keys[i]} may
     * therefore hold a tombstoned (logically dead) reference until the
     * next {@code grow} replaces this array. Liveness is determined by
     * {@code !deleted[i]}, never by {@code keys[i] != null}.
     */
    protected K[] keys;

    /**
     * Cached {@link Object#hashCode()} of the corresponding key, parallel
     * to {@link #keys}. Stale hashes for tombstoned slots are harmless:
     * the probe table {@link #positions} no longer points at those slots
     * after Algorithm-R, and iteration skips them via {@link #deleted}.
     */
    protected int[] hashCodes;

    // ---- Writer-private (each fork gets its own copy) ------------------

    /**
     * Probe table mapping a hash bucket to an entry index in {@link #keys}.
     * A slot's value is the bitwise complement ({@code ~}) of the entry
     * index; a value of {@code 0} marks an empty slot. After
     * {@link #removeFrom} the chain is repaired by Algorithm-R, so no
     * probe entry ever points to a tombstoned slot.
     */
    protected int[] positions;

    /**
     * Tombstone bitmap parallel to {@link #keys}. {@code deleted[i] == true}
     * means slot {@code i} is logically dead in this view.
     * <p>
     * Forking copies this array; subsequent mutations on the fork do not
     * affect the source's tombstone view, even though both share the same
     * {@link #keys} reference.
     */
    protected boolean[] deleted;

    // ---- Scalars (writer-private, copied per fork) --------------------

    /** High-water mark; one past the largest slot ever used. Iteration covers {@code [0, keysPos)}. */
    protected int keysPos = 0;

    /** Number of dead slots in {@code [0, keysPos)}. Live size = {@code keysPos - removedKeysCount}. */
    protected int removedKeysCount = 0;

    // -------------------------------------------------------------------

    protected TxnFastHashBase(final int initialSize) {
        var positionsSize = Integer.highestOneBit(initialSize << 1);
        if (positionsSize < initialSize << 1) {
            positionsSize <<= 1;
        }
        this.positions = new int[positionsSize];
        this.keys = newKeysArray(initialSize);
        this.hashCodes = new int[initialSize];
        this.deleted = new boolean[initialSize];
    }

    protected TxnFastHashBase() {
        this.positions = new int[MINIMUM_HASHES_SIZE];
        this.keys = newKeysArray(MINIMUM_ELEMENTS_SIZE);
        this.hashCodes = new int[MINIMUM_ELEMENTS_SIZE];
        this.deleted = new boolean[MINIMUM_ELEMENTS_SIZE];
    }

    /**
     * Fork constructor — the cheap copy-on-write entry point.
     * <p>
     * Shares {@code keys} and {@code hashCodes} with {@code source} (and
     * any subclass-managed {@code values}, via {@link #copySharedArraysFromFork}).
     * Copies the writer-private bookkeeping ({@code positions},
     * {@code deleted}) and scalars so subsequent mutations on this instance
     * do not corrupt the source's view.
     * <p>
     * <b>Discipline:</b> after this returns, the caller must treat
     * {@code source} as frozen — no further mutation methods may be called
     * on it. The shared {@code keys}/{@code hashCodes} are append-only
     * (new entries land at {@code keysPos++}, beyond {@code source.keysPos}),
     * and removes only flip the writer-private {@code deleted} bit, so the
     * source's view is preserved as long as this discipline holds.
     */
    protected <T extends TxnFastHashBase<K>> TxnFastHashBase(final T source) {
        // Shared (no copy).
        this.keys = source.keys;
        this.hashCodes = source.hashCodes;

        // Writer-private (copied).
        this.positions = source.positions.clone();
        this.deleted = source.deleted.clone();

        // Scalars.
        this.keysPos = source.keysPos;
        this.removedKeysCount = source.removedKeysCount;
    }

    /**
     * Subclasses allocate their typed key array here.
     */
    protected abstract K[] newKeysArray(int size);

    // ----- Probe table -------------------------------------------------

    protected final int calcStartIndexByHashCode(final int hashCode) {
        return hashCode & (positions.length - 1);
    }

    /**
     * @return the new probe-table size, or {@code -1} if no resize is needed
     */
    private int calcNewPositionsSize() {
        if (keysPos << 1 > positions.length) {
            final var newLength = positions.length << 1;
            return newLength < 0 ? Integer.MAX_VALUE : newLength;
        }
        return -1;
    }

    /**
     * Rebuild {@link #positions} from scratch at the given size by walking
     * the dense {@link #keys} array and re-inserting every <i>live</i>
     * entry. Liveness is checked via {@link #deleted} (not {@code != null}),
     * because tombstoned slots may still hold non-null references in
     * shared spine arrays.
     */
    private void fillPositionsArray(int newSize) {
        this.positions = new int[newSize];
        var pos = keysPos - 1;
        while (-1 < pos) {
            if (!deleted[pos]) {
                this.positions[findEmptySlotWithoutEqualityCheck(hashCodes[pos])] = ~pos;
            }
            pos--;
        }
    }

    protected final void growPositionsArrayIfNeeded() {
        final var newSize = calcNewPositionsSize();
        if (newSize < 0) {
            return;
        }
        fillPositionsArray(newSize);
    }

    protected final boolean tryGrowPositionsArrayIfNeeded() {
        final var newSize = calcNewPositionsSize();
        if (newSize < 0) {
            return false;
        }
        fillPositionsArray(newSize);
        return true;
    }

    // ----- Size / liveness ---------------------------------------------

    @Override
    public int size() {
        return keysPos - removedKeysCount;
    }

    @Override
    public final boolean isEmpty() {
        return this.size() == 0;
    }

    /**
     * Return the next slot index for a new entry.
     * <p>
     * Always {@code keysPos++}, never the freelist: reusing a tombstoned
     * slot mid-transaction would write into shared {@code keys}/
     * {@code hashCodes} at an index where some open snapshot still has
     * {@code deleted[i] == false}, which would expose the new value to
     * that snapshot. Tombstoned slots are physically reaped only at
     * {@code grow} time, inside the writer's freshly allocated (and
     * therefore unshared) arrays.
     */
    protected final int getFreeKeyIndex() {
        final int index = keysPos++;
        if (index == keys.length) {
            growKeysAndHashCodeArrays();
        }
        return index;
    }

    /**
     * Allocate fresh {@link #keys}, {@link #hashCodes}, and {@link #deleted}
     * arrays at 1.5× capacity, copy from the old arrays, then reap
     * tombstoned slots in the new arrays.
     * <p>
     * Reaping ({@code keys[i] = null}) happens in the new arrays, so the
     * old arrays — still referenced by any open snapshot — are untouched.
     * The writer-private {@code deleted} bit on a reaped slot stays
     * {@code true} (matching its logical state), and {@code positions} is
     * unchanged (entries for tombstoned slots were already cleared by
     * Algorithm-R at remove time).
     * <p>
     * Subclasses that hold an additional shared values array override
     * {@link #onKeysAndHashCodesGrown(K[], K[], int[], int[], int)} to
     * grow and reap their values array in lock-step.
     */
    protected void growKeysAndHashCodeArrays() {
        var newSize = (keys.length >> 1) + keys.length;
        if (newSize <= keys.length) {
            // Defensive: ensures growth on tiny capacities and saturates safely.
            newSize = keys.length + 1;
        }
        if (newSize < 0) {
            newSize = Integer.MAX_VALUE;
        }
        final K[] oldKeys = this.keys;
        final int[] oldHashCodes = this.hashCodes;
        final boolean[] oldDeleted = this.deleted;
        final int oldLength = oldKeys.length;

        final K[] newKeys = newKeysArray(newSize);
        System.arraycopy(oldKeys, 0, newKeys, 0, oldLength);
        final int[] newHashCodes = new int[newSize];
        System.arraycopy(oldHashCodes, 0, newHashCodes, 0, oldLength);
        final boolean[] newDeleted = new boolean[newSize];
        System.arraycopy(oldDeleted, 0, newDeleted, 0, oldLength);

        // Reap tombstoned slots in the *new* keys array so dead K objects
        // can be GC'd. Only iterate the slice that was actually populated
        // (entries beyond keysPos are uninitialised in either array).
        for (int i = 0; i < keysPos; i++) {
            if (newDeleted[i]) {
                newKeys[i] = null;
            }
        }

        this.keys = newKeys;
        this.hashCodes = newHashCodes;
        this.deleted = newDeleted;

        onKeysAndHashCodesGrown(oldKeys, newKeys, oldHashCodes, newHashCodes, oldLength);
    }

    /**
     * Hook called after {@link #growKeysAndHashCodeArrays} has installed
     * the new arrays. Subclasses (e.g. maps holding a parallel values
     * array) override this to grow their own shared arrays in lock-step.
     * <p>
     * The default does nothing.
     */
    protected void onKeysAndHashCodesGrown(K[] oldKeys, K[] newKeys,
                                           int[] oldHashCodes, int[] newHashCodes,
                                           int oldLength) {
        // no-op for set-shaped collections
    }

    // ----- Removal -----------------------------------------------------

    @Override
    public final boolean tryRemove(K key) {
        return tryRemove(key, key.hashCode());
    }

    public final boolean tryRemove(K key, int hashCode) {
        final var index = findPosition(key, hashCode);
        if (index < 0) {
            return false;
        }
        removeFrom(index);
        return true;
    }

    public final int removeAndGetIndex(final K key) {
        return removeAndGetIndex(key, key.hashCode());
    }

    public final int removeAndGetIndex(final K key, final int hashCode) {
        final var pIndex = findPosition(key, hashCode);
        if (pIndex < 0) {
            return -1;
        }
        final var eIndex = ~positions[pIndex];
        removeFrom(pIndex);
        return eIndex;
    }

    @Override
    public final void removeUnchecked(K key) {
        removeUnchecked(key, key.hashCode());
    }

    public final void removeUnchecked(K key, int hashCode) {
        removeFrom(findPosition(key, hashCode));
    }

    /**
     * Remove the entry referenced by {@code positions[here]}.
     * <p>
     * The COW discipline limits this to writer-private state. Concretely:
     * <ul>
     *   <li>set {@code deleted[entryIndex] = true} (writer-private bitmap),
     *   <li>increment {@code removedKeysCount},
     *   <li>run Algorithm-R on the writer-private {@code positions} table
     *       to repair the probe chain.
     * </ul>
     * Crucially, this method <b>does not</b> null the entry in
     * {@link #keys} or rewrite {@link #hashCodes} — both arrays may be
     * shared with an open snapshot whose {@code deleted[entryIndex] ==
     * false}. The reap happens later inside {@link #growKeysAndHashCodeArrays}
     * in the writer's freshly allocated arrays.
     * <p>
     * Algorithm-R is Knuth, <i>The Art of Computer Programming</i>,
     * vol. 3, p. 527, with the roles of {@code i} and {@code j} swapped
     * so they can be renamed to <i>here</i> and <i>scan</i>.
     */
    protected void removeFrom(int here) {
        final var entryIndex = ~positions[here];
        deleted[entryIndex] = true;
        removedKeysCount++;
        // NB: keys[entryIndex] and hashCodes[entryIndex] are NOT touched.

        while (true) {
            positions[here] = 0;
            int scan = here;
            while (true) {
                if (--scan < 0) scan += positions.length;
                if (positions[scan] == 0) return;
                int r = calcStartIndexByHashCode(hashCodes[~positions[scan]]);
                if ((scan > r || r >= here) && (r >= here || here >= scan) && (here >= scan || scan > r)) {
                    positions[here] = positions[scan];
                    here = scan;
                    break;
                }
            }
        }
    }

    // ----- Lookup ------------------------------------------------------

    @Override
    public final boolean containsKey(K key) {
        final int hashCode = key.hashCode();
        var pIndex = calcStartIndexByHashCode(hashCode);
        while (true) {
            if (0 == positions[pIndex]) {
                return false;
            } else {
                final var eIndex = ~positions[pIndex];
                if (hashCode == hashCodes[eIndex] && key.equals(keys[eIndex])) {
                    return true;
                } else if (--pIndex < 0) {
                    pIndex += positions.length;
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Iterates dense (insertion-ish) order and skips dead slots via
     * {@link #deleted}. See {@link org.apache.jena.mem.collection.FastHashBase#anyMatch}
     * for trade-offs vs. {@link #anyMatchRandomOrder}.
     */
    @Override
    public final boolean anyMatch(Predicate<K> predicate) {
        var pos = keysPos - 1;
        while (-1 < pos) {
            if (!deleted[pos] && predicate.test(keys[pos])) {
                return true;
            }
            pos--;
        }
        return false;
    }

    public final boolean anyMatchRandomOrder(Predicate<K> predicate) {
        var pIndex = positions.length - 1;
        while (-1 < pIndex) {
            if (0 != positions[pIndex] && predicate.test(keys[~positions[pIndex]])) {
                return true;
            }
            pIndex--;
        }
        return false;
    }

    @Override
    public final ExtendedIterator<K> keyIterator() {
        return new SparseTombstoneIterator<>(keys, deleted, keysPos, this);
    }

    @Override
    public final Spliterator<K> keySpliterator() {
        return new SparseTombstoneSpliterator<>(keys, deleted, keysPos, this);
    }

    /**
     * Locate the slot in {@link #positions} that holds {@code key}. If
     * found, returns the (non-negative) probe-table slot index; if absent,
     * returns the bitwise complement of the empty probe-table slot at
     * which the key would be inserted.
     */
    protected final int findPosition(final K key, final int hashCode) {
        var pIndex = calcStartIndexByHashCode(hashCode);
        while (true) {
            if (0 == positions[pIndex]) {
                return ~pIndex;
            } else {
                final var pos = ~positions[pIndex];
                if (hashCode == hashCodes[pos] && key.equals(keys[pos])) {
                    return pIndex;
                } else if (--pIndex < 0) {
                    pIndex += positions.length;
                }
            }
        }
    }

    /**
     * Locate the next empty slot in {@link #positions} along the probe
     * chain for the given hash code, without checking existing entries
     * for equality. Used after a probe-table resize, where no duplicates
     * can exist by construction.
     */
    protected final int findEmptySlotWithoutEqualityCheck(final int hashCode) {
        var pIndex = calcStartIndexByHashCode(hashCode);
        while (true) {
            if (0 == positions[pIndex]) {
                return pIndex;
            } else if (--pIndex < 0) {
                pIndex += positions.length;
            }
        }
    }

    @Override
    public void clear() {
        positions = new int[MINIMUM_HASHES_SIZE];
        keys = newKeysArray(MINIMUM_ELEMENTS_SIZE);
        hashCodes = new int[MINIMUM_ELEMENTS_SIZE];
        deleted = new boolean[MINIMUM_ELEMENTS_SIZE];
        keysPos = 0;
        removedKeysCount = 0;
    }

    /**
     * @return the key at index {@code i}; bounds-and-liveness are not
     * checked. Caller must ensure {@code i} corresponds to a live slot.
     */
    public K getKeyAt(int i) {
        return keys[i];
    }

    /**
     * @return the index of the entry holding {@code key}, or {@code -1} if absent
     */
    public int indexOf(K key) {
        final var pIndex = findPosition(key, key.hashCode());
        if (pIndex < 0) {
            return -1;
        } else {
            return ~positions[pIndex];
        }
    }

    /**
     * Functional interface used by {@link #forEachKey} to receive each
     * live key along with the stable index it occupies.
     */
    @FunctionalInterface
    public interface KeyAndIndexConsumer<K> {
        void accept(K key, int index);
    }

    /**
     * Sequentially invoke {@code consumer} for every live key with its
     * index. Skips tombstoned slots via {@link #deleted}.
     */
    public void forEachKey(KeyAndIndexConsumer<K> consumer) {
        for (int i = 0; i < keysPos; i++) {
            if (!deleted[i]) {
                consumer.accept(keys[i], i);
            }
        }
    }
}

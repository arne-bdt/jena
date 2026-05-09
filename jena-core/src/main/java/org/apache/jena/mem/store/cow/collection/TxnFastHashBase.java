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
 * <h2>Tombstone discipline</h2>
 * Liveness is encoded by an explicit {@link #deleted} bitmap, not by
 * {@code keys[i] == null}. The four invariants the writer must preserve:
 * <ol>
 *   <li><b>Append-only on shared spine arrays.</b> The writer never writes
 *       to {@code keys[i]} or {@code hashCodes[i]} for any {@code i < } the
 *       source snapshot's {@code keysPos}. New entries always go at
 *       {@code keysPos++} (a slot beyond every open snapshot's view), or
 *       at a freelist slot inside the writer's <i>own</i> freshly grown
 *       arrays (post-grow, where nothing is shared with a snapshot).
 *   <li><b>No swap-with-last on shared arrays.</b> Removal does not permute
 *       elements in the dense array. Deletion only sets writer-private
 *       {@code deleted[i] = true} and runs Algorithm-R on the writer-private
 *       {@code positions} table.
 *   <li><b>Iteration skips by {@link #deleted}, never by null.</b> Because
 *       the writer never overwrites alive snapshot slots, a snapshot's
 *       {@code keys[i]} for a tombstoned slot still holds the (now-dead)
 *       reference; only {@code deleted[i]} marks it dead.
 *   <li><b>Freelist is built only at grow time, never on remove.</b>
 *       Linking a freed slot at remove time would require writing into
 *       shared {@code hashCodes[i]} (overwriting the cached hash a
 *       snapshot still relies on), so removes are pure tombstones. At
 *       {@code grow} the writer allocates fresh arrays and walks
 *       {@code deleted[]} to (a) null {@code keys[i]} so dead references
 *       can be GC'd, and (b) link tombstoned slots into a freelist
 *       headed at {@link #lastDeletedIndex}, encoded via the new
 *       {@code hashCodes[]}. Subsequent inserts in the same (or any
 *       later) write transaction consume the freelist first via
 *       {@link #getFreeKeyIndex()} before bumping {@code keysPos}.
 * </ol>
 *
 * <h2>Adaptive growth</h2>
 * The trigger for {@link #growKeysAndHashCodeArrays} is
 * {@code keysPos == keys.length}, but that does not always mean the
 * collection actually needs more capacity — under steady-state churn
 * (equal numbers of removes and adds) it just means the array filled up
 * with tombstones. The size decision is therefore:
 * <ul>
 *   <li>If {@code liveCount + 1 ≤ oldLength}, allocate a <b>same-size</b>
 *       new array. Compaction recovers enough slots; no real growth is
 *       needed. Steady-state churn workloads stay at constant capacity.
 *   <li>Otherwise allocate at the standard 1.5× factor.
 * </ul>
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
 *       {@link #lastDeletedIndex} (head of the post-grow freelist) is also
 *       writer-private.
 * </ul>
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
     * For live slots: cached {@link Object#hashCode()} of the corresponding
     * key, parallel to {@link #keys}.
     * <p>
     * For dead slots in the writer's <i>freshly grown</i> arrays: the index
     * of the previously freed slot, forming a singly-linked freelist whose
     * head is {@link #lastDeletedIndex}. The freelist is built by
     * {@link #growKeysAndHashCodeArrays} and consumed by
     * {@link #getFreeKeyIndex}.
     * <p>
     * For dead slots in arrays still shared with a snapshot (i.e. between
     * the most recent grow and any subsequent removes): the original
     * cached hash is preserved unchanged, because overwriting it would
     * corrupt the snapshot's view of that slot's hash.
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

    /**
     * Head of the singly-linked freelist of physically reaped slots
     * (built at the most recent grow), or {@code -1} when the freelist
     * is empty. The next link of a freelist node {@code i} is stored in
     * {@code hashCodes[i]}.
     * <p>
     * Only slots in the writer's <i>freshly grown</i> arrays appear on
     * this list — slots tombstoned by removes after the most recent grow
     * are tracked by {@link #deleted} only, not by this list. They join
     * the freelist at the next grow.
     */
    protected int lastDeletedIndex = -1;

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
        this.lastDeletedIndex = source.lastDeletedIndex;
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
     * Prefers the freelist (built at the most recent grow) over bumping
     * {@code keysPos}. A freelist slot lives entirely inside the writer's
     * own freshly grown arrays — none of those arrays are shared with a
     * snapshot, so writing a new entry there is safe (the snapshot's view
     * was constructed against the <i>old</i> arrays).
     * <p>
     * If the freelist is empty and {@code keysPos == keys.length}, this
     * triggers {@link #growKeysAndHashCodeArrays}. The grow may either
     * (a) keep the same array length and recover slots via compaction,
     * in which case the freelist is now non-empty, or (b) actually
     * allocate at 1.5×, in which case there is room past {@code keysPos}.
     * The recursive call resolves either case.
     */
    protected final int getFreeKeyIndex() {
        if (lastDeletedIndex >= 0) {
            final int index = lastDeletedIndex;
            // The next link is stored in hashCodes[index] (see
            // growKeysAndHashCodeArrays for how the chain is built).
            lastDeletedIndex = hashCodes[index];
            removedKeysCount--;
            return index;
        }
        if (keysPos == keys.length) {
            growKeysAndHashCodeArrays();
            // After grow either the freelist is populated (compaction)
            // or keys.length increased; recurse to take the right path.
            return getFreeKeyIndex();
        }
        return keysPos++;
    }

    /**
     * Allocate fresh {@link #keys}, {@link #hashCodes}, and {@link #deleted}
     * arrays, copy live state across, and compact the dead slots into a
     * reusable freelist.
     *
     * <h3>Sizing</h3>
     * Keeps the same array length when compaction alone would yield enough
     * room ({@code liveCount + 1 ≤ oldLength}); grows to 1.5× otherwise.
     * Steady-state churn workloads (equal removes/adds) therefore stay at
     * constant capacity.
     *
     * <h3>Compaction</h3>
     * For each slot {@code i} in {@code [0, keysPos)} with
     * {@code deleted[i] == true}:
     * <ul>
     *   <li>Null {@code keys[i]} (and, via the subclass hook,
     *       {@code values[i]}) so the dead reference can be GC'd.
     *   <li>Encode the slot into a singly-linked freelist by writing the
     *       previous head into {@code hashCodes[i]} and updating
     *       {@link #lastDeletedIndex} to {@code i}.
     * </ul>
     * The chain head is the highest index walked (we walk {@code 0 ..
     * keysPos-1}), giving LIFO consumption order in
     * {@link #getFreeKeyIndex}.
     *
     * <h3>Snapshot safety</h3>
     * All writes go into the writer's <i>new</i> arrays. Snapshots forked
     * earlier still reference the old arrays (which are untouched), so
     * compaction is invisible to them. The probe table {@link #positions}
     * doesn't need rebuilding either: it was already correct against the
     * stable indices in {@code [0, keysPos)} (Algorithm-R cleared probe
     * entries for dead slots at remove time).
     */
    protected void growKeysAndHashCodeArrays() {
        final int oldLength = keys.length;
        final int liveCount = keysPos - removedKeysCount;
        final int newSize = decideNewSize(oldLength, liveCount);

        final K[] newKeys = newKeysArray(newSize);
        System.arraycopy(keys, 0, newKeys, 0, oldLength);
        final int[] newHashCodes = new int[newSize];
        System.arraycopy(hashCodes, 0, newHashCodes, 0, oldLength);
        final boolean[] newDeleted = new boolean[newSize];
        System.arraycopy(deleted, 0, newDeleted, 0, oldLength);

        // Build the freelist inside the new arrays. Walking from low to
        // high index puts the highest tombstoned slot at the head, so
        // getFreeKeyIndex() consumes the freelist LIFO.
        int newLast = -1;
        for (int i = 0; i < keysPos; i++) {
            if (newDeleted[i]) {
                newKeys[i] = null;          // free the K reference
                newHashCodes[i] = newLast;  // freelist link (overwrites stale hash)
                newLast = i;
            }
        }

        this.keys = newKeys;
        this.hashCodes = newHashCodes;
        this.deleted = newDeleted;
        this.lastDeletedIndex = newLast;

        onKeysAndHashCodesGrown(oldLength);
    }

    /**
     * Choose the size of the next keys/hashCodes/deleted arrays.
     * <p>
     * If the live count plus one (the impending insert) fits in the
     * current length, we keep the same length: compaction will recover
     * enough slots. Otherwise we grow at the standard 1.5× factor with
     * a saturating overflow guard.
     */
    private static int decideNewSize(int oldLength, int liveCount) {
        if (liveCount + 1 <= oldLength) {
            // Compaction alone recovers enough room.
            return oldLength;
        }
        int grown = (oldLength >> 1) + oldLength;     // 1.5×
        if (grown <= oldLength) grown = oldLength + 1; // tiny-capacity safety
        if (grown < 0) grown = Integer.MAX_VALUE;      // overflow saturate
        return grown;
    }

    /**
     * Hook called after {@link #growKeysAndHashCodeArrays} has installed
     * the new {@link #keys}, {@link #hashCodes}, and {@link #deleted}
     * arrays. Subclasses (e.g. maps holding a parallel values array)
     * override this to grow and reap their own shared arrays in lock-step.
     * <p>
     * On entry the new arrays are already in place on the instance; the
     * hook can read {@code this.keys.length} for the new size and
     * {@code this.deleted} for the tombstone bitmap. {@code oldLength} is
     * the length of the previous arrays (for {@code arraycopy} bounds).
     * <p>
     * The default does nothing.
     */
    protected void onKeysAndHashCodesGrown(int oldLength) {
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
        lastDeletedIndex = -1;
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

    // ----- Package-private inspection (for tests) ----------------------

    /** @return the current capacity of {@link #keys} (after any grows). */
    int internalKeysLength() {
        return keys.length;
    }

    /** @return the current head of the post-grow freelist (or {@code -1}). */
    int internalLastDeletedIndex() {
        return lastDeletedIndex;
    }

    /** @return the current high-water mark, {@link #keysPos}. */
    int internalKeysPos() {
        return keysPos;
    }
}

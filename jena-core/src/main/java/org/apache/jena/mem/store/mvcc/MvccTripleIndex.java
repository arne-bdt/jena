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

package org.apache.jena.mem.store.mvcc;

import org.apache.jena.graph.Triple;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Compact, reader-safe map from a fully-concrete {@link Triple} to the latest
 * dense-array slot at which it was committed. It is the MVCC store's single
 * full-triple dedup/lookup structure, serving both the writer (committed-live
 * test and the slot to stamp on delete) and lock-free readers (an O(1)
 * {@code SPO contains} fast path).
 *
 * <h2>Storage</h2>
 * Open addressing with linear probing over parallel {@code Triple[] keys} and
 * {@code int[] slots} (no per-entry objects), so it costs one reference plus one
 * {@code int} per distinct triple — markedly leaner than a per-triple list index.
 * There are no removals: an MVCC delete is a version stamp on the slot, not an
 * index removal, so a deleted triple keeps its (now version-filtered) entry and a
 * re-add merely updates the recorded slot in place. Entries therefore only grow
 * until a {@linkplain MvccTripleStore vacuum} builds a fresh index for the
 * compacted generation.
 *
 * <h2>Concurrency</h2>
 * A single writer mutates the map under the store's writer lock; any number of
 * readers call {@link #slotOf} lock-free. The arrays live in an immutable
 * {@link Table} holder published through one {@code volatile} field, so a reader
 * reads a consistent {@code (keys, slots, mask)} view even across a resize (it
 * keeps probing the table it captured; entries added concurrently belong to a
 * later snapshot it must not see anyway). Within a table, a freshly inserted
 * bucket writes its slot <em>before</em> release-publishing its key and the reader
 * acquire-reads the key, so observing a key implies observing its slot. A re-add
 * updates the slot with a plain write; a reader may then briefly observe the
 * previous slot, which is harmless — the caller's version filter rejects it and
 * falls back to a full scan.
 */
final class MvccTripleIndex {

    /** Release/acquire element access to the {@code Triple[]} key table. */
    private static final VarHandle KEY = MethodHandles.arrayElementVarHandle(Triple[].class);

    private static final int MIN_CAPACITY = 16;

    /** {@link #slotOf} result when the triple has no entry at all. */
    static final int ABSENT = -1;

    /** Immutable holder so one volatile read yields a matching keys/slots/mask. */
    private record Table(Triple[] keys, int[] slots, int mask) {}

    /** Published view; replaced wholesale on resize. */
    private volatile Table table;

    /** Occupied-bucket count; read and written only by the single writer. */
    private int size;

    MvccTripleIndex() {
        this(MIN_CAPACITY);
    }

    /**
     * @param expectedEntries sizing hint; the table is rounded up to a power of two
     *                        with load-factor headroom so a bulk rebuild avoids resizes.
     */
    MvccTripleIndex(final int expectedEntries) {
        int cap = MIN_CAPACITY;
        final int target = expectedEntries + (expectedEntries >> 1); // ~/0.67 headroom
        while (cap < target && cap > 0) {
            cap <<= 1;
        }
        if (cap <= 0) {
            cap = 1 << 30;
        }
        this.table = new Table(new Triple[cap], new int[cap], cap - 1);
    }

    private static int spread(final int h) {
        return h ^ (h >>> 16);
    }

    /**
     * @param t a fully-concrete triple
     * @return the latest slot recorded for {@code t}, or {@link #ABSENT} if it has
     *         no entry. Lock-free; safe concurrently with the single writer.
     */
    int slotOf(final Triple t) {
        final Table tb = table;                     // one volatile acquire
        final Triple[] ks = tb.keys;
        final int[] sl = tb.slots;
        final int mask = tb.mask;
        int i = spread(t.hashCode()) & mask;
        while (true) {
            final Triple k = (Triple) KEY.getAcquire(ks, i);
            if (k == null) {
                return ABSENT;
            }
            if (k == t || k.equals(t)) {
                return sl[i];
            }
            i = (i + 1) & mask;
        }
    }

    /**
     * Record that {@code t} now lives at {@code slot}, inserting a new entry or
     * updating an existing one. Writer-only, under the store's writer lock.
     */
    void put(final Triple t, final int slot) {
        Table tb = table;
        if ((size + 1) > (tb.keys.length >> 1) + (tb.keys.length >> 2)) { // load factor 0.75
            tb = resize(tb);
        }
        final Triple[] ks = tb.keys;
        final int[] sl = tb.slots;
        final int mask = tb.mask;
        int i = spread(t.hashCode()) & mask;
        while (true) {
            final Triple k = ks[i];                 // plain read: only the writer mutates keys
            if (k == null) {
                sl[i] = slot;                       // publish the slot first ...
                KEY.setRelease(ks, i, t);           // ... then the key (release)
                size++;
                return;
            }
            if (k == t || k.equals(t)) {
                sl[i] = slot;                       // re-add: update the recorded slot in place
                return;
            }
            i = (i + 1) & mask;
        }
    }

    private Table resize(final Table old) {
        int newCap = old.keys.length << 1;
        if (newCap <= 0) {
            newCap = 1 << 30;
        }
        final Triple[] nk = new Triple[newCap];
        final int[] ns = new int[newCap];
        final int nmask = newCap - 1;
        final Triple[] ok = old.keys;
        final int[] os = old.slots;
        for (int i = 0; i < ok.length; i++) {
            final Triple k = ok[i];
            if (k != null) {
                int j = spread(k.hashCode()) & nmask;
                while (nk[j] != null) {
                    j = (j + 1) & nmask;
                }
                nk[j] = k;
                ns[j] = os[i];
            }
        }
        // One volatile publish; a reader's volatile read of table happens-after all
        // the element writes above, so no per-element release is needed here.
        final Table nt = new Table(nk, ns, nmask);
        table = nt;
        return nt;
    }

    /** Writer: discard all entries. */
    void clear() {
        this.table = new Table(new Triple[MIN_CAPACITY], new int[MIN_CAPACITY], MIN_CAPACITY - 1);
        this.size = 0;
    }
}

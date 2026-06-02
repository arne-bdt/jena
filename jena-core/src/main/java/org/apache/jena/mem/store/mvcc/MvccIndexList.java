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

/**
 * Append-only list of {@code int} triple-slot indices used as the value type of
 * the per-node subject/predicate/object indices in the MVCC eager strategy.
 * <p>
 * Unlike the copy-on-write variant's {@code IndexList}, this list is mutated in
 * place by the single writer while lock-free readers traverse it, so it must be
 * safe to publish. It never removes entries (an MVCC delete is a version stamp
 * on the triple slot, not an index removal), which keeps the structure strictly
 * append-only and lets readers rely on a simple snapshot protocol:
 * <ul>
 *   <li>The writer appends with {@link #append(int)}: it writes the element into
 *       {@code elements} (growing first if needed, publishing the filled new
 *       array through the {@code volatile} {@code elements} field) and only then
 *       bumps the {@code volatile} {@code size}.</li>
 *   <li>A reader takes a {@link #snapshot()}: it reads {@code size} (acquire)
 *       then {@code elements} (acquire) and clamps the length to
 *       {@code min(size, elements.length)}. Reading {@code size} before
 *       {@code elements} guarantees that if the reader observes a grown size it
 *       also observes the matching grown array with its contents fully visible;
 *       and the clamp guarantees no out-of-bounds access if it observes an older
 *       size against a newer (or older) array.</li>
 * </ul>
 * Correctness of <em>which</em> slots are live is handled separately by the
 * caller's version filter; this class only guarantees safe, tear-free traversal.
 */
public final class MvccIndexList {

    private static final int INITIAL_SIZE = 2;

    /** Number of valid entries. Volatile: the writer's release / reader's acquire. */
    private volatile int size = 0;

    /** Backing array. Volatile so a grown array publishes its copied contents. */
    private volatile int[] elements = new int[INITIAL_SIZE];

    /** Creates an empty list. */
    public MvccIndexList() {}

    /**
     * @return the current number of entries (a single {@code volatile} read).
     *         Intended as a cheap size <em>hint</em> for choosing the smaller of
     *         several candidate lists; the authoritative, tear-free length for
     *         traversal still comes from {@link #snapshot()}. A concurrent append
     *         may make this momentarily stale, which only affects the heuristic
     *         choice, never correctness.
     */
    public int size() {
        return size;
    }

    /**
     * Append one slot index. Single-writer only (called under the store's writer
     * lock during commit application).
     *
     * @param slot the triple-slot index to append
     */
    public void append(final int slot) {
        final int s = size;            // plain read; only this thread writes it
        int[] arr = elements;          // plain read; only this thread swaps it
        if (s == arr.length) {
            int newLen = (arr.length >> 1) + arr.length;
            if (newLen < 0) {
                newLen = Integer.MAX_VALUE;
            }
            final int[] grown = new int[newLen];
            System.arraycopy(arr, 0, grown, 0, s);
            grown[s] = slot;
            elements = grown;          // volatile release: publishes copied contents + new entry
        } else {
            arr[s] = slot;             // fill before publishing the new size
        }
        size = s + 1;                  // volatile release: publishes the entry
    }

    /**
     * A tear-free, point-in-time view of the list for a reader. The array may be
     * shared with the live list; only indices {@code [0, length)} are valid and
     * stable for the reader's purposes (newer appends are ignored).
     *
     * @param array  the backing array to read from
     * @param length the number of valid entries
     */
    public record Snapshot(int[] array, int length) {}

    /**
     * @return a consistent snapshot for traversal. Reads {@code size} before
     *         {@code elements} and clamps, per the class contract.
     */
    public Snapshot snapshot() {
        final int n = size;            // volatile acquire
        final int[] arr = elements;    // volatile acquire (>= the array matching n)
        return new Snapshot(arr, Math.min(n, arr.length));
    }
}

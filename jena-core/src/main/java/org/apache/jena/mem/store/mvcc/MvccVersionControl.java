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

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The shared "clock" and writer lock for one MVCC store (a single graph, or a
 * whole dataset of graphs that share one timeline).
 * <p>
 * The model mirrors the copy-on-write variant's concurrency contract — one
 * writer at a time, any number of lock-free readers — but achieves isolation
 * through per-triple version stamps rather than per-transaction copies:
 * <ul>
 *   <li>{@link #committedVersion} is a single {@code volatile} counter and the
 *       <em>sole</em> synchronization point. A writer bumps it once at commit;
 *       a reader reads it once at {@code begin(READ)} to pin its snapshot. The
 *       volatile write/read pair establishes happens-before between a commit and
 *       any later reader, so all of a committed transaction's array writes become
 *       visible without per-field fences.</li>
 *   <li>{@link #writeLock} serialises writers. Readers never take it.</li>
 *   <li>The active-reader registry tracks the lowest version any live reader is
 *       pinned at, so a future vacuum can reclaim slots whose {@code till} has
 *       dropped at or below {@link #minActiveReadVersion()} (nothing can ever see
 *       them again).</li>
 * </ul>
 * Versions start at {@code 0} (the empty store, before any commit); the first
 * write commits at version {@code 1}.
 */
public final class MvccVersionControl {

    /**
     * Highest committed version. {@code volatile}: a writer's commit publishes
     * all of its prior array writes; a reader's {@code begin(READ)} acquire makes
     * them visible. The only cross-thread fence in the design.
     */
    private volatile long committedVersion = 0L;

    /** Serialises writers; readers never acquire this. */
    private final ReentrantLock writeLock = new ReentrantLock();

    /**
     * Multiset of the versions currently pinned by active readers, keyed by
     * version with a reference count. A {@link ConcurrentSkipListMap} gives
     * O(log n) register/deregister and O(1) {@code firstKey()} for the minimum.
     * Consulted only by vacuum, so its cost is off the read/write hot path
     * except for the (cheap) register/deregister at reader begin/end.
     */
    private final ConcurrentSkipListMap<Long, AtomicInteger> activeReaders = new ConcurrentSkipListMap<>();

    /** @return the current highest committed version (volatile acquire). */
    public long committedVersion() {
        return committedVersion;
    }

    /** Acquire the single writer slot. Released by {@link #unlockWriter()}. */
    public void lockWriter() {
        writeLock.lock();
    }

    /** Release the writer slot. */
    public void unlockWriter() {
        writeLock.unlock();
    }

    /** @return {@code true} iff the calling thread holds the writer lock. */
    public boolean isWriteLockHeldByCurrentThread() {
        return writeLock.isHeldByCurrentThread();
    }

    /**
     * The version a new write transaction will operate at: one past the current
     * committed version. Uncommitted slots therefore carry a {@code since} the
     * committed readers filter out automatically. Must be called with the writer
     * lock held.
     *
     * @return {@code committedVersion + 1}
     */
    public long nextWriteVersion() {
        return committedVersion + 1L;
    }

    /**
     * Publish a write transaction: make {@code writeVersion} the new committed
     * version. The volatile store releases every array write the writer made.
     * Must be called with the writer lock held.
     *
     * @param writeVersion the version being committed (must equal
     *                     {@link #nextWriteVersion()} at the time the txn began)
     */
    public void publish(long writeVersion) {
        // Plain store is fine for ordering (lock provides it), but the field is
        // volatile so readers observe the bump and the happens-before edge.
        committedVersion = writeVersion;
    }

    /**
     * Pin a reader at the latest committed version, race-free against a concurrent
     * commit/vacuum. Returns the pinned version; the caller may then read the
     * store's generation, which is guaranteed consistent with (or newer than) the
     * pin.
     * <p>
     * The register-then-revalidate loop closes the window between reading
     * {@code committedVersion} and registering: if a commit (possibly triggering a
     * vacuum) slipped in, the registration is rolled back and retried, so any
     * vacuum that runs after this returns is guaranteed to observe the pin and use
     * a cutoff {@code <=} the pinned version. Must be paired with
     * {@link #unpinReader(long)}.
     *
     * @return the pinned committed version
     */
    public long pinReader() {
        while (true) {
            final long v = committedVersion;   // volatile read
            registerReader(v);
            if (v == committedVersion) {       // no commit slipped in: the pin holds
                return v;
            }
            deregisterReader(v);               // stale pin; retry at the new version
        }
    }

    /**
     * Release a pin taken by {@link #pinReader()}.
     *
     * @param version the version returned by {@code pinReader()}
     */
    public void unpinReader(long version) {
        deregisterReader(version);
    }

    /**
     * Register a reader pinned at {@code version}. Must be paired with
     * {@link #deregisterReader(long)}. The increment happens inside
     * {@code compute} so it cannot race with a concurrent deregister pruning
     * the entry to {@code null}.
     *
     * @param version the version the reader pinned at begin
     */
    public void registerReader(long version) {
        activeReaders.compute(version, (v, count) -> {
            if (count == null) {
                count = new AtomicInteger();
            }
            count.incrementAndGet();
            return count;
        });
    }

    /**
     * Deregister a reader previously registered at {@code version}. Atomically
     * decrements the reference count and prunes the entry when it reaches zero.
     *
     * @param version the version the reader was pinned at
     */
    public void deregisterReader(long version) {
        activeReaders.computeIfPresent(version, (v, count) ->
                count.decrementAndGet() == 0 ? null : count);
    }

    /**
     * The lowest version any active reader could still observe. A slot whose
     * {@code till} is {@code <=} this value is invisible to every current and
     * future reader and may be physically reclaimed by vacuum.
     *
     * @return the minimum pinned reader version, or {@link #committedVersion()}
     *         if there are no active readers
     */
    public long minActiveReadVersion() {
        final var first = activeReaders.firstEntry();
        final long committed = committedVersion;
        if (first == null) {
            return committed;
        }
        return Math.min(first.getKey(), committed);
    }
}

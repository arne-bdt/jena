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

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.IndexingStrategy;
import org.apache.jena.mem.store.AbstractTripleStoreTest;
import org.apache.jena.mem.store.TripleStore;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.apache.jena.testing_framework.GraphHelper.node;
import static org.apache.jena.testing_framework.GraphHelper.triple;
import static org.junit.Assert.*;

/**
 * Tests for the low-level MVCC store.
 * <p>
 * The plain {@link TripleStore} contract (add / remove / contains / find / stream /
 * count / copy, value semantics) is inherited from {@link AbstractTripleStoreTest},
 * exercised through the store's non-transactional auto-commit facade on an
 * {@link IndexingStrategy#EAGER} store.
 * <p>
 * The tests defined here focus on the properties that distinguish the MVCC store
 * from the copy-on-write variant and that the generic contract cannot reach:
 * O(1)-begin snapshot isolation, logical (version-stamped) deletes, multi-version
 * delete-then-readd, undo-free abort, vacuum/auto-vacuum, lock-free concurrent
 * reads, and the MANUAL/MINIMAL strategy behaviours.
 */
public class MvccTripleStoreTest extends AbstractTripleStoreTest {

    @Override
    protected TripleStore createTripleStore() {
        return new MvccTripleStore(IndexingStrategy.EAGER);
    }

    private static void write(MvccTripleStore store, Consumer<MvccWriteTxn> ops) {
        store.versionControl().lockWriter();
        try {
            final MvccWriteTxn w = store.openWriteTxn();
            ops.accept(w);
            if (w.hasChanges()) {
                store.commit(w);
            }
        } finally {
            store.versionControl().unlockWriter();
        }
    }

    private static Triple subjectPattern(Triple t) {
        return Triple.create(t.getSubject(), Node.ANY, Node.ANY);
    }

    private static int countFind(MvccReadView v, Triple pattern) {
        return v.find(pattern).toList().size();
    }

    @Test
    public void fullTripleIndexContainsAcrossDeleteReaddAndAbsent() {
        final var store = new MvccTripleStore(IndexingStrategy.EAGER);
        write(store, w -> {
            w.add(triple("s1 p1 o1"));
            w.add(triple("s1 p2 o2"));
            w.add(triple("s2 p1 o2"));
        });
        // s1, p1 and o2 all occur, but (s1 p1 o2) never co-occurs: the full-triple
        // index reports ABSENT and contains returns false without a scan hit.
        final MvccReadView v0 = store.openReadView();
        try {
            assertTrue(v0.contains(triple("s1 p1 o1")));
            assertFalse(v0.contains(triple("s1 p1 o2")));
        } finally {
            v0.close();
        }

        // Delete: a reader pinned before still sees it (the index slot is filtered
        // by version, not removed); a later reader does not.
        final MvccReadView before = store.openReadView();
        write(store, w -> w.remove(triple("s1 p1 o1")));
        final MvccReadView after = store.openReadView();
        try {
            assertTrue("pre-delete reader sees the triple", before.contains(triple("s1 p1 o1")));
            assertFalse("post-delete reader does not", after.contains(triple("s1 p1 o1")));
        } finally {
            before.close();
            after.close();
        }

        // Re-add at a fresh slot: the index entry is updated in place and the latest
        // reader sees it again.
        write(store, w -> w.add(triple("s1 p1 o1")));
        final MvccReadView readd = store.openReadView();
        try {
            assertTrue("re-add is visible to the latest reader", readd.contains(triple("s1 p1 o1")));
            assertEquals(3, readd.count());
        } finally {
            readd.close();
        }
    }

    @Test
    public void duplicateAddIsIdempotent() {
        final var store = new MvccTripleStore(IndexingStrategy.EAGER);
        write(store, w -> {
            w.add(triple("a b c"));
            w.add(triple("a b c"));
        });
        write(store, w -> w.add(triple("a b c")));
        final MvccReadView v = store.openReadView();
        try {
            assertEquals(1, v.count());
        } finally {
            v.close();
        }
    }

    @Test
    public void snapshotIsolationFromLaterWrite() {
        final var store = new MvccTripleStore(IndexingStrategy.EAGER);
        write(store, w -> w.add(triple("s p o1")));

        final MvccReadView pinned = store.openReadView();   // sees o1 only
        write(store, w -> w.add(triple("s p o2")));         // commit after pin

        try {
            assertTrue(pinned.contains(triple("s p o1")));
            assertFalse("reader pinned before the write must not see it",
                    pinned.contains(triple("s p o2")));
            assertEquals(1, pinned.count());

            final MvccReadView fresh = store.openReadView();
            try {
                assertTrue(fresh.contains(triple("s p o2")));
                assertEquals(2, fresh.count());
            } finally {
                fresh.close();
            }
        } finally {
            pinned.close();
        }
    }

    @Test
    public void deleteIsVisibleToNewReadersOnly() {
        final var store = new MvccTripleStore(IndexingStrategy.EAGER);
        write(store, w -> w.add(triple("a b c")));

        final MvccReadView before = store.openReadView();   // sees the triple
        write(store, w -> w.remove(triple("a b c")));       // logical delete

        try {
            assertTrue("delete is in the old reader's future", before.contains(triple("a b c")));
            assertEquals(1, before.count());

            final MvccReadView after = store.openReadView();
            try {
                assertFalse(after.contains(triple("a b c")));
                assertEquals(0, after.count());
                assertTrue(after.isEmpty());
            } finally {
                after.close();
            }
        } finally {
            before.close();
        }
    }

    /**
     * The case the user asked about: a triple deleted at version D and re-added at
     * version A, with three readers — one pinned before D (sees the original), one
     * pinned in the gap [D, A) (sees neither), one pinned at/after A (sees the new
     * one). All three must be simultaneously correct against one shared store.
     */
    @Test
    public void multiVersionDeleteThenReadd() {
        final var store = new MvccTripleStore(IndexingStrategy.EAGER);
        final Triple t = triple("a b c");

        write(store, w -> w.add(t));                 // v1: present
        final MvccReadView readerBeforeDelete = store.openReadView();

        write(store, w -> w.remove(t));              // v2: deleted
        final MvccReadView readerInGap = store.openReadView();

        write(store, w -> w.add(t));                 // v3: re-added (new slot)
        final MvccReadView readerAfterReadd = store.openReadView();

        try {
            assertTrue("pre-delete reader sees the original", readerBeforeDelete.contains(t));
            assertEquals(1, readerBeforeDelete.count());

            assertFalse("gap reader sees neither version", readerInGap.contains(t));
            assertEquals(0, readerInGap.count());

            assertTrue("post-readd reader sees the new version", readerAfterReadd.contains(t));
            assertEquals(1, readerAfterReadd.count());

            // find must not yield duplicates even though two slots hold the value
            assertEquals(1, countFind(readerAfterReadd, subjectPattern(t)));
            assertEquals(1, readerAfterReadd.find(t).toList().size());
        } finally {
            readerBeforeDelete.close();
            readerInGap.close();
            readerAfterReadd.close();
        }
    }

    @Test
    public void abortAppliesNothing() {
        final var store = new MvccTripleStore(IndexingStrategy.EAGER);
        write(store, w -> w.add(triple("keep me yes")));

        // "abort" = open a write txn, mutate, never commit, just unlock.
        store.versionControl().lockWriter();
        try {
            final MvccWriteTxn w = store.openWriteTxn();
            w.add(triple("drop me no"));
            w.remove(triple("keep me yes"));
            // read-your-writes within the txn reflects the changes...
            assertTrue(w.contains(triple("drop me no")));
            assertFalse(w.contains(triple("keep me yes")));
            // ...but we abort by not committing.
        } finally {
            store.versionControl().unlockWriter();
        }

        final MvccReadView v = store.openReadView();
        try {
            assertTrue(v.contains(triple("keep me yes")));
            assertFalse(v.contains(triple("drop me no")));
            assertEquals(1, v.count());
        } finally {
            v.close();
        }
    }

    @Test
    public void deleteThenReaddInSameTransactionIsNetNoChange() {
        final var store = new MvccTripleStore(IndexingStrategy.EAGER);
        write(store, w -> w.add(triple("a b c")));

        write(store, w -> {
            w.remove(triple("a b c"));
            w.add(triple("a b c"));
            assertFalse("net no-op should leave nothing to publish", w.hasChanges());
        });

        final MvccReadView v = store.openReadView();
        try {
            assertEquals(1, v.count());
            assertTrue(v.contains(triple("a b c")));
        } finally {
            v.close();
        }
    }

    /**
     * MINIMAL keeps no index and answers partial patterns by a dense scan, so the
     * counts must match those an EAGER store would give. (EAGER's own partial
     * patterns are covered by the inherited {@link AbstractTripleStoreTest}.)
     */
    @Test
    public void partialPatternsMinimal() {
        assertPartialPatterns(IndexingStrategy.MINIMAL);
    }

    private void assertPartialPatterns(IndexingStrategy strategy) {
        final var store = new MvccTripleStore(strategy);
        final Triple t1 = triple("s1 p1 o1");
        final Triple t2 = triple("s1 p2 o2");
        final Triple t3 = triple("s2 p1 o1");
        write(store, w -> {
            w.add(t1);
            w.add(t2);
            w.add(t3);
        });
        final MvccReadView v = store.openReadView();
        try {
            // SUB_ANY_ANY
            assertEquals(2, countFind(v, Triple.create(t1.getSubject(), Node.ANY, Node.ANY)));
            // ANY_PRE_ANY
            assertEquals(2, countFind(v, Triple.create(Node.ANY, t1.getPredicate(), Node.ANY)));
            // ANY_ANY_OBJ
            assertEquals(2, countFind(v, Triple.create(Node.ANY, Node.ANY, t1.getObject())));
            // SUB_PRE_ANY
            assertEquals(1, countFind(v, Triple.create(t1.getSubject(), t1.getPredicate(), Node.ANY)));
            // SUB_ANY_OBJ
            assertEquals(1, countFind(v, Triple.create(t1.getSubject(), Node.ANY, t1.getObject())));
            // ANY_PRE_OBJ  (p1,o1) matches t1 and t3
            assertEquals(2, countFind(v, Triple.create(Node.ANY, t1.getPredicate(), t1.getObject())));
            // SUB_PRE_OBJ (concrete)
            assertEquals(1, countFind(v, t1));
            assertTrue(v.contains(t1));
            // ANY_ANY_ANY
            assertEquals(3, countFind(v, Triple.create(Node.ANY, Node.ANY, Node.ANY)));
        } finally {
            v.close();
        }
    }

    /**
     * MANUAL keeps no index until explicitly built: a partial-pattern lookup must
     * throw {@link UnsupportedOperationException} until then, while fully-concrete
     * (SPO) and fully-unbound (find-all / count) lookups never need the index and
     * work throughout. (Matches the {@link IndexingStrategy#MANUAL} contract and
     * the copy-on-write store's behaviour.)
     */
    @Test
    public void manualPartialPatternThrowsUntilInitialized() {
        final var store = new MvccTripleStore(IndexingStrategy.MANUAL);
        write(store, w -> {
            w.add(triple("s1 p1 o1"));
            w.add(triple("s1 p2 o2"));
        });
        assertFalse(store.isIndexInitialized());

        final MvccReadView v = store.openReadView();
        try {
            // Partial patterns throw until the index is built.
            assertThrows(UnsupportedOperationException.class,
                    () -> v.find(Triple.create(node("s1"), Node.ANY, Node.ANY)).toList());
            assertThrows(UnsupportedOperationException.class,
                    () -> v.contains(Triple.create(node("s1"), Node.ANY, Node.ANY)));
            // Fully-concrete and fully-unbound lookups never need the index.
            assertTrue(v.contains(triple("s1 p1 o1")));
            assertFalse(v.contains(triple("s1 p1 missing")));
            assertEquals(2, v.count());
            assertEquals(2, countFind(v, Triple.create(Node.ANY, Node.ANY, Node.ANY)));
        } finally {
            v.close();
        }
    }

    private static void initializeIndex(MvccTripleStore store, boolean parallel) {
        store.versionControl().lockWriter();
        try {
            if (parallel) {
                store.initializeIndexParallel();
            } else {
                store.initializeIndex();
            }
        } finally {
            store.versionControl().unlockWriter();
        }
    }

    private static void clearIndex(MvccTripleStore store) {
        store.versionControl().lockWriter();
        try {
            store.clearIndex();
        } finally {
            store.versionControl().unlockWriter();
        }
    }

    @Test
    public void manualInitializeIndexBuildsAndServes() {
        final var store = new MvccTripleStore(IndexingStrategy.MANUAL);
        write(store, w -> {
            w.add(triple("s1 p1 o1"));
            w.add(triple("s1 p2 o2"));
        });
        assertFalse(store.isIndexInitialized());

        initializeIndex(store, false);
        assertTrue(store.isIndexInitialized());

        final MvccReadView v = store.openReadView();
        try {
            assertEquals(2, countFind(v, Triple.create(node("s1"), Node.ANY, Node.ANY)));
        } finally {
            v.close();
        }
    }

    @Test
    public void manualInitializeIndexParallelBuildsAndServes() {
        final var store = new MvccTripleStore(IndexingStrategy.MANUAL);
        write(store, w -> {
            for (int i = 0; i < 200; i++) {
                w.add(triple("s" + (i % 7) + " p o" + i));
            }
        });
        assertFalse(store.isIndexInitialized());

        initializeIndex(store, true);   // parallel build
        assertTrue(store.isIndexInitialized());

        final MvccReadView v = store.openReadView();
        try {
            // The parallel (one-dimension-per-thread) build must agree with a scan.
            assertEquals(200, v.count());
            assertEquals(200, countFind(v, Triple.create(Node.ANY, node("p"), Node.ANY)));
            assertTrue(v.contains(Triple.create(node("s3"), Node.ANY, Node.ANY)));
        } finally {
            v.close();
        }
    }

    @Test
    public void minimalInitializeIndexUpgradesAndSurvivesVacuum() {
        final var store = new MvccTripleStore(IndexingStrategy.MINIMAL);
        write(store, w -> {
            for (int i = 0; i < 10; i++) {
                w.add(triple("s p o" + i));
            }
        });
        assertFalse(store.isIndexInitialized());

        initializeIndex(store, false);  // MINIMAL upgraded to serve from an index
        assertTrue(store.isIndexInitialized());

        vacuum(store);                  // a vacuum must preserve the built index
        assertTrue("vacuum must preserve a built MINIMAL index", store.isIndexInitialized());

        final MvccReadView v = store.openReadView();
        try {
            assertEquals(10, countFind(v, Triple.create(node("s"), Node.ANY, Node.ANY)));
        } finally {
            v.close();
        }
    }

    @Test
    public void clearIndexRevertsManualToThrowing() {
        final var store = new MvccTripleStore(IndexingStrategy.MANUAL);
        write(store, w -> w.add(triple("s1 p1 o1")));

        initializeIndex(store, false);
        assertTrue(store.isIndexInitialized());

        clearIndex(store);
        assertFalse(store.isIndexInitialized());

        final MvccReadView v = store.openReadView();
        try {
            assertThrows(UnsupportedOperationException.class,
                    () -> v.find(Triple.create(node("s1"), Node.ANY, Node.ANY)).toList());
            assertTrue(v.contains(triple("s1 p1 o1")));   // fully-concrete still works
        } finally {
            v.close();
        }
    }

    private static void vacuum(MvccTripleStore store) {
        store.versionControl().lockWriter();
        try {
            store.vacuum();
        } finally {
            store.versionControl().unlockWriter();
        }
    }

    @Test
    public void vacuumReclaimsDeadSlotsAndRebuildsIndex() {
        final var store = new MvccTripleStore(IndexingStrategy.EAGER);
        write(store, w -> {
            for (int i = 0; i < 100; i++) {
                w.add(triple("s" + i + " p o" + i));
            }
        });
        write(store, w -> {
            for (int i = 0; i < 50; i++) {
                w.remove(triple("s" + i + " p o" + i));
            }
        });
        assertEquals("dead slots linger before vacuum", 100, store.physicalSlotCount());

        vacuum(store); // no active readers -> reclaim every dead slot
        assertEquals("vacuum reclaims the 50 deleted slots", 50, store.physicalSlotCount());

        final MvccReadView v = store.openReadView();
        try {
            assertEquals(50, v.count());
            assertFalse(v.contains(triple("s0 p o0")));
            assertTrue(v.contains(triple("s50 p o50")));
            // The index was rebuilt over the renumbered survivors; partial lookups still work.
            assertEquals(50, countFind(v, Triple.create(Node.ANY,
                    triple("x p y").getPredicate(), Node.ANY)));
        } finally {
            v.close();
        }
    }

    @Test
    public void vacuumRetainsSlotsRegisteredReadersNeed() {
        final var store = new MvccTripleStore(IndexingStrategy.EAGER);
        final Triple t = triple("a b c");
        write(store, w -> w.add(t));

        final long v1 = store.versionControl().committedVersion();
        store.versionControl().registerReader(v1);   // a reader pinned at v1
        write(store, w -> w.remove(t));              // delete at v2 > v1
        assertTrue("v1 still sees t before vacuum", store.containsAt(v1, t));

        vacuum(store);                               // cutoff = v1 -> t (till=v2) retained
        assertTrue("vacuum must not reclaim a registered reader's slot", store.containsAt(v1, t));
        assertEquals(1, store.physicalSlotCount());

        store.versionControl().deregisterReader(v1); // reader gone
        vacuum(store);                               // cutoff = committed -> reclaim t
        assertEquals("with no readers, the dead slot is reclaimed", 0, store.physicalSlotCount());
    }

    @Test
    public void vacuumPreservesMultiVersionSnapshots() {
        final var store = new MvccTripleStore(IndexingStrategy.EAGER);
        final Triple t = triple("a b c");

        write(store, w -> w.add(t));                 // v1: present
        final long v1 = store.versionControl().committedVersion();
        store.versionControl().registerReader(v1);

        write(store, w -> w.remove(t));              // v2: deleted
        final long v2 = store.versionControl().committedVersion();
        store.versionControl().registerReader(v2);

        write(store, w -> w.add(t));                 // v3: re-added (new slot)
        final long v3 = store.versionControl().committedVersion();
        store.versionControl().registerReader(v3);

        vacuum(store); // cutoff = v1 -> both the [v1,v2) and [v3,inf) slots are retained

        assertTrue("pre-delete snapshot survives vacuum", store.containsAt(v1, t));
        assertFalse("gap snapshot sees neither version", store.containsAt(v2, t));
        assertTrue("post-readd snapshot survives vacuum", store.containsAt(v3, t));

        store.versionControl().deregisterReader(v1);
        store.versionControl().deregisterReader(v2);
        store.versionControl().deregisterReader(v3);
    }

    @Test
    public void autoVacuumKicksInUnderChurn() {
        final var store = new MvccTripleStore(IndexingStrategy.EAGER);
        // Load enough to clear the auto-vacuum minimum, then delete half with no
        // active readers: the commit's auto-vacuum should compact in place.
        write(store, w -> {
            for (int i = 0; i < 3000; i++) {
                w.add(triple("s" + i + " p o" + i));
            }
        });
        write(store, w -> {
            for (int i = 0; i < 2000; i++) {
                w.remove(triple("s" + i + " p o" + i));
            }
        });
        assertTrue("auto-vacuum should have compacted to roughly the live set, was "
                        + store.physicalSlotCount(),
                store.physicalSlotCount() <= 1001);
        final MvccReadView v = store.openReadView();
        try {
            assertEquals(1000, v.count());
        } finally {
            v.close();
        }
    }

    @Test
    public void concurrentReadersDuringChurnAndVacuum() throws Exception {
        final var store = new MvccTripleStore(IndexingStrategy.EAGER);
        write(store, w -> {
            for (int i = 0; i < 2000; i++) {
                w.add(triple("s" + i + " p o" + i));
            }
        });

        final AtomicBoolean stop = new AtomicBoolean(false);
        final AtomicReference<Throwable> error = new AtomicReference<>();

        // Writer: delete then re-add blocks of triples, creating dead slots and
        // (when no reader lags) triggering auto-vacuum compaction.
        final Thread writer = new Thread(() -> {
            try {
                int round = 0;
                while (!stop.get()) {
                    final int base = (round++ % 5) * 200;
                    write(store, w -> {
                        for (int i = base; i < base + 200; i++) {
                            w.remove(triple("s" + i + " p o" + i));
                        }
                    });
                    write(store, w -> {
                        for (int i = base; i < base + 200; i++) {
                            w.add(triple("s" + i + " p o" + i));
                        }
                    });
                }
            } catch (Throwable th) {
                error.set(th);
            }
        });
        writer.start();

        try {
            // Each reader snapshot must be internally consistent (count() agrees
            // with a full enumeration) and never throw, despite concurrent
            // churn + compaction.
            for (int k = 0; k < 300; k++) {
                final MvccReadView v = store.openReadView();
                try {
                    final int byCount = v.count();
                    final int byScan = v.find(Triple.create(Node.ANY, Node.ANY, Node.ANY)).toList().size();
                    assertEquals("snapshot must be internally consistent", byCount, byScan);
                    // The snapshot lands either between a delete and its re-add (1800)
                    // or outside that window (2000) — never anything else.
                    assertTrue("unexpected snapshot size " + byCount,
                            byCount >= 1800 && byCount <= 2000);
                } finally {
                    v.close();
                }
            }
        } finally {
            stop.set(true);
            writer.join(10_000);
        }
        if (error.get() != null) {
            throw new AssertionError("writer thread failed", error.get());
        }
    }

    @Test
    public void concurrentSpoContainsDuringIndexResize() throws Exception {
        final var store = new MvccTripleStore(IndexingStrategy.EAGER);
        // A stable core that is never deleted: present in every snapshot taken
        // after this seed commit.
        write(store, w -> {
            for (int i = 0; i < 300; i++) {
                w.add(triple("stable" + i + " p o" + i));
            }
        });

        final AtomicBoolean stop = new AtomicBoolean(false);
        final AtomicReference<Throwable> error = new AtomicReference<>();

        // Writer: continuously add fresh triples (driving the full-triple index
        // through repeated resizes) while churning a disjoint re-add block.
        final Thread writer = new Thread(() -> {
            try {
                int round = 0;
                while (!stop.get()) {
                    final int r = round++;
                    write(store, w -> {
                        for (int i = 0; i < 100; i++) {
                            w.add(triple("grow" + r + "_" + i + " p o"));
                        }
                    });
                    write(store, w -> {
                        for (int i = 0; i < 100; i++) {
                            w.remove(triple("churn" + i + " q c" + i));
                        }
                    });
                    write(store, w -> {
                        for (int i = 0; i < 100; i++) {
                            w.add(triple("churn" + i + " q c" + i));
                        }
                    });
                }
            } catch (Throwable th) {
                error.set(th);
            }
        });
        writer.start();

        try {
            for (int k = 0; k < 150; k++) {
                final MvccReadView v = store.openReadView();
                try {
                    for (int i = 0; i < 300; i += 30) {
                        // The stable core resolves via the O(1) index hit path on
                        // every snapshot, even while the index is resizing.
                        assertTrue("stable triple must be present",
                                v.contains(triple("stable" + i + " p o" + i)));
                        assertTrue("stable triple must be found",
                                v.find(triple("stable" + i + " p o" + i)).hasNext());
                        // Triples that were never added must never be reported
                        // present: a racy index read can only fall back, never lie.
                        assertFalse("absent-object triple must never be found",
                                v.contains(triple("stable" + i + " p NEVER")));
                        assertFalse("fresh-subject triple must never be found",
                                v.contains(triple("ghost" + k + "_" + i + " p o")));
                    }
                    final int byCount = v.count();
                    final int byScan = v.find(Triple.create(Node.ANY, Node.ANY, Node.ANY)).toList().size();
                    assertEquals("snapshot must be internally consistent", byCount, byScan);
                } finally {
                    v.close();
                }
            }
        } finally {
            stop.set(true);
            writer.join(10_000);
        }
        if (error.get() != null) {
            throw new AssertionError("writer thread failed", error.get());
        }
    }
}

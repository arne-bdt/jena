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

import org.junit.Test;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Targeted tests for the compaction-at-grow + adaptive-sizing behaviour
 * added to {@link TxnFastHashBase}. The general fork-isolation, tombstone,
 * and basic-API behaviour is covered separately by
 * {@link TxnFastHashSetTest} and {@link TxnFastHashMapTest}; this suite
 * focuses on the parts the new design must enforce:
 * <ul>
 *   <li>{@code keys.length} stays constant under steady-state churn (the
 *       array does not balloon when removes and adds balance out).
 *   <li>{@code keys.length} grows by 1.5× when there really is no room.
 *   <li>Compaction at grow populates the freelist; subsequent inserts
 *       consume freelist slots before bumping {@code keysPos}.
 *   <li>Snapshots taken before a fork's compaction are unaffected — they
 *       still resolve to the original entries through the old arrays.
 * </ul>
 * The tests reach into the package-private inspection methods on
 * {@code TxnFastHashBase} ({@code internalKeysLength},
 * {@code internalLastDeletedIndex}, {@code internalKeysPos}) so that
 * sizing decisions can be asserted directly rather than inferred from
 * timing.
 */
public class TxnFastHashCompactionTest {

    /** Concrete subclass for tests; mirrors the helper in TxnFastHashSetTest. */
    private static final class StringSet extends TxnFastHashSet<String> {
        StringSet() { super(); }
        StringSet(int initialSize) { super(initialSize); }
        StringSet(StringSet src) { super(src); }
        @Override protected String[] newKeysArray(int size) { return new String[size]; }
        StringSet fork() { return new StringSet(this); }
    }

    private static final class StringMap extends TxnFastHashMap<String, String> {
        StringMap() { super(); }
        StringMap(int initialSize) { super(initialSize); }
        StringMap(StringMap src) { super(src); }
        @Override protected String[] newKeysArray(int size) { return new String[size]; }
        @Override protected String[] newValuesArray(int size) { return new String[size]; }
        StringMap fork() { return new StringMap(this); }
    }

    // ----- Adaptive sizing -----------------------------------------

    /**
     * Steady-state churn — same-key variant: each round removes one
     * existing key and immediately re-adds it. The live set is constant;
     * only the slot assignments rotate. The capacity must stay at the
     * initial value forever.
     */
    @Test
    public void capacityStaysConstantUnderSameKeyChurn() {
        StringSet s = new StringSet(64);
        int initialCap = s.internalKeysLength();
        for (int i = 0; i < initialCap; i++) s.tryAdd("k" + i);
        assertEquals(initialCap, s.size());
        assertEquals(initialCap, s.internalKeysLength());

        for (int round = 0; round < 1_000; round++) {
            String key = "k" + (round % initialCap);
            assertTrue("k" + (round % initialCap) + " expected at round " + round,
                    s.tryRemove(key));
            assertTrue("re-add of just-removed key at round " + round,
                    s.tryAdd(key));
            assertEquals("capacity ballooned at round " + round,
                    initialCap, s.internalKeysLength());
        }
        assertEquals(initialCap, s.size());
    }

    /**
     * Steady-state churn — sliding-window variant: each round removes the
     * oldest live key and adds a brand-new one. The live set rotates
     * through different keys but its size stays constant. Capacity must
     * still stay at the initial value.
     */
    @Test
    public void capacityStaysConstantUnderSlidingWindowChurn() {
        final int window = 64;
        StringSet s = new StringSet(window);
        int initialCap = s.internalKeysLength();
        // Seed the window with v0..v63.
        for (int i = 0; i < window; i++) s.tryAdd("v" + i);
        assertEquals(window, s.size());
        assertEquals(initialCap, s.internalKeysLength());

        for (int round = 0; round < 1_000; round++) {
            String oldest = "v" + round;          // currently in the window
            String newest = "v" + (round + window);
            assertTrue(s.tryRemove(oldest));
            assertTrue(s.tryAdd(newest));
            assertEquals("capacity ballooned at round " + round,
                    initialCap, s.internalKeysLength());
            assertEquals(window, s.size());
        }
    }

    /**
     * When the live count actually exceeds capacity, the array must grow
     * to the standard 1.5× factor.
     */
    @Test
    public void capacityGrowsByOneAndAHalfWhenTrulyFull() {
        StringSet s = new StringSet(8);
        // No removes — live count == keysPos. Each add past 8 triggers a
        // real grow. After 12 adds the capacity should be (8 → 12).
        for (int i = 0; i < 12; i++) s.tryAdd("k" + i);
        assertEquals(12, s.internalKeysLength());

        // Push to next grow boundary: 13 entries.
        s.tryAdd("k12");
        // 12 + 1 > 12 → grow by 1.5× → 18.
        assertEquals(18, s.internalKeysLength());
    }

    /**
     * Mixed case: live count + 1 ≤ capacity, but there are dead slots
     * from earlier removes. Grow must keep the same length and surface
     * the dead slots via the freelist.
     */
    @Test
    public void compactionRecoversSlotsViaFreelist() {
        StringSet s = new StringSet(8);
        for (int i = 0; i < 8; i++) s.tryAdd("k" + i);
        // Remove half to seed tombstones.
        for (int i = 0; i < 8; i += 2) s.tryRemove("k" + i);
        assertEquals(4, s.size());
        // Trigger grow: insert one more — keysPos is at 8, no freelist yet.
        s.tryAdd("new0");
        assertEquals("compaction kept the array length",
                8, s.internalKeysLength());
        // Freelist now non-empty after grow; the new entry consumed one
        // slot. Three dead slots remain on the freelist:
        assertNotEquals("freelist should have entries after compaction",
                -1, s.internalLastDeletedIndex());

        // Three more adds should drain the freelist without growing
        // and without bumping keysPos past the array.
        s.tryAdd("new1");
        s.tryAdd("new2");
        s.tryAdd("new3");
        assertEquals(8, s.internalKeysLength());
        assertEquals(-1, s.internalLastDeletedIndex());
        assertEquals(8, s.size());

        // The next add fills the last empty slot via keysPos++ (none of
        // the four pre-existing dead slots are left) — capacity is now
        // truly full, so the next add forces a 1.5× grow. With no dead
        // slots in the new array the freelist stays empty.
        s.tryAdd("new4");
        assertEquals(12, s.internalKeysLength());
        assertEquals(-1, s.internalLastDeletedIndex());
    }

    @Test
    public void compactionDoesNotResurrectRemovedKeys() {
        // Sanity: after compaction, removed keys are gone from contains/
        // iteration — only the slots they occupied are reused.
        StringSet s = new StringSet(8);
        for (int i = 0; i < 8; i++) s.tryAdd("k" + i);
        for (int i = 0; i < 8; i += 2) s.tryRemove("k" + i);
        // Force grow + compaction.
        s.tryAdd("new");

        Set<String> live = new HashSet<>(s.keyStream().toList());
        assertEquals(Set.of("k1", "k3", "k5", "k7", "new"), live);
        for (int i = 0; i < 8; i += 2) assertFalse(s.containsKey("k" + i));
    }

    @Test
    public void freelistConsumptionMatchesIndexLifecycle() {
        // After compaction, an index returned by getFreeKeyIndex (via the
        // freelist) must be a slot < keysPos and not bump keysPos. We
        // verify this end-to-end by observing keysPos before/after a
        // freelist-consuming insert.
        StringSet s = new StringSet(8);
        for (int i = 0; i < 8; i++) s.tryAdd("k" + i);
        for (int i = 0; i < 4; i++) s.tryRemove("k" + i);
        assertEquals(8, s.internalKeysPos());

        // Force grow + compaction (size + 1 > capacity? live=4, cap=8,
        // live+1=5 ≤ 8 → same-size grow). The very *next* insert is the
        // one that triggers the grow because keysPos was at 8. So:
        s.tryAdd("post");
        // After grow, freelist has 4 entries; one was consumed for "post".
        assertEquals("keysPos must not advance when freelist is consumed",
                8, s.internalKeysPos());
        assertEquals(8, s.internalKeysLength());
        // Three more adds drain the freelist — still no keysPos bump.
        s.tryAdd("p1"); s.tryAdd("p2"); s.tryAdd("p3");
        assertEquals(8, s.internalKeysPos());
        assertEquals(-1, s.internalLastDeletedIndex());
    }

    // ----- Snapshot stability across compaction ---------------------

    /**
     * The crucial COW invariant under compaction: a snapshot taken before
     * the writer's grow must continue to see exactly its captured set,
     * even though the writer has reaped slots and reused them.
     */
    @Test
    public void snapshotUnaffectedByForkCompaction() {
        StringSet src = new StringSet(8);
        for (int i = 0; i < 8; i++) src.tryAdd("seed" + i);
        Set<String> srcExpected = new LinkedHashSet<>(src.keyStream().toList());
        assertEquals(8, srcExpected.size());

        StringSet fork = src.fork();
        // Remove four seeds in the fork...
        for (int i = 0; i < 8; i += 2) fork.tryRemove("seed" + i);
        // ...then add four NEW entries. These must consume the freelist
        // (built at the grow that the next add triggers).
        for (int i = 0; i < 5; i++) fork.tryAdd("new" + i);

        // Source: completely unchanged.
        assertEquals("source must observe original 8 entries", srcExpected,
                new LinkedHashSet<>(src.keyStream().toList()));
        for (int i = 0; i < 8; i++) {
            assertTrue("source lost seed" + i, src.containsKey("seed" + i));
        }

        // Fork: 4 surviving seeds + 5 new = 9 live.
        assertEquals(9, fork.size());
        for (int i = 0; i < 8; i += 2) assertFalse(fork.containsKey("seed" + i));
        for (int i = 1; i < 8; i += 2) assertTrue(fork.containsKey("seed" + i));
        for (int i = 0; i < 5; i++) assertTrue(fork.containsKey("new" + i));
    }

    @Test
    public void snapshotUnaffectedAcrossManyChurnRounds() {
        // Long-running stress version of the above. The fork churns
        // through many remove/add cycles while we periodically check the
        // source view. The capacity should stay bounded.
        StringSet src = new StringSet(64);
        for (int i = 0; i < 64; i++) src.tryAdd("seed" + i);
        Set<String> srcExpected = new HashSet<>(src.keyStream().toList());

        StringSet fork = src.fork();
        int forkInitialCap = fork.internalKeysLength();
        for (int round = 0; round < 1_000; round++) {
            String victim = "seed" + (round % 64);
            // Whatever the current shape of the fork, the source must
            // still report exactly its original 64 entries.
            if (round % 50 == 0) {
                assertEquals("source drifted at round " + round,
                        srcExpected, new HashSet<>(src.keyStream().toList()));
            }
            fork.tryRemove(victim);
            fork.tryAdd("seed" + (round % 64));   // re-insert the same key
        }
        // After all rounds, source unchanged.
        assertEquals(srcExpected, new HashSet<>(src.keyStream().toList()));
        // Fork capacity stays bounded at 64.
        assertEquals("fork capacity ballooned despite churn",
                forkInitialCap, fork.internalKeysLength());
    }

    // ----- Map: compaction also reaps values[] -------------------

    @Test
    public void mapCompactionNullsDeadValues() {
        // After grow + compaction, the values[] array's dead slots must
        // also be nulled (so the V references can be GC'd). We test
        // indirectly by ensuring the live values are still correct and
        // dead values are gone from valueIterator.
        StringMap m = new StringMap(8);
        for (int i = 0; i < 8; i++) m.tryPut("k" + i, "v" + i);
        for (int i = 0; i < 8; i += 2) m.tryRemove("k" + i);
        m.tryPut("post", "vp");                  // forces grow + compaction

        Set<String> seenValues = new HashSet<>(m.valueIterator().toList());
        // Live: v1, v3, v5, v7, vp.
        assertEquals(Set.of("v1", "v3", "v5", "v7", "vp"), seenValues);
    }

    @Test
    public void mapForkUpdateThenChurn_keepsSourceStable() {
        // Combined stress: an existing-key update on the fork already
        // does tombstone-and-append, eating one slot per update. Throw
        // in churn and a forced grow; the source must still report the
        // exact original key→value mapping.
        StringMap src = new StringMap(8);
        for (int i = 0; i < 8; i++) src.tryPut("k" + i, "orig" + i);

        StringMap fork = src.fork();
        for (int round = 0; round < 200; round++) {
            fork.put("k" + (round % 8), "v" + round);    // updates a single key
        }
        // Source still has all original values.
        for (int i = 0; i < 8; i++) {
            assertEquals("orig" + i, src.get("k" + i));
        }
        // Fork has the most recent value for each key.
        for (int i = 0; i < 8; i++) {
            int lastRound = (200 / 8 - 1) * 8 + i;     // 192 + i, fits the pattern
            assertEquals("v" + lastRound, fork.get("k" + i));
        }
        // Fork capacity stayed bounded under update churn.
        // (Each update tombstones one slot; compaction reclaims them.)
        assertTrue("fork ballooned under update churn: " + fork.internalKeysLength(),
                fork.internalKeysLength() <= 32);
    }
}

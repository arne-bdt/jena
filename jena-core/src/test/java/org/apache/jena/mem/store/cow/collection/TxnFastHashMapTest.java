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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link TxnFastHashMap}, focusing on the parts that diverge
 * from the baseline {@code FastHashMap}:
 * <ul>
 *   <li>The <i>tombstone-and-append</i> update path: a put on an existing
 *       key must <b>not</b> overwrite {@code values[i]}, because the array
 *       is shared with the source snapshot.
 *   <li>Fork isolation across put / remove / update / compute.
 *   <li>Iteration via {@link SparseTombstoneIterator} /
 *       {@link SparseTombstoneSpliterator}.
 * </ul>
 */
public class TxnFastHashMapTest {

    /** Concrete test subclass — JenaMap doesn't expose the abstract array factories. */
    private static final class StringMap extends TxnFastHashMap<String, String> {
        StringMap() { super(); }
        StringMap(int initialSize) { super(initialSize); }
        StringMap(StringMap src) { super(src); }
        @Override protected String[] newKeysArray(int size) { return new String[size]; }
        @Override protected String[] newValuesArray(int size) { return new String[size]; }
        StringMap fork() { return new StringMap(this); }
    }

    private static Map<String, String> snapshot(StringMap m) {
        Map<String, String> out = new HashMap<>();
        m.forEachKey((k, idx) -> out.put(k, m.getValueAt(idx)));
        return out;
    }

    // ----- Basic operations ------------------------------------------

    @Test
    public void putAndGet() {
        StringMap m = new StringMap();
        assertTrue(m.tryPut("a", "1"));
        assertTrue(m.tryPut("b", "2"));
        assertEquals("1", m.get("a"));
        assertEquals("2", m.get("b"));
        assertNull(m.get("missing"));
        assertEquals("default", m.getOrDefault("missing", "default"));
    }

    @Test
    public void getOrDefaultReturnsStoredValueForPresentKey() {
        StringMap m = new StringMap();
        m.put("a", "1");
        assertEquals("1", m.getOrDefault("a", "fallback"));   // present: stored, not the default
        assertEquals("fallback", m.getOrDefault("missing", "fallback"));
    }

    @Test
    public void computeInsertGrowsPositionsTableMidOperation() {
        // Insert many distinct keys via compute(absent -> value) so the probe
        // table resizes during compute(); compute must recompute the (now
        // invalid) probe index after the resize and still insert correctly.
        StringMap m = new StringMap();
        final int N = 500;
        for (int i = 0; i < N; i++) {
            final String k = "k" + i;
            m.compute(k, prev -> { assertNull(prev); return "v" + k; });
        }
        assertEquals(N, m.size());
        for (int i = 0; i < N; i++) {
            assertEquals("vk" + i, m.get("k" + i));
        }
    }

    @Test
    public void tryPutOnExistingKeyReturnsFalseAndUpdatesValue() {
        StringMap m = new StringMap();
        assertTrue(m.tryPut("a", "1"));
        assertFalse(m.tryPut("a", "2"));        // already present
        assertEquals("2", m.get("a"));
        assertEquals(1, m.size());
    }

    @Test
    public void removeMakesGetReturnNull() {
        StringMap m = new StringMap();
        m.tryPut("a", "1"); m.tryPut("b", "2");
        assertTrue(m.tryRemove("a"));
        assertNull(m.get("a"));
        assertEquals("2", m.get("b"));
        assertEquals(1, m.size());
    }

    @Test
    public void clearWipesEverything() {
        StringMap m = new StringMap();
        m.tryPut("a", "1"); m.tryPut("b", "2");
        m.clear();
        assertTrue(m.isEmpty());
        assertNull(m.get("a"));
        m.tryPut("c", "3");                     // can re-use after clear
        assertEquals(1, m.size());
    }

    // ----- Update path: tombstone-and-append -----------------------

    @Test
    public void updateOnSameMapIsObservable() {
        StringMap m = new StringMap();
        m.tryPut("a", "v1");
        m.put("a", "v2");                       // update, not insert
        assertEquals(1, m.size());
        assertEquals("v2", m.get("a"));
    }

    @Test
    public void updateOnSameMapAdvancesKeysPos() {
        // tombstone-and-append consumes one extra slot; verify by adding
        // many updates to the same key and checking liveSize stays at 1.
        StringMap m = new StringMap();
        m.tryPut("a", "v0");
        for (int i = 1; i < 50; i++) {
            m.put("a", "v" + i);
            assertEquals("update must not change live size", 1, m.size());
            assertEquals("v" + i, m.get("a"));
        }
    }

    @Test
    public void updateForkDoesNotAlterSourceValue() {
        StringMap src = new StringMap();
        src.tryPut("a", "v1");
        src.tryPut("b", "v2");

        StringMap fork = src.fork();
        fork.put("a", "v1-FORKED");

        // Source must still report the original value.
        assertEquals("v1", src.get("a"));
        // Fork sees its update.
        assertEquals("v1-FORKED", fork.get("a"));

        Map<String, String> srcView = snapshot(src);
        assertEquals(Map.of("a", "v1", "b", "v2"), srcView);

        Map<String, String> forkView = snapshot(fork);
        assertEquals(Map.of("a", "v1-FORKED", "b", "v2"), forkView);
    }

    @Test
    public void updateAfterFork_thenAnotherUpdate_keepsSourceStable() {
        // Stress the tombstone-and-append by chaining updates; the source
        // must still see the original value through every step.
        StringMap src = new StringMap();
        src.tryPut("k", "original");

        StringMap fork = src.fork();
        for (int i = 0; i < 20; i++) {
            fork.put("k", "v" + i);
            assertEquals("original", src.get("k"));
        }
        assertEquals("v19", fork.get("k"));
    }

    // ----- Fork isolation across composite operations --------------

    @Test
    public void forkInsertionsAreInvisibleToSource() {
        StringMap src = new StringMap();
        src.tryPut("seed", "S");

        StringMap fork = src.fork();
        for (int i = 0; i < 100; i++) fork.tryPut("k" + i, "v" + i);

        assertEquals(1, src.size());
        assertEquals("S", src.get("seed"));
        assertNull(src.get("k0"));

        assertEquals(101, fork.size());
        assertEquals("v0", fork.get("k0"));
    }

    @Test
    public void forkRemovalsAreInvisibleToSource() {
        StringMap src = new StringMap();
        for (int i = 0; i < 10; i++) src.tryPut("k" + i, "v" + i);

        StringMap fork = src.fork();
        for (int i = 0; i < 10; i += 2) fork.tryRemove("k" + i);

        // Source: still 10 entries.
        assertEquals(10, src.size());
        for (int i = 0; i < 10; i++) assertEquals("v" + i, src.get("k" + i));

        // Fork: 5 odd entries.
        assertEquals(5, fork.size());
        for (int i = 0; i < 10; i++) {
            if (i % 2 == 0) assertNull(fork.get("k" + i));
            else assertEquals("v" + i, fork.get("k" + i));
        }
    }

    /**
     * Supported pattern: many read-only forks plus a single writer fork.
     * The writer's tombstone-and-append must leave every reader's view
     * unchanged (reader's keysPos is still at the pre-fork value, so it
     * never iterates the new slot the writer appended into).
     * <p>
     * The COW design assumes <b>one writer at a time</b>; this is enforced
     * at the graph layer by a write lock. Two concurrent mutating forks
     * sharing a common parent would both append at the same {@code keysPos}
     * and race on the shared {@code values[]} slot — that pattern is out
     * of contract and isn't exercised here.
     */
    @Test
    public void readerForksUnaffectedByWriterFork() {
        StringMap src = new StringMap();
        src.tryPut("x", "X");

        StringMap reader1 = src.fork();         // never mutated
        StringMap reader2 = src.fork();         // never mutated
        StringMap writer  = src.fork();
        writer.put("x", "W");                   // tombstone-and-append
        writer.tryPut("new", "NEW");

        // Source plus all reader forks must still observe the original.
        assertEquals("X", src.get("x"));
        assertEquals("X", reader1.get("x"));
        assertEquals("X", reader2.get("x"));
        assertNull(src.get("new"));
        assertNull(reader1.get("new"));
        assertNull(reader2.get("new"));

        // Writer sees its update.
        assertEquals("W", writer.get("x"));
        assertEquals("NEW", writer.get("new"));
    }

    @Test
    public void forkSurvivesGrow() {
        StringMap src = new StringMap(8);
        for (int i = 0; i < 8; i++) src.tryPut("seed" + i, "S" + i);

        StringMap fork = src.fork();
        for (int i = 0; i < 500; i++) fork.tryPut("k" + i, "v" + i);

        // Source view stays at 8 seeds.
        assertEquals(8, src.size());
        for (int i = 0; i < 8; i++) assertEquals("S" + i, src.get("seed" + i));
        for (int i = 0; i < 500; i++) assertNull(src.get("k" + i));

        // Fork: seeds + new entries.
        assertEquals(508, fork.size());
        for (int i = 0; i < 8; i++) assertEquals("S" + i, fork.get("seed" + i));
        for (int i = 0; i < 500; i++) assertEquals("v" + i, fork.get("k" + i));
    }

    // ----- compute / computeIfAbsent ---------------------------------

    @Test
    public void computeIfAbsentInsertsOnce() {
        StringMap m = new StringMap();
        assertEquals("first", m.computeIfAbsent("k", () -> "first"));
        assertEquals("first", m.computeIfAbsent("k", () -> "second"));
        assertEquals("first", m.get("k"));
    }

    @Test
    public void computeUpdateOnFork_doesNotAffectSource() {
        StringMap src = new StringMap();
        src.tryPut("k", "1");

        StringMap fork = src.fork();
        fork.compute("k", v -> v + "+fork");

        assertEquals("1", src.get("k"));
        assertEquals("1+fork", fork.get("k"));
    }

    @Test
    public void computeRemoveOnFork_doesNotAffectSource() {
        StringMap src = new StringMap();
        src.tryPut("k", "1");

        StringMap fork = src.fork();
        fork.compute("k", v -> null);             // remove

        assertEquals("1", src.get("k"));
        assertEquals(1, src.size());
        assertNull(fork.get("k"));
        assertEquals(0, fork.size());
    }

    @Test
    public void computeInsertOnFork_doesNotAffectSource() {
        StringMap src = new StringMap();

        StringMap fork = src.fork();
        fork.compute("new", v -> "fresh");

        assertNull(src.get("new"));
        assertEquals(0, src.size());
        assertEquals("fresh", fork.get("new"));
    }

    // ----- Iteration over values ------------------------------------

    @Test
    public void valueIteratorSkipsTombstones() {
        StringMap m = new StringMap();
        for (int i = 0; i < 10; i++) m.tryPut("k" + i, "v" + i);
        m.tryRemove("k3");
        m.tryRemove("k7");

        Set<String> seen = m.valueIterator().toSet();
        assertEquals(8, seen.size());
        assertFalse(seen.contains("v3"));
        assertFalse(seen.contains("v7"));
    }

    @Test
    public void valueSpliteratorSkipsTombstonesAndUpdatedSlots() {
        // After update on "k", values[oldSlot] still holds "old" but
        // deleted[oldSlot]=true. The spliterator must skip it.
        StringMap m = new StringMap();
        m.tryPut("k", "old");
        m.put("k", "new");

        Set<String> seen = StreamSupport.stream(m.valueSpliterator(), false)
                .collect(Collectors.toCollection(HashSet::new));
        assertEquals(Set.of("new"), seen);
    }

    @Test
    public void valueIteratorOnSourceSeesOldValueAfterForkUpdate() {
        // The decisive test for tombstone-and-append: the source's iterator
        // must walk through values[] using its own deleted[] (which still
        // says oldSlot is alive) and read "old" — not "new".
        StringMap src = new StringMap();
        src.tryPut("k", "old");

        StringMap fork = src.fork();
        fork.put("k", "new");

        Set<String> srcValues = src.valueIterator().toSet();
        assertEquals("source must observe pre-fork value", Set.of("old"), srcValues);

        Set<String> forkValues = fork.valueIterator().toSet();
        assertEquals("fork sees the new value only", Set.of("new"), forkValues);
    }
}

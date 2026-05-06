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
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link TxnFastHashBase} / {@link TxnFastHashSet}.
 * <p>
 * These cover the two responsibilities that distinguish the COW collection
 * from the baseline {@code FastHashSet}:
 * <ul>
 *   <li>Tombstone-based liveness — removed keys are skipped via
 *       {@code deleted[]}, not by {@code keys[i] != null}.
 *   <li>Fork isolation — a fork shares the source's {@code keys}/{@code hashCodes}
 *       but has its own {@code positions}/{@code deleted}, so mutations on
 *       the fork must never be visible to the source's view.
 * </ul>
 * Tests use Strings as keys: hash and equality semantics are well-defined
 * and easy to reason about.
 */
public class TxnFastHashSetTest {

    /** Concrete test subclass — JenaSet doesn't expose the abstract newKeysArray. */
    private static final class StringSet extends TxnFastHashSet<String> {
        StringSet() { super(); }
        StringSet(int initialSize) { super(initialSize); }
        StringSet(StringSet src) { super(src); }
        @Override protected String[] newKeysArray(int size) { return new String[size]; }

        StringSet fork() { return new StringSet(this); }
    }

    private static Set<String> drain(StringSet s) {
        // Stream goes through the spliterator under the hood, so this also
        // exercises SparseTombstoneSpliterator.
        return s.keyStream().collect(Collectors.toCollection(LinkedHashSet::new));
    }

    // ----- anyMatch ---------------------------------------------------

    @Test
    public void anyMatchAndAnyMatchRandomOrderSkipTombstones() {
        StringSet s = new StringSet();
        s.tryAdd("a"); s.tryAdd("b"); s.tryAdd("c");
        s.tryRemove("b");

        // anyMatch walks the dense array in reverse; anyMatchRandomOrder walks
        // the probe table. Both must see live keys and skip the tombstone.
        assertTrue(s.anyMatch("a"::equals));
        assertTrue(s.anyMatch("c"::equals));
        assertFalse("a tombstoned element must not match", s.anyMatch("b"::equals));
        assertFalse(s.anyMatch("z"::equals));

        assertTrue(s.anyMatchRandomOrder("c"::equals));
        assertFalse(s.anyMatchRandomOrder("b"::equals));
        assertFalse(s.anyMatchRandomOrder("z"::equals));
    }

    // ----- Basic operations ------------------------------------------

    @Test
    public void addAndContains() {
        StringSet s = new StringSet();
        assertTrue(s.tryAdd("a"));
        assertTrue(s.tryAdd("b"));
        assertFalse(s.tryAdd("a"));         // already present
        assertEquals(2, s.size());
        assertTrue(s.containsKey("a"));
        assertTrue(s.containsKey("b"));
        assertFalse(s.containsKey("c"));
    }

    @Test
    public void removeMaintainsContains() {
        StringSet s = new StringSet();
        s.tryAdd("a");
        s.tryAdd("b");
        s.tryAdd("c");
        assertTrue(s.tryRemove("b"));
        assertEquals(2, s.size());
        assertTrue(s.containsKey("a"));
        assertFalse(s.containsKey("b"));
        assertTrue(s.containsKey("c"));
        assertFalse(s.tryRemove("b"));      // already absent
    }

    @Test
    public void iterationSkipsTombstones() {
        StringSet s = new StringSet();
        for (String k : new String[]{"a", "b", "c", "d", "e"})
            s.tryAdd(k);
        s.tryRemove("b");
        s.tryRemove("d");
        Set<String> got = drain(s);
        assertEquals(Set.of("a", "c", "e"), got);
    }

    @Test
    public void clearResetsState() {
        StringSet s = new StringSet();
        s.tryAdd("a"); s.tryAdd("b");
        s.clear();
        assertTrue(s.isEmpty());
        assertEquals(0, s.size());
        assertTrue(drain(s).isEmpty());
        // can re-use after clear
        assertTrue(s.tryAdd("c"));
        assertEquals(1, s.size());
    }

    // ----- Grow ------------------------------------------------------

    @Test
    public void growHandlesManyEntries() {
        StringSet s = new StringSet(8);
        for (int i = 0; i < 1000; i++)
            assertTrue(s.tryAdd("k" + i));
        assertEquals(1000, s.size());
        for (int i = 0; i < 1000; i++)
            assertTrue("missing " + i, s.containsKey("k" + i));
        Set<String> drained = drain(s);
        assertEquals(1000, drained.size());
    }

    @Test
    public void growReapsTombstonedSlots() {
        StringSet s = new StringSet(8);
        // Insert enough to force several grows, removing every other key
        // to populate tombstones across multiple grow boundaries.
        for (int i = 0; i < 200; i++) s.tryAdd("k" + i);
        for (int i = 0; i < 200; i += 2) assertTrue(s.tryRemove("k" + i));
        // Drive more grows after the tombstones exist.
        for (int i = 200; i < 1000; i++) s.tryAdd("k" + i);
        // Live size = (200 - 100 still alive from first batch) + 800.
        assertEquals(900, s.size());

        // Sanity: removed keys are gone, kept keys are present.
        for (int i = 0; i < 200; i += 2) assertFalse(s.containsKey("k" + i));
        for (int i = 1; i < 200; i += 2) assertTrue(s.containsKey("k" + i));
        for (int i = 200; i < 1000; i++) assertTrue(s.containsKey("k" + i));
    }

    // ----- Fork isolation -------------------------------------------

    @Test
    public void forkIsIndependentForInsertions() {
        StringSet src = new StringSet();
        src.tryAdd("a"); src.tryAdd("b");

        StringSet fork = src.fork();
        fork.tryAdd("c");

        assertEquals(Set.of("a", "b", "c"), drain(fork));
        assertEquals("source must not observe fork's insertion",
                Set.of("a", "b"), drain(src));
        assertFalse(src.containsKey("c"));
        assertTrue(fork.containsKey("c"));
    }

    @Test
    public void forkIsIndependentForRemovals() {
        StringSet src = new StringSet();
        src.tryAdd("a"); src.tryAdd("b"); src.tryAdd("c");

        StringSet fork = src.fork();
        assertTrue(fork.tryRemove("b"));

        assertEquals(Set.of("a", "c"), drain(fork));
        assertEquals("source must not observe fork's removal",
                Set.of("a", "b", "c"), drain(src));
        assertTrue(src.containsKey("b"));
        assertFalse(fork.containsKey("b"));
    }

    @Test
    public void forkSurvivesGrow() {
        StringSet src = new StringSet(8);
        for (int i = 0; i < 8; i++) src.tryAdd("seed" + i);

        StringSet fork = src.fork();
        // Push fork through several grows.
        for (int i = 0; i < 500; i++) fork.tryAdd("fork" + i);

        // Source still sees only its seeds.
        Set<String> srcView = drain(src);
        assertEquals(8, srcView.size());
        for (int i = 0; i < 8; i++) assertTrue(srcView.contains("seed" + i));
        for (int i = 0; i < 500; i++) assertFalse(src.containsKey("fork" + i));

        // Fork sees seeds + own additions.
        assertEquals(508, fork.size());
        for (int i = 0; i < 8; i++) assertTrue(fork.containsKey("seed" + i));
        for (int i = 0; i < 500; i++) assertTrue(fork.containsKey("fork" + i));
    }

    @Test
    public void forkRemovalThenAddDoesNotCorruptSource() {
        StringSet src = new StringSet(8);
        // Fill enough to land entries on a known slot layout.
        Set<String> seeds = new HashSet<>();
        for (int i = 0; i < 50; i++) {
            String k = "seed" + i;
            seeds.add(k);
            src.tryAdd(k);
        }

        StringSet fork = src.fork();
        // Remove half, re-add others, in interleaved pattern.
        for (int i = 0; i < 50; i += 2) fork.tryRemove("seed" + i);
        for (int i = 0; i < 100; i++) fork.tryAdd("new" + i);

        // Source must still report the original seed set.
        assertEquals(seeds, drain(src));
        assertEquals(50, src.size());

        // Fork: 25 surviving seeds + 100 new = 125.
        assertEquals(125, fork.size());
    }

    @Test
    public void multipleForksAreMutuallyIndependent() {
        StringSet src = new StringSet();
        for (int i = 0; i < 10; i++) src.tryAdd("s" + i);

        StringSet a = src.fork();
        StringSet b = src.fork();

        a.tryAdd("only-a");
        b.tryRemove("s5");

        assertTrue(a.containsKey("only-a"));
        assertFalse(b.containsKey("only-a"));
        assertTrue(a.containsKey("s5"));
        assertFalse(b.containsKey("s5"));
        assertTrue("source unchanged", src.containsKey("s5"));
        assertFalse("source unchanged", src.containsKey("only-a"));
    }

    // ----- addAndGetIndex semantics ---------------------------------

    @Test
    public void addAndGetIndexReturnsStableIndex() {
        StringSet s = new StringSet();
        int idxA = s.addAndGetIndex("a");
        int idxB = s.addAndGetIndex("b");
        assertNotEquals(idxA, idxB);
        // Re-add returns ~existingIndex (negative).
        int reAdd = s.addAndGetIndex("a");
        assertTrue("re-add should return ~existingIndex, was " + reAdd, reAdd < 0);
        assertEquals(idxA, ~reAdd);
        assertEquals("a", s.getKeyAt(idxA));
        assertEquals("b", s.getKeyAt(idxB));
    }

    @Test
    public void indexOfReturnsMinusOneForAbsent() {
        StringSet s = new StringSet();
        s.tryAdd("a");
        assertTrue(s.indexOf("a") >= 0);
        assertEquals(-1, s.indexOf("missing"));
    }

    // ----- forEachKey skips tombstones -------------------------------

    @Test
    public void forEachKeySkipsTombstones() {
        StringSet s = new StringSet();
        for (int i = 0; i < 10; i++) s.tryAdd("k" + i);
        s.tryRemove("k3");
        s.tryRemove("k7");

        Set<String> visited = new HashSet<>();
        s.forEachKey((k, idx) -> visited.add(k));
        assertEquals(8, visited.size());
        assertFalse(visited.contains("k3"));
        assertFalse(visited.contains("k7"));
    }
}

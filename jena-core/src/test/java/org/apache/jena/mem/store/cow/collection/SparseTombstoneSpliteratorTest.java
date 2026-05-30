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

import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Set;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Pins the {@code characteristics()} contract of
 * {@link SparseTombstoneSpliterator}.
 * <p>
 * The same spliterator class walks both keys (via
 * {@link TxnFastHashBase#keySpliterator()}) and values (via
 * {@link TxnFastHashMap#valueSpliterator()}). The mem maps/sets reject
 * {@code null} keys and values, and dead slots are skipped via the
 * {@code deleted} bitmap, so every yielded element is non-null: the class
 * declares {@code NONNULL}, matching its non-CoW twin
 * {@link org.apache.jena.mem.spliterator.SparseArraySpliterator}.
 * <p>
 * It also declares {@code DISTINCT} (the underlying set guarantees distinct
 * keys, and the value walk is over distinct slots). It must <i>not</i>
 * declare {@code ORDERED}: although iteration walks the slice
 * deterministically from high index down to low, the indexed-set family does
 * not promise callers a stable encounter order (sibling spliterators/iterators
 * may walk in the opposite direction). It must <i>not</i> declare
 * {@code IMMUTABLE} (the owning collection can still be structurally mutated
 * by a writer for the duration of a write transaction).
 */
public class SparseTombstoneSpliteratorTest {

    private static final class StringSet extends TxnFastHashSet<String> {
        @Override protected String[] newKeysArray(int size) { return new String[size]; }
    }

    private static final class StringMap extends TxnFastHashMap<String, String> {
        @Override protected String[] newKeysArray(int size)   { return new String[size]; }
        @Override protected String[] newValuesArray(int size) { return new String[size]; }
    }

    @Test
    public void keySpliteratorDeclaresDistinctNonNullNotOrderedNotImmutable() {
        StringSet s = new StringSet();
        s.tryAdd("a");
        s.tryAdd("b");
        Spliterator<String> spl = s.keySpliterator();
        assertTrue("DISTINCT expected", spl.hasCharacteristics(Spliterator.DISTINCT));
        assertTrue("NONNULL expected (null keys rejected)",
                spl.hasCharacteristics(Spliterator.NONNULL));
        assertFalse("ORDERED forbidden (family does not promise encounter order)",
                spl.hasCharacteristics(Spliterator.ORDERED));
        assertFalse("IMMUTABLE forbidden (writer may mutate)",
                spl.hasCharacteristics(Spliterator.IMMUTABLE));
    }

    @Test
    public void valueSpliteratorDeclaresDistinctNonNullNotOrdered() {
        StringMap m = new StringMap();
        m.put("a", "A");
        m.put("b", "B");                   // null values are rejected (assert)
        Spliterator<String> spl = m.valueSpliterator();
        assertTrue("DISTINCT expected", spl.hasCharacteristics(Spliterator.DISTINCT));
        assertTrue("NONNULL expected (null values rejected)",
                spl.hasCharacteristics(Spliterator.NONNULL));
        assertFalse("ORDERED forbidden (family does not promise encounter order)",
                spl.hasCharacteristics(Spliterator.ORDERED));
    }

    @Test
    public void exactCharacteristicsMask() {
        // Belt-and-braces: pin the exact bitset so a future change to either
        // direction (adding/removing a flag) breaks this test loudly.
        StringSet s = new StringSet();
        s.tryAdd("a");
        Spliterator<String> spl = s.keySpliterator();
        assertEquals(Spliterator.DISTINCT | Spliterator.NONNULL,
                spl.characteristics());
    }

    // ----- iteration behaviour (trySplit / forEachRemaining / CME) -----

    private static StringSet setOf(String... keys) {
        StringSet s = new StringSet();
        for (String k : keys) s.tryAdd(k);
        return s;
    }

    @Test
    public void parallelStreamYieldsEveryLiveElementOnce() {
        StringSet s = new StringSet();
        for (int i = 0; i < 1000; i++) s.tryAdd("k" + i);
        for (int i = 0; i < 1000; i += 3) s.tryRemove("k" + i);   // scatter tombstones

        // A parallel stream drives trySplit() recursively; the result must be
        // exactly the live set, with nothing dropped or duplicated.
        Set<String> collected = StreamSupport.stream(s.keySpliterator(), true)
                .collect(Collectors.toCollection(HashSet::new));

        Set<String> expected = new HashSet<>();
        for (int i = 0; i < 1000; i++) if (i % 3 != 0) expected.add("k" + i);
        assertEquals(expected, collected);
        assertEquals(s.size(), collected.size());
    }

    @Test
    public void trySplitCoversDisjointHalves() {
        StringSet s = setOf("a", "b", "c", "d", "e", "f", "g", "h");
        s.tryRemove("c");
        s.tryRemove("f");

        Spliterator<String> a = s.keySpliterator();
        Spliterator<String> b = a.trySplit();
        assertNotNull("a set of this size must split", b);

        Set<String> seen = new HashSet<>();
        a.forEachRemaining(seen::add);
        b.forEachRemaining(seen::add);
        assertEquals(Set.of("a", "b", "d", "e", "g", "h"), seen);
    }

    @Test
    public void forEachRemainingSkipsTombstones() {
        StringSet s = setOf("a", "b", "c", "d");
        s.tryRemove("b");

        Set<String> seen = new HashSet<>();
        s.keySpliterator().forEachRemaining(seen::add);
        assertEquals(Set.of("a", "c", "d"), seen);
    }

    @Test
    public void structuralModificationThrowsOnForEachRemaining() {
        StringSet s = setOf("a", "b");
        Spliterator<String> spl = s.keySpliterator();   // snapshots size
        s.tryAdd("c");
        assertThrows(ConcurrentModificationException.class,
                () -> spl.forEachRemaining(x -> {}));
    }

    @Test
    public void structuralModificationThrowsOnTryAdvance() {
        StringSet s = setOf("a", "b");
        Spliterator<String> spl = s.keySpliterator();
        s.tryRemove("a");
        assertThrows(ConcurrentModificationException.class,
                () -> spl.tryAdvance(x -> {}));
    }
}

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

import org.apache.jena.util.iterator.ExtendedIterator;
import org.junit.Test;

import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Tests for {@link SparseTombstoneIterator}, the iteration primitive of the
 * copy-on-write collections. Exercised through {@link TxnFastHashSet#keyIterator()}
 * so the iterator is driven exactly as production code drives it. Covers the
 * three behaviours not pinned elsewhere: tombstone skipping (via both
 * {@code hasNext}/{@code next} and {@code forEachRemaining}), the
 * {@code hasNext}-then-{@code forEachRemaining} hand-off (no element lost or
 * duplicated), and the concurrent-modification tripwire.
 */
public class SparseTombstoneIteratorTest {

    private static final class StringSet extends TxnFastHashSet<String> {
        @Override protected String[] newKeysArray(int size) { return new String[size]; }
    }

    private static StringSet setOf(String... keys) {
        StringSet s = new StringSet();
        for (String k : keys) s.tryAdd(k);
        return s;
    }

    @Test
    public void nextAndHasNextSkipTombstones() {
        StringSet s = setOf("a", "b", "c", "d");
        s.tryRemove("b");

        Set<String> seen = new HashSet<>();
        ExtendedIterator<String> it = s.keyIterator();
        while (it.hasNext()) seen.add(it.next());

        assertEquals(Set.of("a", "c", "d"), seen);
    }

    @Test
    public void forEachRemainingSkipsTombstones() {
        StringSet s = setOf("a", "b", "c", "d");
        s.tryRemove("b");
        s.tryRemove("d");

        Set<String> seen = new HashSet<>();
        s.keyIterator().forEachRemaining(seen::add);

        assertEquals(Set.of("a", "c"), seen);
    }

    @Test
    public void hasNextThenForEachRemainingYieldsEveryLiveElementOnce() {
        StringSet s = setOf("a", "b", "c");

        ExtendedIterator<String> it = s.keyIterator();
        assertTrue(it.hasNext());               // caches a peeked element

        Set<String> seen = new HashSet<>();
        it.forEachRemaining(seen::add);         // must still yield the peeked one
        assertEquals(Set.of("a", "b", "c"), seen);
    }

    @Test
    public void emptySetIteratesToNothing() {
        ExtendedIterator<String> it = new StringSet().keyIterator();
        assertFalse(it.hasNext());
    }

    @Test
    public void structuralModificationAfterCreationThrowsOnNext() {
        StringSet s = setOf("a", "b");
        ExtendedIterator<String> it = s.keyIterator();   // snapshots size at construction
        s.tryAdd("c");                                   // structural change
        assertThrows(ConcurrentModificationException.class, it::next);
    }

    @Test
    public void structuralModificationAfterCreationThrowsOnForEachRemaining() {
        StringSet s = setOf("a", "b");
        ExtendedIterator<String> it = s.keyIterator();
        s.tryRemove("a");                                // structural change
        assertThrows(ConcurrentModificationException.class,
                () -> it.forEachRemaining(x -> {}));
    }
}

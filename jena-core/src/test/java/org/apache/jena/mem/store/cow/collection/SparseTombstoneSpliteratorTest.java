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

import java.util.Spliterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Pins the {@code characteristics()} contract of
 * {@link SparseTombstoneSpliterator}.
 * <p>
 * The same spliterator class is used to walk both keys (via
 * {@link TxnFastHashBase#keySpliterator()}, where {@code NONNULL} would be
 * accurate) and values (via {@link TxnFastHashMap#valueSpliterator()},
 * where {@code put(K, null)} is permitted and {@code NONNULL} would be
 * unsafe). The class therefore must <i>not</i> declare {@code NONNULL}.
 * <p>
 * It must declare {@code ORDERED} (iteration walks the slice deterministically
 * from high index down to low) and {@code DISTINCT} (the underlying set
 * guarantees distinct keys, and the value walk is over distinct slots).
 * It must <i>not</i> declare {@code IMMUTABLE} (the owning collection can
 * still be structurally mutated by a writer for the duration of a write
 * transaction).
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
    public void keySpliteratorDeclaresOrderedDistinctNotNonNullNotImmutable() {
        StringSet s = new StringSet();
        s.tryAdd("a");
        s.tryAdd("b");
        Spliterator<String> spl = s.keySpliterator();
        assertTrue("ORDERED expected", spl.hasCharacteristics(Spliterator.ORDERED));
        assertTrue("DISTINCT expected", spl.hasCharacteristics(Spliterator.DISTINCT));
        assertFalse("NONNULL forbidden (shared with value walk)",
                spl.hasCharacteristics(Spliterator.NONNULL));
        assertFalse("IMMUTABLE forbidden (writer may mutate)",
                spl.hasCharacteristics(Spliterator.IMMUTABLE));
    }

    @Test
    public void valueSpliteratorDeclaresOrderedDistinctNotNonNull() {
        StringMap m = new StringMap();
        m.put("a", "A");
        m.put("b", null);                  // null values are permitted
        Spliterator<String> spl = m.valueSpliterator();
        assertTrue("ORDERED expected", spl.hasCharacteristics(Spliterator.ORDERED));
        assertTrue("DISTINCT expected", spl.hasCharacteristics(Spliterator.DISTINCT));
        assertFalse("NONNULL forbidden (null values permitted)",
                spl.hasCharacteristics(Spliterator.NONNULL));
    }

    @Test
    public void exactCharacteristicsMask() {
        // Belt-and-braces: pin the exact bitset so a future change to either
        // direction (adding/removing a flag) breaks this test loudly.
        StringSet s = new StringSet();
        s.tryAdd("a");
        Spliterator<String> spl = s.keySpliterator();
        assertEquals(Spliterator.ORDERED | Spliterator.DISTINCT,
                spl.characteristics());
    }
}

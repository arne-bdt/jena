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

package org.apache.jena.mem.store.cow;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.mem.store.indexed.IndexList;
import org.junit.Test;

import static org.apache.jena.testing_framework.GraphHelper.node;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link TxnNodesToIndices}. The map's behaviour is largely
 * inherited from {@link org.apache.jena.mem.store.cow.collection.TxnFastHashMap},
 * which has dedicated tests; this class focuses on the {@link IndexList}
 * value type and the fork / deep-copy semantics.
 */
public class TxnNodesToIndicesTest {

    private static Node n(String s) {
        return node("" + s);
    }

    @Test
    public void putGetRemove() {
        TxnNodesToIndices m = new TxnNodesToIndices();
        Node a = n("a");
        IndexList list = new IndexList();
        list.add(0); list.add(1);

        m.put(a, list);
        assertSame(list, m.get(a));
        assertEquals(1, m.size());

        assertTrue(m.tryRemove(a));
        assertNull(m.get(a));
        assertEquals(0, m.size());
    }

    @Test
    public void forkSharesListReferencesUntilReplaced() {
        // The values[] array holds IndexList references shared between
        // source and fork. The fork doesn't clone them — clone-on-touch
        // is the working-copy layer's responsibility (above this class).
        TxnNodesToIndices src = new TxnNodesToIndices();
        Node a = n("a");
        IndexList list = new IndexList();
        list.add(7);
        src.put(a, list);

        TxnNodesToIndices fork = src.fork();
        // Pre-replacement: both views resolve a to the same IndexList instance.
        assertSame(list, src.get(a));
        assertSame(list, fork.get(a));

        // Fork installs a different IndexList for the same key (the
        // working-copy layer would do this after cloning to mutate).
        IndexList replacement = new IndexList();
        replacement.add(7); replacement.add(99);
        fork.put(a, replacement);

        // Source still observes the original list; fork sees the replacement.
        assertSame(list, src.get(a));
        assertSame(replacement, fork.get(a));
    }

    @Test
    public void forkInsertionsAndRemovalsAreIsolated() {
        TxnNodesToIndices src = new TxnNodesToIndices();
        for (int i = 0; i < 10; i++) {
            IndexList l = new IndexList();
            l.add(i);
            src.put(n("k" + i), l);
        }

        TxnNodesToIndices fork = src.fork();
        for (int i = 0; i < 10; i += 2) fork.tryRemove(n("k" + i));
        IndexList newList = new IndexList();
        newList.add(42);
        fork.put(n("brand-new"), newList);

        // Source still has all 10 originals and no brand-new.
        assertEquals(10, src.size());
        for (int i = 0; i < 10; i++) assertNotNull(src.get(n("k" + i)));
        assertNull(src.get(n("brand-new")));

        // Fork: 5 originals + 1 new.
        assertEquals(6, fork.size());
        for (int i = 0; i < 10; i++) {
            if (i % 2 == 0) assertNull(fork.get(n("k" + i)));
            else assertNotNull(fork.get(n("k" + i)));
        }
        assertSame(newList, fork.get(n("brand-new")));
    }

    @Test
    public void copyDeepCopiesListValuesAndIsIndependent() {
        TxnNodesToIndices src = new TxnNodesToIndices();
        Node a = n("a"), b = n("b");
        IndexList listA = new IndexList(); listA.add(7); listA.add(8);
        IndexList listB = new IndexList(); listB.add(1);
        src.put(a, listA);
        src.put(b, listB);

        TxnNodesToIndices copy = src.copy();

        // Values are independent clones, not shared references (contrast
        // forkSharesListReferencesUntilReplaced above).
        assertNotSame(listA, copy.get(a));
        assertNotSame(listB, copy.get(b));
        assertEquals(2, copy.get(a).size());
        assertEquals(7, copy.get(a).getIndexAt(0));
        assertEquals(8, copy.get(a).getIndexAt(1));

        // Mutating a value through the copy must not touch the source's list.
        copy.get(a).add(99);
        assertEquals(2, src.get(a).size());
        assertEquals(3, copy.get(a).size());

        // Structural independence in BOTH directions — fork() forbids
        // mutating the source, copy() does not.
        copy.tryRemove(b);
        src.put(n("c"), new IndexList());
        assertEquals(3, src.size());          // a, b, c
        assertNotNull(src.get(b));
        assertEquals(1, copy.size());         // a only (b removed, c never added)
        assertNull(copy.get(n("c")));
    }
}

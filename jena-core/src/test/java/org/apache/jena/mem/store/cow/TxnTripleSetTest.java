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

import org.apache.jena.graph.Triple;
import org.junit.Test;

import static org.apache.jena.testing_framework.GraphHelper.triple;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link TxnTripleSet}. The bulk of the behaviour is already
 * covered by {@code TxnFastHashSetTest}; these tests focus on the additions
 * specific to {@code TxnTripleSet}: the keys-length accessor and the fork
 * semantics.
 */
public class TxnTripleSetTest {

    @Test
    public void getInternalKeysLengthReportsCurrentCapacity() {
        TxnTripleSet ts = new TxnTripleSet();
        int initial = ts.getInternalKeysLength();
        // Insert enough to force at least one grow; the reported length
        // must increase past the initial capacity.
        for (int i = 0; i < initial * 4; i++) {
            ts.tryAdd(triple("s" + i + " p o"));
        }
        assertTrue("keys.length must grow past the initial capacity",
                ts.getInternalKeysLength() > initial);
    }

    @Test
    public void forkIsolationForTriples() {
        TxnTripleSet src = new TxnTripleSet();
        Triple a = triple("a p o"), b = triple("b p o"), c = triple("c p o");
        src.tryAdd(a); src.tryAdd(b);

        TxnTripleSet fork = src.fork();
        fork.tryAdd(c);
        fork.tryRemove(a);

        assertTrue(src.containsKey(a));
        assertTrue(src.containsKey(b));
        assertFalse(src.containsKey(c));

        assertFalse(fork.containsKey(a));
        assertTrue(fork.containsKey(b));
        assertTrue(fork.containsKey(c));
    }

    @Test
    public void copyIsIndependentDeepCopy() {
        TxnTripleSet src = new TxnTripleSet();
        Triple a = triple("a p o"), b = triple("b p o"), d = triple("d p o");
        src.tryAdd(a);

        TxnTripleSet copy = src.copy();

        // Unlike fork(), copy() leaves BOTH instances independently mutable.
        // Mutating the copy must not touch the source ...
        copy.tryAdd(b);
        assertFalse("source must not see the copy's additions", src.containsKey(b));

        // ... and (the stronger guarantee fork() does NOT make) mutating the
        // source must not touch the copy: fork() requires the source to be
        // frozen, copy() does not.
        src.tryAdd(d);
        assertFalse("copy must not see the source's later additions", copy.containsKey(d));

        assertTrue(src.containsKey(a));
        assertTrue(src.containsKey(d));
        assertTrue(copy.containsKey(a));
        assertTrue(copy.containsKey(b));
    }

    @Test
    public void copyAfterChurnPreservesSurvivorsAndStaysIndependent() {
        TxnTripleSet src = new TxnTripleSet();
        final int N = 64;
        for (int i = 0; i < N; i++) src.tryAdd(triple("s" + i + " p o" + i));
        // Remove half (scatter tombstones), then add more to force grow/compaction.
        for (int i = 0; i < N; i += 2) src.tryRemove(triple("s" + i + " p o" + i));
        for (int i = N; i < N + 32; i++) src.tryAdd(triple("s" + i + " p o" + i));

        TxnTripleSet copy = src.copy();

        // The copy sees exactly the survivors, despite the tombstones and grow.
        assertEquals(src.size(), copy.size());
        for (int i = 1; i < N; i += 2) assertTrue(copy.containsKey(triple("s" + i + " p o" + i)));
        for (int i = N; i < N + 32; i++) assertTrue(copy.containsKey(triple("s" + i + " p o" + i)));
        for (int i = 0; i < N; i += 2) assertFalse(copy.containsKey(triple("s" + i + " p o" + i)));

        // Independent both ways after the churn.
        copy.tryAdd(triple("brand p new"));
        assertFalse(src.containsKey(triple("brand p new")));
        src.tryRemove(triple("s1 p o1"));
        assertTrue("copy must not see the source's later removal",
                copy.containsKey(triple("s1 p o1")));
    }
}

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
    public void copyDelegatesToFork() {
        TxnTripleSet src = new TxnTripleSet();
        src.tryAdd(triple("a p o"));

        TxnTripleSet copy = src.copy();
        copy.tryAdd(triple("b p o"));

        // Same fork semantics: source unaffected by copy's mutations.
        assertFalse(src.containsKey(triple("b p o")));
        assertTrue(copy.containsKey(triple("a p o")));
        assertTrue(copy.containsKey(triple("b p o")));
    }
}

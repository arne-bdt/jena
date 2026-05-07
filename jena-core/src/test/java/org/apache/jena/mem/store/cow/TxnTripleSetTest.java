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

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link TxnTripleSet}. The bulk of the behaviour is already
 * covered by {@code TxnFastHashSetTest}; these tests focus on the additions
 * specific to {@code TxnTripleSet}: the grow hook and the fork semantics.
 */
public class TxnTripleSetTest {

    private static Triple t(String s) {
        return Triple.create(NodeFactory.createURI("http://ex/" + s),
                             NodeFactory.createURI("http://ex/p"),
                             NodeFactory.createURI("http://ex/o"));
    }

    @Test
    public void onKeysGrowHookFiresOnGrow() {
        TxnTripleSet ts = new TxnTripleSet();
        List<Integer> grownLengths = new ArrayList<>();
        ts.setOnKeysGrowHook(grownLengths::add);

        int initial = ts.getInternalKeysLength();
        // Insert enough to force at least one grow.
        for (int i = 0; i < initial * 4; i++) {
            ts.tryAdd(t("s" + i));
        }
        assertFalse("hook should fire at least once across multiple grows", grownLengths.isEmpty());
        assertTrue("each fired length must exceed the initial capacity",
                grownLengths.stream().allMatch(l -> l > initial));
    }

    @Test
    public void forkDoesNotInheritGrowHook() {
        // The grow hook is writer-private — the fork must NOT inherit the
        // source's hook (that hook is wired to the source's parallel arrays,
        // not to anything the fork knows about).
        TxnTripleSet src = new TxnTripleSet();
        boolean[] sourceFired = {false};
        src.setOnKeysGrowHook(len -> sourceFired[0] = true);

        TxnTripleSet fork = src.fork();
        // Drive the fork through several grows; the source's hook must not fire.
        for (int i = 0; i < 1000; i++) fork.tryAdd(t("k" + i));

        assertFalse("source's hook must not fire on fork's growth", sourceFired[0]);
    }

    @Test
    public void forkIsolationForTriples() {
        TxnTripleSet src = new TxnTripleSet();
        Triple a = t("a"), b = t("b"), c = t("c");
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
        src.tryAdd(t("a"));

        TxnTripleSet copy = src.copy();
        copy.tryAdd(t("b"));

        // Same fork semantics: source unaffected by copy's mutations.
        assertFalse(src.containsKey(t("b")));
        assertTrue(copy.containsKey(t("a")));
        assertTrue(copy.containsKey(t("b")));
    }
}

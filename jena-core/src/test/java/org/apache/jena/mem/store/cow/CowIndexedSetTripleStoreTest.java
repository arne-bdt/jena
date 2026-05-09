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

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.jena.testing_framework.GraphHelper.triple;
import static org.junit.Assert.*;

/**
 * Direct tests for {@link CowWriteTxn} at the store layer
 * (the graph-level lifecycle is tested separately by
 * {@code GraphMemIndexedSetCowTxnTest}). Covers:
 * <ul>
 *   <li>The basic {@code TripleStore} contract on a freshly-built store.
 *   <li>Equivalence between {@link CowWriteTxn#forkForWrite()}
 *       and {@link CowWriteTxn#forkForWriteParallel()} — both
 *       must produce a mutated fork that reaches the same end state for
 *       any sequence of operations and must leave the source untouched.
 * </ul>
 * Heavier snapshot-stability guarantees are exercised by
 * {@code CowIndexedSetTripleStoreFuzzTest}.
 */
public class CowIndexedSetTripleStoreTest {


    private static Set<Triple> drain(CowWriteTxn s) {
        return s.stream().collect(Collectors.toCollection(HashSet::new));
    }

    @Test
    public void emptyStoreIsEmpty() {
        CowWriteTxn s = new CowWriteTxn();
        assertTrue(s.isEmpty());
        assertEquals(0, s.countTriples());
        assertFalse(s.contains(triple("a b c")));
    }

    @Test
    public void addRemoveContains() {
        CowWriteTxn s = new CowWriteTxn();
        s.add(triple("a b c"));
        s.add(triple("a b d"));
        assertEquals(2, s.countTriples());
        assertTrue(s.contains(triple("a b c")));
        assertFalse(s.contains(triple("a b missing")));

        s.remove(triple("a b c"));
        assertEquals(1, s.countTriples());
        assertFalse(s.contains(triple("a b c")));
        assertTrue(s.contains(triple("a b d")));
    }

    @Test
    public void patternMatchAcrossAllEightCases() {
        CowWriteTxn s = new CowWriteTxn();
        Triple t1 = triple("s1 p o1");
        Triple t2 = triple("s1 p o2");
        Triple t3 = triple("s2 p o1");
        s.add(t1); s.add(t2); s.add(t3);

        // SUB_PRE_OBJ and ANY_ANY_ANY are exercised via existing graph tests;
        // here we verify each of the six index-driven patterns.
        assertEquals(2, s.stream(Triple.createMatch(t1.getSubject(), null, null)).count());
        assertEquals(3, s.stream(Triple.createMatch(null, t1.getPredicate(), null)).count());
        assertEquals(2, s.stream(Triple.createMatch(null, null, t1.getObject())).count());
        assertEquals(2, s.stream(Triple.createMatch(t1.getSubject(), t1.getPredicate(), null)).count());
        assertEquals(2, s.stream(Triple.createMatch(null, t1.getPredicate(), t1.getObject())).count());
        assertEquals(1, s.stream(Triple.createMatch(t1.getSubject(), null, t1.getObject())).count());
    }

    @Test
    public void forkForWriteAndForkForWriteParallel_produceEquivalentResults() {
        // Build a non-trivial source store.
        Random rnd = new Random(123L);
        CowWriteTxn source = new CowWriteTxn();
        Set<Triple> seeds = new HashSet<>();
        for (int i = 0; i < 500; i++) {
            Triple x = triple("s" + rnd.nextInt(100)
                    + " p" + rnd.nextInt(20)
                    + " o" + rnd.nextInt(100));
            if (seeds.add(x)) source.add(x);
        }
        Set<Triple> sourceContents = drain(source);

        // Fork both ways; apply the same mutation script to each fork.
        CowWriteTxn seq = source.forkForWrite();
        CowWriteTxn par = source.forkForWriteParallel();

        Random opsRnd = new Random(456L);
        for (int i = 0; i < 200; i++) {
            Triple x = triple("s" + opsRnd.nextInt(100)
                    + " p" + opsRnd.nextInt(20)
                    + " o" + opsRnd.nextInt(100));
            if (opsRnd.nextBoolean()) {
                seq.add(x);
                par.add(x);
            } else {
                seq.remove(x);
                par.remove(x);
            }
        }

        // Forks must agree.
        assertEquals(seq.countTriples(), par.countTriples());
        assertEquals(drain(seq), drain(par));

        // Source must be unchanged by either fork's activity.
        assertEquals(sourceContents, drain(source));
    }

    @Test
    public void forkForWriteParallel_preservesSnapshotIsolation() {
        // Same property as for the sequential fork: writes to the parallel
        // fork must not leak into the source.
        CowWriteTxn source = new CowWriteTxn();
        for (int i = 0; i < 200; i++) source.add(triple("s" + i + " " + "p" + " " + "o"));
        Set<Triple> srcSnapshot = drain(source);

        CowWriteTxn par = source.forkForWriteParallel();
        for (int i = 0; i < 100; i++) par.remove(triple("s" + i + " " + "p" + " " + "o"));
        for (int i = 0; i < 50; i++) par.add(triple("new" + i + " " + "p" + " " + "o"));

        assertEquals("source view drifted under parallel fork", srcSnapshot, drain(source));
        assertEquals(150, par.countTriples());
    }

    @Test
    public void clearResetsToEmptyAndAllowsFurtherUse() {
        CowWriteTxn s = new CowWriteTxn();
        for (int i = 0; i < 50; i++) s.add(triple("s" + i + " " + "p" + " " + "o"));
        s.clear();
        assertTrue(s.isEmpty());
        assertEquals(0, s.countTriples());
        s.add(triple("post clear ok"));
        assertEquals(1, s.countTriples());
        assertTrue(s.contains(triple("post clear ok")));
    }
}

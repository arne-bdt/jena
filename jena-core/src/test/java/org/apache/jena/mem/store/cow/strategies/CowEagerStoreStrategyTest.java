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

package org.apache.jena.mem.store.cow.strategies;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.IndexingStrategy;
import org.apache.jena.mem.pattern.MatchPattern;
import org.apache.jena.mem.store.cow.CowWriteTxn;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.jena.testing_framework.GraphHelper.node;
import static org.apache.jena.testing_framework.GraphHelper.triple;
import static org.junit.Assert.*;

/**
 * Direct unit tests for {@link CowEagerStoreStrategy}. Most of the
 * machinery is exercised end-to-end through {@code CowWriteTxnTest} and
 * the strategy-correctness suites; the cases here pin down a couple of
 * properties of the strategy in isolation:
 * <ul>
 *   <li>{@code addToIndex} resizes its reverse-index arrays inline as
 *       the writer's keys array grows. The earlier design used a
 *       {@code setOnKeysGrowHook} callback for this; the current design
 *       resizes inline (one length compare on the hot path), which
 *       removes a writer-private callback field from the shared triple
 *       set.
 *   <li>The eager strategy and the minimal strategy must return the
 *       same triple set for every partial pattern over the same data.
 * </ul>
 */
public class CowEagerStoreStrategyTest {

    private static Node n(String s) {
        return node("" + s);
    }

    /**
     * Drive the writer through enough triples to force several
     * keys-array grows. With {@code addToIndex} resizing the
     * reverse-index arrays inline, the writes must succeed and lookups
     * must return correct results — without any explicit hook wiring.
     */
    @Test
    public void addToIndexResizesReverseArraysInlineAsKeysGrow() {
        CowWriteTxn store = new CowWriteTxn(IndexingStrategy.EAGER);
        final int N = 256;
        for (int i = 0; i < N; i++) {
            store.add(triple("s" + i + " p o" + i));
        }
        for (int i = 0; i < N; i++) {
            Triple match = Triple.createMatch(n("s" + i), null, null);
            assertEquals("eager must find each subject exactly once",
                    1, store.stream(match).count());
        }
    }

    @Test
    public void minimalAndEagerAgreeOnSimplePartialPatterns() {
        // Sanity that the two strategies agree on the data they return for
        // partial patterns.
        CowWriteTxn eager = new CowWriteTxn(IndexingStrategy.EAGER);
        CowWriteTxn minimal = new CowWriteTxn(IndexingStrategy.MINIMAL);
        for (int i = 0; i < 20; i++) {
            Triple x = triple("s" + i + " p" + (i % 3) + " o" + i);
            eager.add(x);
            minimal.add(x);
        }
        for (MatchPattern p : MatchPattern.values()) {
            if (p == MatchPattern.SUB_PRE_OBJ || p == MatchPattern.ANY_ANY_ANY)
                continue;
            Triple match = switch (p) {
                case SUB_ANY_ANY -> Triple.createMatch(n("s1"), null, null);
                case ANY_PRE_ANY -> Triple.createMatch(null, n("p1"), null);
                case ANY_ANY_OBJ -> Triple.createMatch(null, null, n("o2"));
                case SUB_PRE_ANY -> Triple.createMatch(n("s4"), n("p1"), null);
                case ANY_PRE_OBJ -> Triple.createMatch(null, n("p1"), n("o4"));
                case SUB_ANY_OBJ -> Triple.createMatch(n("s7"), null, n("o7"));
                default -> throw new IllegalStateException();
            };
            Set<Triple> e = eager.stream(match).collect(Collectors.toCollection(HashSet::new));
            Set<Triple> m = minimal.stream(match).collect(Collectors.toCollection(HashSet::new));
            assertEquals("eager and minimal must agree on " + p, m, e);
        }
    }
}

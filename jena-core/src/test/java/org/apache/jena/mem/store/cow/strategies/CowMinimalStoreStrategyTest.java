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

import org.apache.jena.graph.Triple;
import org.apache.jena.mem.pattern.MatchPattern;
import org.apache.jena.mem.store.cow.TxnTripleSet;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.jena.testing_framework.GraphHelper.node;
import static org.apache.jena.testing_framework.GraphHelper.triple;
import static org.junit.Assert.*;

/**
 * Sanity tests for {@link CowMinimalStoreStrategy}: it implements the
 * {@link CowStoreStrategy} pattern-match methods with a uniform linear
 * scan, ignoring the {@code MatchPattern} argument. The tests below pin
 * down that "ignored" property: passing two different patterns must yield
 * the same set of results for the same probe triple.
 */
public class CowMinimalStoreStrategyTest {


    private static Set<Triple> drain(java.util.stream.Stream<Triple> s) {
        return s.collect(Collectors.toCollection(HashSet::new));
    }

    @Test
    public void streamMatchIgnoresPatternArgument() {
        TxnTripleSet triples = new TxnTripleSet();
        for (int i = 0; i < 10; i++) {
            triples.tryAdd(triple("s" + i + " " + "p" + " " + "o" + i));
        }
        CowMinimalStoreStrategy strategy = new CowMinimalStoreStrategy(triples);

        Triple probe = Triple.createMatch(
                node("s3"), null, null);

        // Use two different MatchPattern values for the same probe; the
        // strategy must produce the same result either way.
        Set<Triple> a = drain(strategy.streamMatch(probe, MatchPattern.SUB_ANY_ANY));
        Set<Triple> b = drain(strategy.streamMatch(probe, MatchPattern.ANY_ANY_OBJ));

        assertEquals("MinimalStrategy must ignore the pattern argument", a, b);
        assertEquals(1, a.size());
        assertTrue(a.contains(triple("s3 p o3")));
    }

    @Test
    public void containsMatchIgnoresPatternArgument() {
        TxnTripleSet triples = new TxnTripleSet();
        for (int i = 0; i < 5; i++) {
            triples.tryAdd(triple("s" + i + " " + "p" + " " + "o" + i));
        }
        CowMinimalStoreStrategy strategy = new CowMinimalStoreStrategy(triples);

        Triple probe = Triple.createMatch(
                node("s2"), null, null);
        assertTrue(strategy.containsMatch(probe, MatchPattern.SUB_ANY_ANY));
        assertTrue(strategy.containsMatch(probe, MatchPattern.ANY_PRE_OBJ));
    }
}

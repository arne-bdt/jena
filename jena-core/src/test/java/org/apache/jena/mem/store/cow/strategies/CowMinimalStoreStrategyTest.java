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
import org.apache.jena.mem.store.cow.TxnTripleSet;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.jena.testing_framework.GraphHelper.node;
import static org.apache.jena.testing_framework.GraphHelper.triple;
import static org.junit.Assert.*;

/**
 * Sanity tests for {@link CowMinimalStoreStrategy}: it answers the
 * {@link CowStoreStrategy} partial-pattern methods with a uniform linear
 * scan over the triple set (it builds no index). The tests below check that
 * each split method filters to the right triples.
 */
public class CowMinimalStoreStrategyTest {


    private static Set<Triple> drain(java.util.stream.Stream<Triple> s) {
        return s.collect(Collectors.toCollection(HashSet::new));
    }

    @Test
    public void streamMatchesByPartialPattern() {
        TxnTripleSet triples = new TxnTripleSet();
        for (int i = 0; i < 10; i++) {
            triples.tryAdd(triple("s" + i + " p o" + i));
        }
        CowMinimalStoreStrategy strategy = new CowMinimalStoreStrategy(triples);

        // SUB_ANY_ANY: only s3's triple.
        Set<Triple> bySubject = drain(strategy.streamSubAnyAny(node("s3")));
        assertEquals(1, bySubject.size());
        assertTrue(bySubject.contains(triple("s3 p o3")));

        // ANY_PRE_ANY: the shared predicate matches all ten.
        assertEquals(10, drain(strategy.streamAnyPreAny(node("p"))).size());

        // ANY_ANY_OBJ: only o7's triple.
        Set<Triple> byObject = drain(strategy.streamAnyAnyObj(node("o7")));
        assertEquals(1, byObject.size());
        assertTrue(byObject.contains(triple("s7 p o7")));
    }

    @Test
    public void containsMatchesByPartialPattern() {
        TxnTripleSet triples = new TxnTripleSet();
        for (int i = 0; i < 5; i++) {
            triples.tryAdd(triple("s" + i + " p o" + i));
        }
        CowMinimalStoreStrategy strategy = new CowMinimalStoreStrategy(triples);

        assertTrue(strategy.containsSubAnyAny(node("s2")));
        assertTrue(strategy.containsAnyPreObj(node("p"), node("o2")));    // s2 p o2 exists
        assertFalse(strategy.containsAnyPreObj(node("p"), node("oX")));   // no such object
        assertFalse(strategy.containsSubAnyAny(node("nope")));
    }

    @Test
    public void findMatchesByPartialPattern() {
        TxnTripleSet triples = new TxnTripleSet();
        for (int i = 0; i < 10; i++) {
            triples.tryAdd(triple("s" + i + " p o" + i));
        }
        CowMinimalStoreStrategy strategy = new CowMinimalStoreStrategy(triples);

        assertEquals(Set.of(triple("s3 p o3")), strategy.findSubAnyAny(node("s3")).toSet());
        assertEquals(10, strategy.findAnyPreAny(node("p")).toList().size());
        assertEquals(Set.of(triple("s7 p o7")), strategy.findAnyAnyObj(node("o7")).toSet());
        assertEquals(Set.of(triple("s5 p o5")), strategy.findSubPreAny(node("s5"), node("p")).toSet());
        assertEquals(Set.of(triple("s5 p o5")), strategy.findSubAnyObj(node("s5"), node("o5")).toSet());
        assertEquals(Set.of(triple("s5 p o5")), strategy.findAnyPreObj(node("p"), node("o5")).toSet());
        assertFalse(strategy.findSubAnyAny(node("nope")).hasNext());
    }
}

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
package org.apache.jena.mem;

import org.junit.Test;

import static org.apache.jena.testing_framework.GraphHelper.node;
import static org.apache.jena.testing_framework.GraphHelper.triple;
import static org.junit.Assert.*;

/**
 * Behaviour of {@link GraphMemMvcc} under the {@link IndexingStrategy#MANUAL MANUAL}
 * strategy, which the parameterized {@link GraphMemMvccTest} cannot cover because
 * partial-pattern lookups throw until the index is explicitly built.
 * <p>
 * Per the {@link IndexingStrategy#MANUAL} contract: partial patterns throw
 * {@link UnsupportedOperationException} until {@link GraphMemMvcc#initializeIndex()}
 * is called; fully-concrete (SPO) lookups and fully-unbound "find all" / size /
 * stream-all never need the index and work throughout.
 */
public class GraphMemMvccManualTest {

    private static GraphMemMvcc populated() {
        final GraphMemMvcc g = new GraphMemMvcc(IndexingStrategy.MANUAL);
        g.add(triple("s1 p o1"));
        g.add(triple("s1 p2 o"));
        g.add(triple("s2 p o2"));
        return g;
    }

    @Test
    public void partialPatternLookupsThrowBeforeInit() {
        final GraphMemMvcc g = populated();
        assertFalse(g.isIndexInitialized());

        assertThrows(UnsupportedOperationException.class,
                () -> g.contains(node("s1"), null, null));
        assertThrows(UnsupportedOperationException.class,
                () -> g.find(node("s1"), null, null).toList());
        assertThrows(UnsupportedOperationException.class,
                () -> g.stream(node("s1"), null, null).count());
    }

    @Test
    public void fullyConcreteAndFindAllWorkBeforeInit() {
        final GraphMemMvcc g = populated();
        assertFalse(g.isIndexInitialized());

        // Fully-concrete lookups bypass the auxiliary index.
        assertTrue(g.contains(triple("s1 p o1")));
        assertFalse(g.contains(triple("s1 p missing")));

        // find-all / size / stream-all never use the index.
        assertEquals(3, g.size());
        assertEquals(3, g.find(null, null, null).toList().size());
        assertEquals(3, g.stream().count());
    }

    @Test
    public void partialPatternLookupsServedAfterInit() {
        final GraphMemMvcc g = populated();

        g.initializeIndex();
        assertTrue(g.isIndexInitialized());

        assertTrue(g.contains(node("s1"), null, null));
        assertEquals(2, g.find(node("s1"), null, null).toList().size());
        assertEquals(2, g.stream(node("s1"), null, null).count());
        assertEquals(2, g.find(null, node("p"), null).toList().size());
    }

    @Test
    public void partialPatternLookupsServedAfterInitParallel() {
        final GraphMemMvcc g = populated();

        g.initializeIndexParallel();
        assertTrue(g.isIndexInitialized());

        assertTrue(g.contains(node("s1"), null, null));
        assertEquals(2, g.find(node("s1"), null, null).toList().size());
        assertEquals(2, g.stream(node("s1"), null, null).count());
    }

    @Test
    public void clearIndexRevertsToThrowing() {
        final GraphMemMvcc g = populated();
        g.initializeIndex();
        assertTrue(g.isIndexInitialized());
        assertTrue(g.contains(node("s1"), null, null));     // served via the index

        g.clearIndex();
        assertFalse(g.isIndexInitialized());

        // Back to the un-built MANUAL contract: partial patterns throw again,
        // fully-concrete lookups still work.
        assertThrows(UnsupportedOperationException.class,
                () -> g.contains(node("s1"), null, null));
        assertTrue(g.contains(triple("s1 p o1")));
    }
}

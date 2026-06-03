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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;

import static org.apache.jena.testing_framework.GraphHelper.node;
import static org.apache.jena.testing_framework.GraphHelper.triple;
import static org.junit.Assert.*;

/**
 * Runs the shared {@link AbstractGraphMemTest} contract against {@link GraphMemMvcc}
 * for the indexing strategies whose full {@link org.apache.jena.graph.Graph} surface
 * works without an explicit index build: {@link IndexingStrategy#EAGER EAGER} and
 * {@link IndexingStrategy#MINIMAL MINIMAL}.
 * <p>
 * {@link IndexingStrategy#MANUAL MANUAL} is excluded here because, per the
 * {@code MANUAL} contract, partial-pattern lookups throw until
 * {@link GraphMemMvcc#initializeIndex()} is called — which the inherited tests do
 * not do. It is covered separately by {@link GraphMemMvccManualTest}.
 * ({@code LAZY}/{@code LAZY_PARALLEL} are not supported by the MVCC store.)
 */
@RunWith(Parameterized.class)
public class GraphMemMvccTest extends AbstractGraphMemTest {

    @Parameterized.Parameter
    public IndexingStrategy indexingStrategy;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return List.of(
                new Object[]{IndexingStrategy.EAGER},
                new Object[]{IndexingStrategy.MINIMAL});
    }

    @Override
    protected GraphMem createGraph() {
        return new GraphMemMvcc(indexingStrategy);
    }

    private GraphMemMvcc sutAsMvcc() {
        return (GraphMemMvcc) super.sut;
    }

    @Test
    public void testGetIndexingStrategy() {
        assertEquals(indexingStrategy, sutAsMvcc().getIndexingStrategy());
    }

    @Test
    public void testIsIndexInitialized() {
        final var sut = sutAsMvcc();
        assertIndexInitializedMatchesStrategy(sut);

        sut.add(triple("s p o"));
        assertIndexInitializedMatchesStrategy(sut);
    }

    private void assertIndexInitializedMatchesStrategy(GraphMemMvcc sut) {
        switch (sut.getIndexingStrategy()) {
            case EAGER -> assertTrue(sut.isIndexInitialized());
            case MINIMAL -> assertFalse(sut.isIndexInitialized());
            default -> throw new IllegalArgumentException(
                    "Unexpected strategy: " + sut.getIndexingStrategy());
        }
    }

    @Test
    public void testInitializeIndex() {
        final var sut = sutAsMvcc();
        sut.add(triple("s p o"));
        sut.add(triple("s p2 o2"));

        sut.initializeIndex();

        // After initializeIndex the index is built for every supported strategy.
        assertTrue(sut.isIndexInitialized());
        assertEquals(2, sut.find(node("s"), null, null).toList().size());
        assertTrue(sut.contains(triple("s p o")));
    }

    @Test
    public void testInitializeIndexParallel() {
        final var sut = sutAsMvcc();
        sut.add(triple("s p o"));
        sut.add(triple("s p2 o2"));

        sut.initializeIndexParallel();

        assertTrue(sut.isIndexInitialized());
        assertEquals(2, sut.find(node("s"), null, null).toList().size());
        assertTrue(sut.contains(triple("s p o")));
    }

    @Test
    public void testClearIndex() {
        final var sut = sutAsMvcc();
        sut.add(triple("s p o"));
        sut.add(triple("s p2 o2"));
        sut.initializeIndex();

        sut.clearIndex();

        // clearIndex reverts to the configured strategy: EAGER stays indexed,
        // MINIMAL stops serving from an index (but lookups still work via scan).
        switch (indexingStrategy) {
            case EAGER -> assertTrue(sut.isIndexInitialized());
            case MINIMAL -> assertFalse(sut.isIndexInitialized());
            default -> throw new IllegalArgumentException(
                    "Unexpected strategy: " + indexingStrategy);
        }
        assertEquals(2, sut.find(node("s"), null, null).toList().size());
    }

    @Test
    public void testCopyPreservesStrategyAndType() {
        final var sut = sutAsMvcc();
        sut.add(triple("s p o"));

        final GraphMemMvcc copy = sut.copy();
        assertEquals(indexingStrategy, copy.getIndexingStrategy());
        assertTrue(copy.contains(triple("s p o")));

        // Mutations in the copy must not affect the source.
        copy.add(triple("s2 p2 o2"));
        assertFalse(sut.contains(triple("s2 p2 o2")));
        assertTrue(copy.contains(triple("s2 p2 o2")));
    }
}

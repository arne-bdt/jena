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
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.IndexingStrategy;
import org.apache.jena.mem.pattern.MatchPattern;
import org.apache.jena.mem.store.cow.CowIndexedSetTripleStore;
import org.apache.jena.mem.store.cow.TxnTripleSet;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Direct unit tests for {@link CowEagerStoreStrategy} that target the two
 * behaviour changes introduced by the review pass:
 * <ul>
 *   <li>The {@code installGrowHook} constructor flag — when {@code false},
 *       the strategy must not write to the shared
 *       {@link TxnTripleSet#setOnKeysGrowHook} field.
 *   <li>The defensive resize in {@link CowEagerStoreStrategy#addToIndex}
 *       — when no hook is installed, adding a triple at an index past the
 *       current reverse-index capacity must succeed, not throw
 *       {@link ArrayIndexOutOfBoundsException}.
 * </ul>
 */
public class CowEagerStoreStrategyTest {

    private static Triple t(String s, String p, String o) {
        return Triple.create(NodeFactory.createURI("http://ex/" + s),
                             NodeFactory.createURI("http://ex/" + p),
                             NodeFactory.createURI("http://ex/" + o));
    }

    private static Node n(String s) {
        return NodeFactory.createURI("http://ex/" + s);
    }

    private static Object hookFieldOf(TxnTripleSet triples) throws Exception {
        Field f = TxnTripleSet.class.getDeclaredField("onKeysGrowHook");
        f.setAccessible(true);
        return f.get(triples);
    }

    @Test
    public void installGrowHookFalseLeavesHookFieldNull() throws Exception {
        TxnTripleSet triples = new TxnTripleSet();
        // Sanity: brand-new triple set has no hook.
        assertNull(hookFieldOf(triples));

        new CowEagerStoreStrategy(triples, /*parallel*/ false, /*installGrowHook*/ false);

        assertNull("strategy must not install a hook on the triple set",
                hookFieldOf(triples));
    }

    @Test
    public void installGrowHookTrueRegistersTheCallback() throws Exception {
        TxnTripleSet triples = new TxnTripleSet();
        new CowEagerStoreStrategy(triples, /*parallel*/ false, /*installGrowHook*/ true);

        assertNotNull("default constructor must install a hook",
                hookFieldOf(triples));
    }

    /**
     * With the hook skipped, the strategy must still cope when the triple
     * set's keys array grows past the reverse-index arrays' initial size:
     * {@code addToIndex} resizes the arrays on demand.
     */
    @Test
    public void addToIndexResizesReverseArraysOnDemandWhenNoHook() {
        // A store using the (default) hook-installing path; we'll forcibly
        // replace its strategy with a hook-less one to exercise the
        // defensive-resize fallback.
        CowIndexedSetTripleStore store = new CowIndexedSetTripleStore(IndexingStrategy.MANUAL);

        // Pre-load enough triples to make the next add land in a slot that
        // requires a keys-grow.
        final int N = 64;
        for (int i = 0; i < N; i++) {
            store.add(t("s" + i, "p", "o" + i));
        }

        // Build the eager strategy from the loaded store with NO hook.
        // From this point on we drive index population manually via the
        // strategy's API rather than the store, so we can observe the
        // resize-on-demand path even in tests.
        CowEagerStoreStrategy eager =
                new CowEagerStoreStrategy(store.getTriples(),
                        /*parallel*/ false, /*installGrowHook*/ false);

        // Add many more triples directly through the store. The store's
        // strategy slot is still MANUAL (we didn't install ours), so we
        // need to install our hook-less eager via initializeIndex's path —
        // simplest: reflectively write the strategy slot.
        replaceStrategy(store, eager);

        // Now keep adding triples through the store. Each add() routes
        // through addToIndex() on our hook-less eager. Without the
        // resize-on-demand fallback, this would AIOOBE the moment the
        // triple set's keys[] grows past the initial reverse-index length.
        for (int i = N; i < 4 * N; i++) {
            store.add(t("s" + i, "p", "o" + i));
        }

        // Eager-strategy lookups must still return correct answers.
        Triple anyMatch = Triple.createMatch(n("s100"), null, null);
        Set<Triple> got = store.stream(anyMatch).collect(Collectors.toCollection(HashSet::new));
        assertEquals(1, got.size());
        assertTrue(got.contains(t("s100", "p", "o100")));
    }

    private static void replaceStrategy(CowIndexedSetTripleStore store, CowStoreStrategy s) {
        try {
            Field f = CowIndexedSetTripleStore.class.getDeclaredField("strategy");
            f.setAccessible(true);
            @SuppressWarnings("unchecked")
            java.util.concurrent.atomic.AtomicReference<CowStoreStrategy> ref =
                    (java.util.concurrent.atomic.AtomicReference<CowStoreStrategy>) f.get(store);
            ref.set(s);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Many concurrent readers triggering the LAZY → EAGER race-build on the
     * same published snapshot must not race-write the snapshot's
     * {@code onKeysGrowHook} field. After the race settles, the snapshot's
     * triple set's hook field must still be {@code null}.
     */
    @Test
    public void lazyUpgradeOnSnapshotDoesNotMutateSharedHookField() throws Exception {
        CowIndexedSetTripleStore snapshot =
                new CowIndexedSetTripleStore(IndexingStrategy.LAZY);
        for (int i = 0; i < 50; i++) snapshot.add(t("s" + i, "p", "o" + i));

        // Verify pre-conditions.
        assertNull("LAZY does not touch the keys-grow hook",
                hookFieldOf(snapshot.getTriples()));
        assertFalse(snapshot.isIndexInitialized());

        final int numReaders = 8;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(numReaders);
        AtomicReference<Throwable> err = new AtomicReference<>();

        Thread[] readers = new Thread[numReaders];
        for (int i = 0; i < numReaders; i++) {
            readers[i] = new Thread(() -> {
                try {
                    start.await();
                    Triple match = Triple.createMatch(n("s10"), null, null);
                    snapshot.stream(match).count();         // triggers upgrade
                } catch (Throwable th) { err.set(th); }
                finally { done.countDown(); }
            });
            readers[i].start();
        }
        start.countDown();
        done.await();
        if (err.get() != null) throw new AssertionError(err.get());

        // Even though the upgrade ran on every reader, none of them
        // installed a hook on the shared snapshot's TxnTripleSet.
        assertNull("snapshot's keys-grow hook must remain null after a "
                        + "lazy-upgrade race on a published snapshot",
                hookFieldOf(snapshot.getTriples()));
    }

    @Test
    public void minimalAndEagerAgreeOnSimplePartialPatterns() {
        // Sanity that the two strategies agree on the data they return for
        // partial patterns — guards the defensive resize against silently
        // breaking semantics.
        CowIndexedSetTripleStore eager =
                new CowIndexedSetTripleStore(IndexingStrategy.EAGER);
        CowIndexedSetTripleStore minimal =
                new CowIndexedSetTripleStore(IndexingStrategy.MINIMAL);
        for (int i = 0; i < 20; i++) {
            Triple x = t("s" + i, "p" + (i % 3), "o" + i);
            eager.add(x);
            minimal.add(x);
        }
        for (MatchPattern p : MatchPattern.values()) {
            // The pattern enum includes patterns the store handles before
            // the strategy is consulted; we just want a coverage probe.
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

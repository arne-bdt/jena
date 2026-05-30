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
import org.apache.jena.mem.IndexingStrategy;
import org.apache.jena.mem.store.cow.CowWriteTxn;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.jena.testing_framework.GraphHelper.node;
import static org.apache.jena.testing_framework.GraphHelper.triple;
import static org.junit.Assert.*;

/**
 * Strategy-level tests for {@link CowWriteTxn} that exercise
 * each of the five {@link IndexingStrategy} variants end-to-end:
 * <ul>
 *   <li>{@code EAGER}: index always present.
 *   <li>{@code LAZY} / {@code LAZY_PARALLEL}: auto-build on first lookup,
 *       sequential or parallel.
 *   <li>{@code MANUAL}: pattern lookups throw until {@code initializeIndex}
 *       is called.
 *   <li>{@code MINIMAL}: linearly scans the triple set on every match.
 * </ul>
 * Also covers fork inheritance of strategy state, {@code clearIndex} revert
 * semantics, and the LAZY → EAGER first-lookup race on a published
 * snapshot held by multiple readers.
 */
public class CowStoreStrategiesTest {


    private static Set<Triple> seedTriples() {
        Set<Triple> seeds = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            seeds.add(triple("s" + i + " " + "p" + " " + "o" + i));
            seeds.add(triple("s" + i + " " + "p2" + " " + "o"));
        }
        return seeds;
    }

    private static CowWriteTxn seeded(IndexingStrategy s) {
        CowWriteTxn store = new CowWriteTxn(s);
        for (Triple x : seedTriples()) store.add(x);
        return store;
    }

    private static Set<Triple> drain(java.util.stream.Stream<Triple> stream) {
        return stream.collect(Collectors.toCollection(HashSet::new));
    }

    // ----- EAGER (regression coverage of the refactor) ----------------

    @Test
    public void eagerReturnsExpectedMatches() {
        CowWriteTxn store = seeded(IndexingStrategy.EAGER);
        assertTrue("eager must report initialized", store.isIndexInitialized());

        // SUB_ANY_ANY: all triples with subject "s3" — the seeds give two.
        Triple match = Triple.createMatch(node("s3"), null, null);
        Set<Triple> got = drain(store.stream(match));
        assertEquals(2, got.size());
        assertTrue(store.contains(match));
    }

    // ----- LAZY ------------------------------------------------------

    @Test
    public void lazyDelaysIndexBuildUntilFirstLookup() {
        CowWriteTxn store = seeded(IndexingStrategy.LAZY);
        assertEquals(IndexingStrategy.LAZY, store.getIndexingStrategy());
        assertFalse("lazy must report uninitialized before any lookup",
                store.isIndexInitialized());

        // The next lookup triggers the build.
        Triple match = Triple.createMatch(node("s3"), null, null);
        Set<Triple> got = drain(store.stream(match));
        assertEquals(2, got.size());
        assertTrue("lazy must be initialized after first lookup",
                store.isIndexInitialized());
    }

    @Test
    public void lazyParallelBuildsCorrectIndex() {
        CowWriteTxn store = seeded(IndexingStrategy.LAZY_PARALLEL);
        assertFalse(store.isIndexInitialized());

        Triple match = Triple.createMatch(null,
                node("p"),
                node("o3"));
        Set<Triple> got = drain(store.stream(match));
        assertEquals(1, got.size());
        assertTrue(store.isIndexInitialized());
    }

    @Test
    public void lazyAddsAfterInitialBuildAreReflected() {
        // After an auto-build, the strategy is EAGER and subsequent adds
        // must update the index (not just the triple set).
        CowWriteTxn store = seeded(IndexingStrategy.LAZY);
        store.contains(Triple.createMatch(node("s3"), null, null));
        assertTrue(store.isIndexInitialized());

        store.add(triple("s99 p o99"));
        assertTrue(store.contains(
                Triple.createMatch(node("s99"), null, null)));
    }

    @Test
    public void lazyConcurrentFirstLookupRaceProducesConsistentAnswers() throws Exception {
        // Many threads hitting a still-pending lazy strategy must each
        // get the correct answer; the last-writer-wins volatile publish
        // only decides which built strategy is published, not which
        // answers are returned.
        CowWriteTxn store = seeded(IndexingStrategy.LAZY_PARALLEL);
        final int numReaders = 8;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(numReaders);
        AtomicReference<Throwable> err = new AtomicReference<>();
        Set<Set<Triple>> answers = new HashSet<>();

        Thread[] readers = new Thread[numReaders];
        for (int i = 0; i < numReaders; i++) {
            readers[i] = new Thread(() -> {
                try {
                    start.await();
                    Triple match = Triple.createMatch(null,
                            node("p"), null);
                    Set<Triple> got = drain(store.stream(match));
                    synchronized (answers) { answers.add(got); }
                } catch (Throwable th) { err.set(th); }
                finally { done.countDown(); }
            });
            readers[i].start();
        }
        start.countDown();
        done.await();
        if (err.get() != null) throw new AssertionError(err.get());

        // All readers must agree on the answer.
        assertEquals("readers disagreed under LAZY race", 1, answers.size());
        // After the race, the index is initialized exactly once.
        assertTrue(store.isIndexInitialized());
    }

    // ----- MANUAL ----------------------------------------------------

    @Test
    public void manualThrowsOnPatternLookupUntilInitialized() {
        CowWriteTxn store = seeded(IndexingStrategy.MANUAL);
        assertFalse(store.isIndexInitialized());

        Triple partial = Triple.createMatch(
                node("s3"), null, null);
        assertThrows(UnsupportedOperationException.class, () -> store.contains(partial));
        assertThrows(UnsupportedOperationException.class, () -> store.stream(partial));
        assertThrows(UnsupportedOperationException.class, () -> store.find(partial));

        // Fully-concrete and fully-open patterns bypass the strategy and
        // must still work.
        Triple concrete = triple("s3 p o3");
        assertTrue(store.contains(concrete));
        assertEquals(seedTriples().size(), store.countTriples());

        store.initializeIndex();
        assertTrue(store.isIndexInitialized());
        assertTrue(store.contains(partial));
        assertEquals(2, drain(store.stream(partial)).size());
    }

    @Test
    public void manualInitializeIndexParallelWorks() {
        CowWriteTxn store = seeded(IndexingStrategy.MANUAL);
        store.initializeIndexParallel();
        assertTrue(store.isIndexInitialized());
        Triple partial = Triple.createMatch(
                node("s3"), null, null);
        assertEquals(2, drain(store.stream(partial)).size());
    }

    // ----- MINIMAL ---------------------------------------------------

    @Test
    public void minimalAnswersLookupsByLinearScan() {
        CowWriteTxn store = seeded(IndexingStrategy.MINIMAL);
        assertFalse("minimal stays uninitialized", store.isIndexInitialized());

        Triple partial = Triple.createMatch(
                node("s3"), null, null);
        assertEquals(2, drain(store.stream(partial)).size());
        // Even after lookups, no auto-upgrade happens.
        assertFalse(store.isIndexInitialized());
    }

    @Test
    public void minimalCanBeUpgradedExplicitly() {
        CowWriteTxn store = seeded(IndexingStrategy.MINIMAL);
        store.initializeIndex();
        assertTrue(store.isIndexInitialized());
    }

    // ----- resetIndexStrategy semantics ------------------------------

    @Test
    public void resetIndexStrategyRevertsLazyToPending() {
        CowWriteTxn store = seeded(IndexingStrategy.LAZY);
        Triple partial = Triple.createMatch(
                node("s3"), null, null);
        store.stream(partial).count();              // triggers auto-build
        assertTrue(store.isIndexInitialized());

        store.resetIndexStrategy();
        assertFalse("resetIndexStrategy must revert lazy to pending",
                store.isIndexInitialized());

        // Next lookup re-builds.
        store.stream(partial).count();
        assertTrue(store.isIndexInitialized());
    }

    @Test
    public void resetIndexStrategyRevertsManualToThrowing() {
        CowWriteTxn store = seeded(IndexingStrategy.MANUAL);
        store.initializeIndex();
        assertTrue(store.isIndexInitialized());

        store.resetIndexStrategy();
        Triple partial = Triple.createMatch(
                node("s3"), null, null);
        assertThrows(UnsupportedOperationException.class, () -> store.contains(partial));
    }

    @Test
    public void resetIndexStrategyOnMinimalIsNoOpVisibly() {
        CowWriteTxn store = seeded(IndexingStrategy.MINIMAL);
        Triple partial = Triple.createMatch(
                node("s3"), null, null);
        assertEquals(2, drain(store.stream(partial)).size());

        store.resetIndexStrategy();
        // Still works the same way.
        assertEquals(2, drain(store.stream(partial)).size());
    }

    // ----- Fork inherits strategy state ------------------------------

    @Test
    public void forkOfLazyPendingIsAlsoLazyPending() {
        CowWriteTxn src = seeded(IndexingStrategy.LAZY);
        assertFalse(src.isIndexInitialized());

        CowWriteTxn fork = src.forkForWrite();
        assertEquals(IndexingStrategy.LAZY, fork.getIndexingStrategy());
        assertFalse("fork of LAZY-pending must also be pending",
                fork.isIndexInitialized());

        // Fork triggers its own build.
        Triple partial = Triple.createMatch(
                node("s3"), null, null);
        assertEquals(2, drain(fork.stream(partial)).size());
        assertTrue(fork.isIndexInitialized());
        // Source independent — its strategy slot is still pending.
        assertFalse(src.isIndexInitialized());
    }

    @Test
    public void forkOfLazyAlreadyUpgradedIsEager() {
        CowWriteTxn src = seeded(IndexingStrategy.LAZY);
        // Trigger the upgrade on the source.
        Triple partial = Triple.createMatch(
                node("s3"), null, null);
        src.stream(partial).count();
        assertTrue(src.isIndexInitialized());

        CowWriteTxn fork = src.forkForWrite();
        assertTrue("fork of upgraded LAZY must inherit the eager state",
                fork.isIndexInitialized());
    }

    @Test
    public void forkOfManualStaysManual() {
        CowWriteTxn src = seeded(IndexingStrategy.MANUAL);
        CowWriteTxn fork = src.forkForWrite();
        assertEquals(IndexingStrategy.MANUAL, fork.getIndexingStrategy());

        Triple partial = Triple.createMatch(
                node("s3"), null, null);
        assertThrows(UnsupportedOperationException.class, () -> fork.contains(partial));
    }

    @Test
    public void forkOfMinimalStaysMinimal() {
        CowWriteTxn src = seeded(IndexingStrategy.MINIMAL);
        CowWriteTxn fork = src.forkForWrite();
        assertEquals(IndexingStrategy.MINIMAL, fork.getIndexingStrategy());
        assertFalse(fork.isIndexInitialized());

        // Mutations on the fork don't leak back to the source.
        fork.add(triple("only-in-fork p o"));
        assertEquals(seedTriples().size() + 1, fork.countTriples());
        assertEquals(seedTriples().size(), src.countTriples());

        Triple partial = Triple.createMatch(
                node("only-in-fork"), null, null);
        assertEquals(1, drain(fork.stream(partial)).size());
        assertEquals(0, drain(src.stream(partial)).size());
    }

    @Test
    public void forkOfEagerKeepsSnapshotIsolation() {
        // Sanity that the strategy refactor didn't break the prior COW
        // invariants. Mutations on the fork must not be visible to src.
        CowWriteTxn src = seeded(IndexingStrategy.EAGER);
        CowWriteTxn fork = src.forkForWrite();
        fork.remove(triple("s0 p o0"));
        fork.add(triple("only-in-fork p o"));

        assertTrue("source kept original triple", src.contains(triple("s0 p o0")));
        assertFalse("source did not gain fork's triple",
                src.contains(triple("only-in-fork p o")));
        assertFalse(fork.contains(triple("s0 p o0")));
        assertTrue(fork.contains(triple("only-in-fork p o")));
    }
}

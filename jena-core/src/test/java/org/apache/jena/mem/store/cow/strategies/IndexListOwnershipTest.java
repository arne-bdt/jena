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
import org.apache.jena.mem.store.cow.CowIndexedSetTripleStore;
import org.apache.jena.mem.store.cow.TxnNodesToIndices;
import org.apache.jena.mem.store.indexed.IndexList;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.Assert.*;

/**
 * Targeted tests for the {@link IndexList#getOwnerId() ownership-id}
 * scheme that drives clone-on-first-touch in
 * {@link CowEagerStoreStrategy}. Verifies the predicate that:
 * <ol>
 *   <li>each strategy receives a unique {@code ownerId};
 *   <li>lists created by a strategy are stamped with that strategy's id;
 *   <li>a fork's first mutation of a shared list clones it and stamps
 *       the clone with the fork's id, leaving the source's list untouched;
 *   <li>further mutations of the same list inside the same fork hit the
 *       fast path (no further cloning).
 * </ol>
 * The behavioural correctness end-to-end (snapshot isolation, fuzz
 * stability) is already covered by
 * {@code CowIndexedSetTripleStoreFuzzTest} and the strategy tests; this
 * suite specifically checks the bookkeeping mechanism so the scheme can
 * be benchmarked with confidence.
 */
public class IndexListOwnershipTest {

    private static Triple t(String s, String p, String o) {
        return Triple.create(NodeFactory.createURI("http://ex/" + s),
                             NodeFactory.createURI("http://ex/" + p),
                             NodeFactory.createURI("http://ex/" + o));
    }

    private static Node n(String s) {
        return NodeFactory.createURI("http://ex/" + s);
    }

    /** Reach into a {@link CowIndexedSetTripleStore} for its eager strategy. */
    private static CowEagerStoreStrategy eagerStrategyOf(CowIndexedSetTripleStore store)
            throws Exception {
        Field f = CowIndexedSetTripleStore.class.getDeclaredField("strategy");
        f.setAccessible(true);
        Object atomicRef = f.get(store);
        Method get = atomicRef.getClass().getMethod("get");
        return (CowEagerStoreStrategy) get.invoke(atomicRef);
    }

    /** Reach into the strategy's subjectIndex to verify list ownership. */
    private static TxnNodesToIndices subjectIndexOf(CowEagerStoreStrategy strategy)
            throws Exception {
        Field f = CowEagerStoreStrategy.class.getDeclaredField("subjectIndex");
        f.setAccessible(true);
        return (TxnNodesToIndices) f.get(strategy);
    }

    /** Reach into the strategy for its ownerId. */
    private static long ownerIdOf(CowEagerStoreStrategy strategy) throws Exception {
        Field f = CowEagerStoreStrategy.class.getDeclaredField("ownerId");
        f.setAccessible(true);
        return f.getLong(strategy);
    }

    // ----- IndexList field plumbing -----------------------------------

    @Test
    public void newIndexListHasZeroOwnerId() {
        IndexList list = new IndexList();
        assertEquals("default ownerId is 0 (untracked)", 0L, list.getOwnerId());
    }

    @Test
    public void cloneInheritsOwnerId() {
        IndexList src = new IndexList();
        src.setOwnerId(42L);
        IndexList clone = src.clone();
        assertEquals("clone() inherits ownerId", 42L, clone.getOwnerId());
    }

    @Test
    public void setterIsPlainField() {
        // Trivial sanity that the setter writes the field, the getter reads it.
        IndexList list = new IndexList();
        list.setOwnerId(123L);
        assertEquals(123L, list.getOwnerId());
        list.setOwnerId(0L);
        assertEquals(0L, list.getOwnerId());
    }

    // ----- Strategy-level ownership semantics -------------------------

    @Test
    public void distinctStrategiesHaveDistinctOwnerIds() throws Exception {
        CowIndexedSetTripleStore a = new CowIndexedSetTripleStore(IndexingStrategy.EAGER);
        CowIndexedSetTripleStore b = new CowIndexedSetTripleStore(IndexingStrategy.EAGER);
        assertNotEquals(ownerIdOf(eagerStrategyOf(a)),
                        ownerIdOf(eagerStrategyOf(b)));
    }

    @Test
    public void listsCreatedByStrategyAreStampedWithItsOwnerId() throws Exception {
        CowIndexedSetTripleStore store = new CowIndexedSetTripleStore(IndexingStrategy.EAGER);
        store.add(t("s1", "p", "o1"));
        store.add(t("s1", "p", "o2"));

        long id = ownerIdOf(eagerStrategyOf(store));
        IndexList l = subjectIndexOf(eagerStrategyOf(store)).get(n("s1"));
        assertNotNull(l);
        assertEquals(id, l.getOwnerId());
    }

    @Test
    public void forkClonesOnFirstMutationAndStampsWithForkId() throws Exception {
        CowIndexedSetTripleStore src = new CowIndexedSetTripleStore(IndexingStrategy.EAGER);
        src.add(t("s1", "p", "o1"));
        src.add(t("s1", "p", "o2"));

        long srcId = ownerIdOf(eagerStrategyOf(src));
        IndexList srcList = subjectIndexOf(eagerStrategyOf(src)).get(n("s1"));
        assertEquals(srcId, srcList.getOwnerId());

        CowIndexedSetTripleStore fork = src.forkForWrite();
        long forkId = ownerIdOf(eagerStrategyOf(fork));
        assertNotEquals("fork must have a fresh ownerId", srcId, forkId);

        // Before any mutation, the fork's spine still resolves the key
        // to the SAME (shared) IndexList — and crucially, that list's
        // ownerId is still the source's id.
        IndexList beforeMutation = subjectIndexOf(eagerStrategyOf(fork)).get(n("s1"));
        assertSame("pre-mutation, fork must see the source's exact list",
                srcList, beforeMutation);
        assertEquals(srcId, beforeMutation.getOwnerId());

        // Trigger a mutation — adding a triple with the same subject hits
        // ensureWritableList which detects the mismatch and clones.
        fork.add(t("s1", "p", "o3"));

        IndexList afterMutation = subjectIndexOf(eagerStrategyOf(fork)).get(n("s1"));
        assertNotSame("mutation must replace the spine slot with a clone",
                srcList, afterMutation);
        assertEquals("the clone is stamped with the fork's ownerId",
                forkId, afterMutation.getOwnerId());
        // Source's list still has the source's id.
        assertEquals("source's list ownerId is unchanged",
                srcId, srcList.getOwnerId());
    }

    @Test
    public void secondMutationOnSameForkHitsFastPath() throws Exception {
        // Once a list is cloned by a fork, subsequent mutations of the
        // same key inside the same fork must NOT clone again — that's
        // the optimisation the ownerId scheme buys us.
        CowIndexedSetTripleStore src = new CowIndexedSetTripleStore(IndexingStrategy.EAGER);
        src.add(t("s1", "p", "o1"));

        CowIndexedSetTripleStore fork = src.forkForWrite();
        fork.add(t("s1", "p", "o2"));               // first mutation: clone
        IndexList afterFirst = subjectIndexOf(eagerStrategyOf(fork)).get(n("s1"));

        fork.add(t("s1", "p", "o3"));               // second mutation: in-place
        IndexList afterSecond = subjectIndexOf(eagerStrategyOf(fork)).get(n("s1"));

        assertSame("second mutation must reuse the already-cloned list (no further clone)",
                afterFirst, afterSecond);
        assertEquals(3, afterSecond.size());
    }

    @Test
    public void clearIndexThenRebuildKeepsOwnership() throws Exception {
        // After clearIndex on EAGER, a fresh empty eager strategy is
        // installed (same kind, but empty). Adding a triple must stamp
        // the new list with the NEW strategy's ownerId.
        CowIndexedSetTripleStore store = new CowIndexedSetTripleStore(IndexingStrategy.EAGER);
        store.add(t("s1", "p", "o1"));
        long oldId = ownerIdOf(eagerStrategyOf(store));

        store.clearIndex();                          // installs a fresh eager
        long newId = ownerIdOf(eagerStrategyOf(store));
        assertNotEquals("a fresh strategy must have a fresh ownerId", oldId, newId);

        // The triples themselves are still in the canonical set; the
        // index is empty and re-fills as we add.
        store.add(t("s1", "p", "o1"));
        IndexList l = subjectIndexOf(eagerStrategyOf(store)).get(n("s1"));
        assertNotNull(l);
        assertEquals(newId, l.getOwnerId());
    }
}

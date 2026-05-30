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
import org.apache.jena.mem.IndexingStrategy;
import org.apache.jena.mem.store.cow.CowWriteTxn;
import org.apache.jena.mem.store.cow.TxnNodesToIndices;
import org.apache.jena.mem.store.indexed.IndexList;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.apache.jena.testing_framework.GraphHelper.node;
import static org.apache.jena.testing_framework.GraphHelper.triple;
import static org.junit.Assert.*;

/**
 * Targeted tests for the spine-level ownership scheme that drives
 * clone-on-first-touch in {@link CowEagerStoreStrategy}. The scheme
 * lives on {@link TxnNodesToIndices} (inherited from
 * {@code TxnFastHashMap}) as a per-slot, writer-private bitmap
 * {@code valueOwnedByThisWriter}: a set bit means the value at that
 * slot was placed by this writer (and is therefore safe to mutate in
 * place); a clear bit means the value is still shared with a snapshot
 * and must be cloned before mutation. The bit is set inside
 * {@code put}'s insertion path (so {@code ensureWritableList}'s
 * tombstone-and-append automatically marks the new slot writer-owned)
 * and is allocated <i>fresh</i> on fork (no propagation from source —
 * a fresh writer has placed nothing yet).
 *
 * <p>End-to-end correctness (snapshot isolation, fuzz stability) is
 * already covered by {@code CowIndexedSetTripleStoreFuzzTest} and the
 * strategy tests; this suite specifically pins down the bookkeeping.
 */
public class IndexListOwnershipTest {

    private static Node n(String s) {
        return node(s);
    }

    /** Reach into a {@link CowWriteTxn} for its eager strategy. */
    private static CowEagerStoreStrategy eagerStrategyOf(CowWriteTxn store)
            throws Exception {
        // The strategy field lives on CowWriteTxn (not its abstract base):
        // the slot is intentionally kept off CowStore so the snapshot and the
        // writer can use different concurrency models for it (volatile vs
        // plain). See CowStore's Javadoc.
        Field f = CowWriteTxn.class.getDeclaredField("strategy");
        f.setAccessible(true);
        return (CowEagerStoreStrategy) f.get(store);
    }

    /** Reach into the strategy's subjectIndex for direct slot inspection. */
    private static TxnNodesToIndices subjectIndexOf(CowEagerStoreStrategy strategy)
            throws Exception {
        Field f = CowEagerStoreStrategy.class.getDeclaredField("subjectIndex");
        f.setAccessible(true);
        return (TxnNodesToIndices) f.get(strategy);
    }

    // -------------------------------------------------------------------

    @Test
    public void freshlyAddedListIsWriterOwned() throws Exception {
        CowWriteTxn store = new CowWriteTxn(IndexingStrategy.EAGER);
        store.add(triple("s1 p o1"));
        store.add(triple("s1 p o2"));

        TxnNodesToIndices spine = subjectIndexOf(eagerStrategyOf(store));
        int eIndex = spine.indexOf(n("s1"));
        assertTrue("eIndex must be valid for present key", eIndex >= 0);
        assertTrue("freshly-inserted slot must be marked writer-owned",
                spine.isValueOwnedByThisWriter(eIndex));
    }

    @Test
    public void forkSeesParentSlotAsNotOwned() throws Exception {
        CowWriteTxn src = new CowWriteTxn(IndexingStrategy.EAGER);
        src.add(triple("s1 p o1"));

        CowWriteTxn fork = src.forkForWrite();

        TxnNodesToIndices forkSpine = subjectIndexOf(eagerStrategyOf(fork));
        int eIndex = forkSpine.indexOf(n("s1"));
        assertTrue("fork must still see the key", eIndex >= 0);
        // The bitmap is allocated fresh on fork, so the slot inherited
        // from the source is not writer-owned by the new fork.
        assertFalse("inherited slot must NOT be writer-owned by the fork",
                forkSpine.isValueOwnedByThisWriter(eIndex));
    }

    @Test
    public void firstMutationClonesAndMarksWriterOwned() throws Exception {
        CowWriteTxn src = new CowWriteTxn(IndexingStrategy.EAGER);
        src.add(triple("s1 p o1"));

        TxnNodesToIndices srcSpine = subjectIndexOf(eagerStrategyOf(src));
        IndexList srcList = srcSpine.get(n("s1"));

        CowWriteTxn fork = src.forkForWrite();
        // Trigger clone-on-first-touch by adding another triple with
        // the same subject.
        fork.add(triple("s1 p o2"));

        TxnNodesToIndices forkSpine = subjectIndexOf(eagerStrategyOf(fork));
        int eIndex = forkSpine.indexOf(n("s1"));
        IndexList forkList = forkSpine.getValueAt(eIndex);

        assertNotSame("first mutation must clone the shared list",
                srcList, forkList);
        assertTrue("the new slot must be marked writer-owned",
                forkSpine.isValueOwnedByThisWriter(eIndex));
        // Source's list is untouched.
        assertEquals("source's list size is unchanged", 1, srcList.size());
    }

    @Test
    public void secondMutationOnSameForkHitsFastPath() throws Exception {
        // Once a list has been cloned (and marked writer-owned in the
        // spine bitmap) by a fork, subsequent mutations of the same key
        // inside the same fork must NOT clone again — that's the
        // optimisation the spine-bit scheme buys us.
        CowWriteTxn src = new CowWriteTxn(IndexingStrategy.EAGER);
        src.add(triple("s1 p o1"));

        CowWriteTxn fork = src.forkForWrite();
        fork.add(triple("s1 p o2"));               // first mutation: clone
        TxnNodesToIndices forkSpine = subjectIndexOf(eagerStrategyOf(fork));
        IndexList afterFirst = forkSpine.get(n("s1"));

        fork.add(triple("s1 p o3"));               // second mutation: in-place
        IndexList afterSecond = forkSpine.get(n("s1"));

        assertSame("second mutation must reuse the already-cloned list",
                afterFirst, afterSecond);
        assertEquals(3, afterSecond.size());
    }

    @Test
    public void resetIndexStrategyClearsOwnership() throws Exception {
        // After resetIndexStrategy on EAGER, a fresh empty eager strategy
        // is installed and immediately re-indexes the existing triples
        // (EAGER's invariant is that the index reflects every triple). The
        // re-indexing goes through ensureWritableList on each freshly
        // constructed spine, so every slot is stamped writer-owned — the
        // old strategy's ownership bookkeeping is gone, and the new
        // strategy's slots are owned by *this* writer from the moment
        // they are populated.
        CowWriteTxn store = new CowWriteTxn(IndexingStrategy.EAGER);
        store.add(triple("s1 p o1"));

        store.resetIndexStrategy();                 // installs a fresh eager

        TxnNodesToIndices spine = subjectIndexOf(eagerStrategyOf(store));
        int eIndex = spine.indexOf(n("s1"));
        assertTrue("fresh strategy re-indexed existing triples", eIndex >= 0);
        assertTrue("re-indexed slot is writer-owned",
                spine.isValueOwnedByThisWriter(eIndex));
    }
}

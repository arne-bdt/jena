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

package org.apache.jena.mem.store.mvcc.strategies;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.store.mvcc.MvccIndexList;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Eager MVCC strategy: maintains a complete subject/predicate/object index over
 * all committed triples at all times. Each index is a reader-safe
 * {@link ConcurrentHashMap} from node to {@link MvccIndexList} of slot indices.
 * <p>
 * For a pattern, {@link #candidates(Triple)} returns the index list of the first
 * concrete dimension (subject, then predicate, then object); the store then
 * applies the version filter and full-pattern match to that list. A fully
 * unbound pattern scans the dense range.
 * <p>
 * No reverse-index arrays are needed (unlike the non-transactional eager
 * strategy) because MVCC never removes from an index list — deletes are version
 * stamps on the slot, handled by the store's visibility check.
 */
public final class MvccEagerStoreStrategy implements MvccStoreStrategy {

    private final ConcurrentHashMap<Node, MvccIndexList> sIndex = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Node, MvccIndexList> pIndex = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Node, MvccIndexList> oIndex = new ConcurrentHashMap<>();

    @Override
    public Candidates candidates(final Triple match) {
        final Node s = match.getSubject();
        if (MvccStoreStrategy.isConcrete(s)) {
            return Candidates.of(sIndex.get(s));
        }
        final Node p = match.getPredicate();
        if (MvccStoreStrategy.isConcrete(p)) {
            return Candidates.of(pIndex.get(p));
        }
        final Node o = match.getObject();
        if (MvccStoreStrategy.isConcrete(o)) {
            return Candidates.of(oIndex.get(o));
        }
        return Candidates.DENSE;
    }

    @Override
    public void onCommitAdd(final Triple t, final int slot) {
        sIndex.computeIfAbsent(t.getSubject(), k -> new MvccIndexList()).append(slot);
        pIndex.computeIfAbsent(t.getPredicate(), k -> new MvccIndexList()).append(slot);
        oIndex.computeIfAbsent(t.getObject(), k -> new MvccIndexList()).append(slot);
    }

    @Override
    public boolean isIndexInitialized() {
        return true;
    }

    @Override
    public void clear() {
        sIndex.clear();
        pIndex.clear();
        oIndex.clear();
    }
}

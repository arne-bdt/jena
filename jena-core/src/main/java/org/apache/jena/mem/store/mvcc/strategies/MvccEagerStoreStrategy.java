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
 * For a pattern, {@link #candidates(Triple)} returns the <em>shortest</em> index
 * list among the concrete dimensions (so a two/three-bound pattern scans the most
 * selective of subject/predicate/object rather than always the subject); the
 * store then applies the version filter and full-pattern match to that list. If
 * any concrete dimension is absent from its index the pattern is empty. A fully
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
        final Node p = match.getPredicate();
        final Node o = match.getObject();
        final boolean concreteS = MvccStoreStrategy.isConcrete(s);
        final boolean concreteP = MvccStoreStrategy.isConcrete(p);
        final boolean concreteO = MvccStoreStrategy.isConcrete(o);
        if (!concreteS && !concreteP && !concreteO) {
            return Candidates.DENSE;            // fully unbound: scan the dense range
        }
        // Pick the shortest index list among the concrete components, so a
        // two/three-bound pattern scans the most selective dimension (e.g. a rare
        // object) instead of always the subject. A concrete component absent from
        // its index means no slot carries it, so the whole pattern is empty: the
        // index lists are append-only and a vacuum re-indexes every reader-visible
        // (live or retained-dead) slot, so "absent" is authoritative for any
        // version a current reader can hold.
        MvccIndexList best = null;
        int bestSize = Integer.MAX_VALUE;
        Dim bestDim = Dim.NONE;
        if (concreteS) {
            final MvccIndexList l = sIndex.get(s);
            if (l == null) {
                return Candidates.EMPTY;
            }
            bestSize = l.size();
            best = l;
            bestDim = Dim.SUBJECT;
        }
        if (concreteP) {
            final MvccIndexList l = pIndex.get(p);
            if (l == null) {
                return Candidates.EMPTY;
            }
            final int n = l.size();
            if (n < bestSize) {
                bestSize = n;
                best = l;
                bestDim = Dim.PREDICATE;
            }
        }
        if (concreteO) {
            final MvccIndexList l = oIndex.get(o);
            if (l == null) {
                return Candidates.EMPTY;
            }
            if (l.size() < bestSize) {
                best = l;
                bestDim = Dim.OBJECT;
            }
        }
        return Candidates.of(best, bestDim);
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

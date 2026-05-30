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

/**
 * Controls how the MVCC store narrows a pattern lookup to a set of candidate
 * triple slots, and how the auxiliary subject/predicate/object index is
 * maintained as triples are committed.
 * <p>
 * In MVCC there is no swap-with-last removal: a delete only stamps a version on
 * the triple slot, so the index is strictly append-only and {@code onCommitAdd}
 * is the only maintenance hook. Dead/older slots linger in the index lists and
 * are skipped by the caller's version filter until a future vacuum compacts them.
 * <p>
 * Implementations expose their indices as reader-safe structures (e.g.
 * {@link java.util.concurrent.ConcurrentHashMap} of {@link MvccIndexList}); the
 * writer mutates them only while holding the store's writer lock.
 */
public interface MvccStoreStrategy {

    /**
     * The candidate slots a reader should scan for a pattern, before applying the
     * version filter and full-pattern match. {@code dense} means "scan the whole
     * dense slot range"; otherwise {@code list} is the index list to scan, which
     * may be {@code null} to mean "no candidates" (a bound node absent from the
     * index).
     *
     * @param list  the index list to scan, or {@code null} for none
     * @param dense whether to scan the whole dense slot range instead
     */
    record Candidates(MvccIndexList list, boolean dense) {
        /** Scan every slot in the dense range. */
        public static final Candidates DENSE = new Candidates(null, true);
        /** No candidate slots at all (empty result). */
        public static final Candidates EMPTY = new Candidates(null, false);

        /** @return {@link #DENSE} unused here; a list source, or {@link #EMPTY} if {@code list} is null. */
        public static Candidates of(MvccIndexList list) {
            return list == null ? EMPTY : new Candidates(list, false);
        }
    }

    /**
     * @param match the lookup pattern (subject/predicate/object, with
     *              non-concrete nodes acting as wildcards)
     * @return the candidate-slot source for the pattern
     */
    Candidates candidates(Triple match);

    /**
     * Maintain the index for a triple appended at the given slot during commit
     * application. Called by the single writer under the writer lock.
     *
     * @param t    the committed triple
     * @param slot its stable slot index in the dense arrays
     */
    void onCommitAdd(Triple t, int slot);

    /** @return whether the auxiliary index is built and serving lookups directly. */
    boolean isIndexInitialized();

    /** Discard all index state (used by {@code clear()}). */
    void clear();

    /**
     * @param node a pattern node
     * @return {@code true} iff {@code node} is a concrete node (so it can be used
     *         to narrow the lookup); {@code Node.ANY}, variables and {@code null}
     *         are treated as wildcards
     */
    static boolean isConcrete(final Node node) {
        return node != null && node.isConcrete();
    }
}

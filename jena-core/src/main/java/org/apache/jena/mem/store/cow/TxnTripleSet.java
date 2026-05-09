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

package org.apache.jena.mem.store.cow;

import org.apache.jena.atlas.lib.Copyable;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.store.cow.collection.TxnFastHashSet;
import org.apache.jena.mem.store.indexed.IndexedTripleSource;

/**
 * Copy-on-write twin of {@link org.apache.jena.mem.store.indexed.TripleSet}.
 * The canonical {@link Triple} collection inside the COW indexed store;
 * each triple has a stable {@code int} index that the strategy uses as
 * the key into its parallel reverse-index arrays.
 * <p>
 * Adds {@link #getInternalKeysLength} on top of {@link TxnFastHashSet} so
 * dependent writer-private arrays (e.g. the eager strategy's
 * {@code sReverseIndices} / {@code pReverseIndices} / {@code oReverseIndices})
 * can be sized to match the current capacity of the {@code keys} array.
 * The strategy resizes those arrays inline inside its {@code addToIndex}
 * (a single length compare on the hot path); since only the writer
 * grows {@code keys}, no callback or growth hook is needed.
 * <p>
 * Public surface intentionally matches {@code TripleSet} so the eager
 * strategy can be ported with minimal refactoring.
 */
public class TxnTripleSet
        extends TxnFastHashSet<Triple>
        implements Copyable<TxnTripleSet>, IndexedTripleSource {

    public TxnTripleSet() {
        super();
    }

    /**
     * Fork constructor — see
     * {@link org.apache.jena.mem.store.cow.collection.TxnFastHashBase#TxnFastHashBase(
     * org.apache.jena.mem.store.cow.collection.TxnFastHashBase)}.
     */
    private TxnTripleSet(final TxnTripleSet source) {
        super(source);
    }

    @Override
    protected Triple[] newKeysArray(int size) {
        return new Triple[size];
    }

    /**
     * Cheap fork (the canonical Phase B path). Discipline: after this
     * returns, treat the source as frozen — only the fork is mutated.
     */
    public TxnTripleSet fork() {
        return new TxnTripleSet(this);
    }

    /**
     * {@inheritDoc}
     * <p>
     * <b>This is a fork, not a deep copy.</b> The source must not be
     * mutated after this call — only the returned instance is safe to
     * mutate. See {@link #fork()} and the class Javadoc.
     * <p>
     * If a future caller needs a truly independent deep copy, this is
     * the place to add it.
     */
    @Override
    public TxnTripleSet copy() {
        return fork();
    }

    /**
     * @return the current capacity of the {@code keys} array (one past
     * the largest index that may yet be allocated before a grow)
     */
    public int getInternalKeysLength() {
        return keys.length;
    }
}

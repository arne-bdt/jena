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
import org.apache.jena.graph.Node;
import org.apache.jena.mem.store.cow.collection.TxnFastHashMap;
import org.apache.jena.mem.store.indexed.IndexList;

/**
 * Copy-on-write twin of {@link org.apache.jena.mem.store.indexed.NodesToIndices}.
 * Maps a {@link Node} to the {@link IndexList} of triple indices that
 * mention it in the corresponding component slot (subject, predicate,
 * or object), used by the eager strategy as one of its three indices.
 * <p>
 * The clone-on-first-touch policy on the {@link IndexList} values lives
 * in the eager strategy's
 * {@link org.apache.jena.mem.store.cow.strategies.CowEagerStoreStrategy}{@code .ensureWritableList},
 * which queries the inherited {@link #isValueOwnedByThisWriter(int)}
 * bitmap and clones via {@link #put(Node, IndexList)} (the COW
 * tombstone-and-append). This class itself stays policy-free.
 */
public class TxnNodesToIndices
        extends TxnFastHashMap<Node, IndexList>
        implements Copyable<TxnNodesToIndices> {

    public TxnNodesToIndices() {
        super();
    }

    /**
     * Fork constructor — see
     * {@link org.apache.jena.mem.store.cow.collection.TxnFastHashBase#TxnFastHashBase(
     * org.apache.jena.mem.store.cow.collection.TxnFastHashBase)}.
     * The shared {@code values[]} entries (the {@link IndexList}s) are
     * not cloned here; clone-on-first-touch happens lazily inside the
     * eager strategy when the writer first mutates a particular list.
     */
    private TxnNodesToIndices(final TxnNodesToIndices source) {
        super(source);
    }

    @Override
    protected Node[] newKeysArray(int size) {
        return new Node[size];
    }

    @Override
    protected IndexList[] newValuesArray(int size) {
        return new IndexList[size];
    }

    /** Cheap fork. See class doc for clone-on-first-touch responsibilities. */
    public TxnNodesToIndices fork() {
        return new TxnNodesToIndices(this);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns an <b>independent deep copy</b>, mirroring
     * {@link org.apache.jena.mem.store.indexed.NodesToIndices#copy()}: the
     * base arrays are cloned and each value is an independent
     * {@link IndexList#copy() clone} of the corresponding source list, so this
     * source and the returned map are both freely mutable down to their index
     * lists and neither sees the other's changes.
     * <p>
     * Contrast {@link #fork()}, the cheap copy-on-write path, which shares the
     * spine and {@code values} arrays (deferring per-list duplication to the
     * eager strategy's clone-on-first-touch) and therefore requires the source
     * to be treated as frozen afterwards.
     */
    @Override
    public TxnNodesToIndices copy() {
        final TxnNodesToIndices result = new TxnNodesToIndices();
        result.copyBaseStateFrom(this);
        result.copyValuesFrom(this, IndexList::copy);
        return result;
    }
}

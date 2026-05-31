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

package org.apache.jena.sparql.core.mem;

import static java.lang.ThreadLocal.withInitial;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.jena.atlas.lib.tuple.TupleMap;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ReadWrite;
import org.slf4j.Logger;

import io.lacuna.bifurcan.Map;

/**
 * A partial implementation of {@link TupleTable} backed by nested
 * <a href="https://github.com/lacuna/bifurcan">Bifurcan</a> {@link Map}s and {@link io.lacuna.bifurcan.Set}s.
 * Bifurcan's {@code Map}/{@code Set} are CHAMP structures (the layout described by Steindorfer and Vinju) that, unlike
 * a plain HAMT, keep an identical memory layout for equivalent collections and a compact node layout for fast
 * iteration.
 * <p>
 * Transaction model:
 * <ul>
 * <li>A {@code READ} transaction shares the current <em>forked</em> (immutable) snapshot directly; concurrent readers
 *     are safe because the snapshot is never mutated in place.</li>
 * <li>A {@code WRITE} transaction takes a {@code linear()} (transient) working copy of the <em>root</em> map. The root
 *     is then updated in place across the many {@code add}/{@code delete} calls of the transaction, while the nested
 *     index structure below the root is updated functionally (path-copying, structurally shared with earlier
 *     snapshots). This is the cheap, common way to use a transient collection: mutate one collection many times, then
 *     publish it once.</li>
 * <li>{@link #commit()} publishes the new immutable snapshot with a single {@code forked()} on the root (O(1)); the
 *     functional updates already left everything below the root forked and shared (MVCC).</li>
 * </ul>
 * Bifurcan's editor-token mechanism guarantees that {@code forked()}/{@code linear()} never mutate the receiver and
 * that mutating a {@code linear()} copy of a forked collection never affects the forked original, which is what makes
 * this safe under concurrent readers.
 *
 * @param <RootMapType> the type of the top-level (first slot) index map
 * @param <TupleType> the type of tuple in which a subclass of this class transacts
 * @param <ConsumerType> a type of consumer that can accept as many elements as exist in {@code TupleType}
 */
public abstract class BifurcanTupleTable<RootMapType extends Map<Node, ?>, TupleType, ConsumerType>
        extends OrderedTupleTable<TupleType, ConsumerType> implements TupleTable<TupleType> {

    /**
     * @return an empty, <em>forked</em> (immutable) value to which to initialize the table data.
     */
    protected abstract RootMapType initial();

    // The committed, immutable (forked) snapshot, swapped atomically on commit.
    private final AtomicReference<RootMapType> current = new AtomicReference<>(initial());

    protected AtomicReference<RootMapType> primary() {
        return current;
    }

    // The per-thread working view: the forked snapshot for READ, a linear working copy of the root for WRITE.
    private final ThreadLocal<RootMapType> local = withInitial(() -> null);

    private final String tableName;

    /**
     * @param n a name for this table
     * @param order the order of elements in this table
     */
    public BifurcanTupleTable(final String n, final TupleMap order) {
        super(order);
        this.tableName = n;
    }

    protected abstract Logger log();

    /**
     * Logs to DEBUG prepending the table name in order to distinguish amongst different indexes
     */
    protected void debug(final String msg, final Object... values) {
        if ( log().isDebugEnabled() )
            log().debug(tableName + ": " + msg, values);
    }

    /**
     * @return the working view of the table for the current transaction
     */
    protected RootMapType local() {
        return local.get();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void begin(final ReadWrite rw) {
        RootMapType root = primary().get();
        // READ shares the immutable snapshot; WRITE (or null, as used by the low-level table tests) takes a linear
        // working copy of the root. linear() is O(1) and shares structure with the snapshot via copy-on-write.
        if ( rw != ReadWrite.READ )
            root = (RootMapType) root.linear();
        local.set(root);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void commit() {
        debug("Publishing transactional snapshot as the shared reference.");
        primary().set((RootMapType) local().forked());
        end();
    }

    @Override
    public void end() {
        debug("Abandoning transactional reference.");
        local.remove();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void clear() {
        local.set((RootMapType) initial().linear());
    }

    protected boolean isConcrete(final Node n) {
        return n != null && n.isConcrete();
    }
}

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

import static java.util.stream.Stream.empty;
import static java.util.stream.Stream.of;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.stream.Stream;

import org.apache.jena.atlas.lib.tuple.TConsumer4;
import org.apache.jena.atlas.lib.tuple.TFunction4;
import org.apache.jena.atlas.lib.tuple.TupleMap;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.slf4j.Logger;

import io.lacuna.bifurcan.Map;
import io.lacuna.bifurcan.Set;

/**
 * An implementation of {@link QuadTable} based on nested Bifurcan {@link Map}s ({@code Node->Node->Node->Set<Node>}) in
 * one particular slot order (e.g. GSPO, SPOG or OPSG). Intended for high-speed in-memory use, exploiting Bifurcan's
 * linear (transient) collections for cheap writes. See {@link BifurcanTupleTable} for the transaction model.
 */
public class BifurcanQuadTable extends BifurcanTupleTable<Map<Node, Map<Node, Map<Node, Set<Node>>>>, Quad, TConsumer4<Node>>
        implements QuadTable {

    /**
     * @param order an internal order for this table
     */
    public BifurcanQuadTable(final String order) {
        this("GSPO", order);
    }

    /**
     * @param canonical the canonical order outside this table
     * @param order the internal order for this table
     */
    public BifurcanQuadTable(final String canonical, final String order) {
        this(canonical + "->" + order, TupleMap.create(canonical, order));
    }

    /**
     * @param tableName a name for this table
     * @param order the order of elements in this table
     */
    public BifurcanQuadTable(final String tableName, final TupleMap order) {
        super(tableName, order);
    }

    private static final Logger log = getLogger(BifurcanQuadTable.class);

    @Override
    protected Logger log() {
        return log;
    }

    @Override
    protected Map<Node, Map<Node, Map<Node, Set<Node>>>> initial() {
        return Map.empty();
    }

    @Override
    public void add(final Quad q) {
        map(add()).accept(q);
    }

    @Override
    public void delete(final Quad q) {
        map(delete()).accept(q);
    }

    @Override
    public Stream<Quad> find(final Node g, final Node s, final Node p, final Node o) {
        return map(find).apply(g, s, p, o);
    }

    /**
     * We descend through the nested {@link Map}s building up {@link Stream}s of partial tuples from which we develop a
     * {@link Stream} of full tuples which is our result. Use {@link Node#ANY} or <code>null</code> for a wildcard.
     */
    private final TFunction4<Node, Stream<Quad>> find = (first, second, third, fourth) -> {
        debug("Querying on four-tuple pattern: {} {} {} {} .", first, second, third, fourth);
        final Map<Node, Map<Node, Map<Node, Set<Node>>>> l1 = local();
        if ( isConcrete(first) ) {
            debug("Using a specific first slot value.");
            final Map<Node, Map<Node, Set<Node>>> l2 = l1.get(first, null);
            if ( l2 == null )
                return empty();
            if ( isConcrete(second) ) {
                debug("Using a specific second slot value.");
                final Map<Node, Set<Node>> l3 = l2.get(second, null);
                if ( l3 == null )
                    return empty();
                if ( isConcrete(third) ) {
                    debug("Using a specific third slot value.");
                    final Set<Node> l4 = l3.get(third, null);
                    if ( l4 == null )
                        return empty();
                    if ( isConcrete(fourth) ) {
                        debug("Using a specific fourth slot value.");
                        return l4.contains(fourth) ? of(unmap(first, second, third, fourth)) : empty();
                    }
                    debug("Using a wildcard fourth slot value.");
                    return l4.stream().map(x4 -> unmap(first, second, third, x4));
                }
                debug("Using wildcard third and fourth slot values.");
                return l3.stream().flatMap(e3 -> e3.value().stream().map(x4 -> unmap(first, second, e3.key(), x4)));
            }
            debug("Using wildcard second, third and fourth slot values.");
            return l2.stream().flatMap(e2 -> e2.value().stream()
                    .flatMap(e3 -> e3.value().stream().map(x4 -> unmap(first, e2.key(), e3.key(), x4))));
        }
        debug("Using a wildcard for all slot values.");
        return l1.stream().flatMap(e1 -> e1.value().stream()
                .flatMap(e2 -> e2.value().stream()
                        .flatMap(e3 -> e3.value().stream().map(x4 -> unmap(e1.key(), e2.key(), e3.key(), x4)))));
    };

    @Override
    protected TConsumer4<Node> add() {
        return (x1, x2, x3, x4) -> {
            debug("Adding four-tuple: {} {} {} {} .", x1, x2, x3, x4);
            // Functional update of the nested (forked) index below the root; single in-place put on the linear root.
            final Map<Node, Map<Node, Map<Node, Set<Node>>>> l1 = local();
            final Map<Node, Map<Node, Set<Node>>> l2 = l1.get(x1, null);
            final Map<Node, Set<Node>> l3 = l2 == null ? null : l2.get(x2, null);
            final Set<Node> leaf = l3 == null ? null : l3.get(x3, null);
            if ( leaf != null && leaf.contains(x4) )
                return;
            final Set<Node> leaf2 = ( leaf == null ? new Set<Node>() : leaf ).add(x4);
            final Map<Node, Set<Node>> l3b =
                (Map<Node, Set<Node>>) ( l3 == null ? new Map<Node, Set<Node>>() : l3 ).put(x3, leaf2);
            final Map<Node, Map<Node, Set<Node>>> l2b =
                (Map<Node, Map<Node, Set<Node>>>) ( l2 == null ? new Map<Node, Map<Node, Set<Node>>>() : l2 ).put(x2, l3b);
            l1.put(x1, l2b);
        };
    }

    @Override
    protected TConsumer4<Node> delete() {
        return (x1, x2, x3, x4) -> {
            debug("Removing four-tuple: {} {} {} {} .", x1, x2, x3, x4);
            final Map<Node, Map<Node, Map<Node, Set<Node>>>> l1 = local();
            final Map<Node, Map<Node, Set<Node>>> l2 = l1.get(x1, null);
            if ( l2 == null )
                return;
            final Map<Node, Set<Node>> l3 = l2.get(x2, null);
            if ( l3 == null )
                return;
            final Set<Node> leaf = l3.get(x3, null);
            if ( leaf == null || !leaf.contains(x4) )
                return;
            // Functional update, pruning now-empty containers; single in-place put/remove on the linear root.
            final Set<Node> leaf2 = leaf.remove(x4);
            final Map<Node, Set<Node>> l3b =
                leaf2.size() == 0L ? l3.remove(x3) : (Map<Node, Set<Node>>) l3.put(x3, leaf2);
            final Map<Node, Map<Node, Set<Node>>> l2b =
                l3b.size() == 0L ? l2.remove(x2) : (Map<Node, Map<Node, Set<Node>>>) l2.put(x2, l3b);
            if ( l2b.size() == 0L )
                l1.remove(x1);
            else
                l1.put(x1, l2b);
        };
    }
}

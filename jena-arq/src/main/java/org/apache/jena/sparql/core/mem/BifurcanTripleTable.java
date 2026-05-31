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

import org.apache.jena.atlas.lib.tuple.TConsumer3;
import org.apache.jena.atlas.lib.tuple.TFunction3;
import org.apache.jena.atlas.lib.tuple.TupleMap;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.slf4j.Logger;

import io.lacuna.bifurcan.Map;
import io.lacuna.bifurcan.Set;

/**
 * A {@link TripleTable} employing nested Bifurcan {@link Map}s ({@code Node->Node->Set<Node>}) to index triples in one
 * particular slot order (e.g. SPO, POS or OSP). See {@link BifurcanTupleTable} for the transaction model.
 */
public class BifurcanTripleTable extends BifurcanTupleTable<Map<Node, Map<Node, Set<Node>>>, Triple, TConsumer3<Node>>
        implements TripleTable {

    /**
     * @param order an internal order for this table
     */
    public BifurcanTripleTable(final String order) {
        this("SPO", order);
    }

    /**
     * @param canonical the canonical order outside this table
     * @param order the internal order for this table
     */
    public BifurcanTripleTable(final String canonical, final String order) {
        this(canonical + "->" + order, TupleMap.create(canonical, order));
    }

    /**
     * @param tableName a name for this table
     * @param order the order of elements in this table
     */
    public BifurcanTripleTable(final String tableName, final TupleMap order) {
        super(tableName, order);
    }

    private static final Logger log = getLogger(BifurcanTripleTable.class);

    @Override
    protected Logger log() {
        return log;
    }

    @Override
    protected Map<Node, Map<Node, Set<Node>>> initial() {
        return Map.empty();
    }

    @Override
    public void add(final Triple t) {
        map(add()).accept(t);
    }

    @Override
    public void delete(final Triple t) {
        map(delete()).accept(t);
    }

    @Override
    public Stream<Triple> find(final Node s, final Node p, final Node o) {
        return map(find).apply(s, p, o);
    }

    /**
     * We descend through the nested {@link Map}s building up {@link Stream}s of partial tuples from which we develop a
     * {@link Stream} of full tuples which is our result. Use {@link Node#ANY} or <code>null</code> for a wildcard.
     */
    private final TFunction3<Node, Stream<Triple>> find = (first, second, third) -> {
        debug("Querying on three-tuple pattern: {} {} {} .", first, second, third);
        final Map<Node, Map<Node, Set<Node>>> l1 = local();
        if ( isConcrete(first) ) {
            debug("Using a specific first slot value.");
            final Map<Node, Set<Node>> l2 = l1.get(first, null);
            if ( l2 == null )
                return empty();
            if ( isConcrete(second) ) {
                debug("Using a specific second slot value.");
                final Set<Node> l3 = l2.get(second, null);
                if ( l3 == null )
                    return empty();
                if ( isConcrete(third) ) {
                    debug("Using a specific third slot value.");
                    return l3.contains(third) ? of(unmap(first, second, third)) : empty();
                }
                debug("Using a wildcard third slot value.");
                return l3.stream().map(x3 -> unmap(first, second, x3));
            }
            debug("Using wildcard second and third slot values.");
            return l2.stream().flatMap(e2 -> e2.value().stream().map(x3 -> unmap(first, e2.key(), x3)));
        }
        debug("Using a wildcard for all slot values.");
        return l1.stream().flatMap(e1 -> e1.value().stream()
                .flatMap(e2 -> e2.value().stream().map(x3 -> unmap(e1.key(), e2.key(), x3))));
    };

    @Override
    protected TConsumer3<Node> add() {
        return (x1, x2, x3) -> {
            debug("Adding three-tuple {} {} {}", x1, x2, x3);
            // Functional update of the nested (forked) index below the root; single in-place put on the linear root.
            final Map<Node, Map<Node, Set<Node>>> l1 = local();
            final Map<Node, Set<Node>> l2 = l1.get(x1, null);
            final Set<Node> leaf = l2 == null ? null : l2.get(x2, null);
            if ( leaf != null && leaf.contains(x3) )
                return;
            final Set<Node> leaf2 = ( leaf == null ? new Set<Node>() : leaf ).add(x3);
            final Map<Node, Set<Node>> l2b =
                (Map<Node, Set<Node>>) ( l2 == null ? new Map<Node, Set<Node>>() : l2 ).put(x2, leaf2);
            l1.put(x1, l2b);
        };
    }

    @Override
    protected TConsumer3<Node> delete() {
        return (x1, x2, x3) -> {
            debug("Deleting three-tuple {} {} {}", x1, x2, x3);
            final Map<Node, Map<Node, Set<Node>>> l1 = local();
            final Map<Node, Set<Node>> l2 = l1.get(x1, null);
            if ( l2 == null )
                return;
            final Set<Node> leaf = l2.get(x2, null);
            if ( leaf == null || !leaf.contains(x3) )
                return;
            // Functional update, pruning now-empty containers; single in-place put/remove on the linear root.
            final Set<Node> leaf2 = leaf.remove(x3);
            final Map<Node, Set<Node>> l2b =
                leaf2.size() == 0L ? l2.remove(x2) : (Map<Node, Set<Node>>) l2.put(x2, leaf2);
            if ( l2b.size() == 0L )
                l1.remove(x1);
            else
                l1.put(x1, l2b);
        };
    }
}

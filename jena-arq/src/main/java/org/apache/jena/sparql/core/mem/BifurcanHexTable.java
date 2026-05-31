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

import static java.util.EnumSet.noneOf;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toMap;
import static org.apache.jena.graph.Node.ANY;
import static org.apache.jena.sparql.core.mem.QuadTableForm.GSPO;
import static org.apache.jena.sparql.core.mem.QuadTableForm.chooseFrom;
import static org.apache.jena.sparql.core.mem.QuadTableForm.tableForms;
import static org.apache.jena.sparql.core.mem.TupleSlot.GRAPH;
import static org.apache.jena.sparql.core.mem.TupleSlot.OBJECT;
import static org.apache.jena.sparql.core.mem.TupleSlot.PREDICATE;
import static org.apache.jena.sparql.core.mem.TupleSlot.SUBJECT;

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.core.Quad;

/**
 * A six-way {@link QuadTable} using all of the available forms in {@link QuadTableForm}, backed by
 * {@link BifurcanQuadTable} indexes. This is the Bifurcan-backed analogue of {@link HexTable}: it reuses
 * {@code QuadTableForm} purely for the slot orderings and the index-selection logic, and selects the most useful index
 * form(s) for any given operation.
 *
 * @see HexTable
 */
public class BifurcanHexTable implements QuadTable {

    private final Map<QuadTableForm, QuadTable> indexBlock = new EnumMap<QuadTableForm, QuadTable>(
        tableForms().collect(toMap(x -> x, BifurcanHexTable::indexFor)));

    /**
     * @return the {@link BifurcanQuadTable} for a given form, with the same form-specific optimisations that
     *         {@link QuadTableForm} applies to its {@link PMapQuadTable}s.
     */
    private static QuadTable indexFor(final QuadTableForm form) {
        switch (form) {
        case GSPO:
            // GSPO has the graph in its first slot, so listing graph names is just listing the top-level keys.
            return new BifurcanQuadTable(form.name()) {
                @Override
                public Stream<Node> listGraphNodes() {
                    return local().keys().stream();
                }
            };
        case SPOG:
        case OPSG:
            // These orders place the graph last, so quads for a given triple are adjacent in a find() scan; we can
            // de-duplicate the union-graph view without building up a set of already-seen triples.
            return new BifurcanQuadTable(form.name()) {
                @Override
                public Stream<Quad> findInUnionGraph(final Node s, final Node p, final Node o) {
                    final AtomicReference<Triple> mostRecentlySeen = new AtomicReference<>();
                    return find(ANY, s, p, o).map(Quad::asTriple)
                            .filter(t -> !t.equals(mostRecentlySeen.getAndSet(t)))
                            .map(t -> Quad.create(Quad.unionGraph, t));
                }
            };
        default:
            return new BifurcanQuadTable(form.name());
        }
    }

    /**
     * A block of six indexes to which we provide access as though they were one.
     */
    protected Map<QuadTableForm, QuadTable> indexBlock() {
        return indexBlock;
    }

    @Override
    public Stream<Quad> find(final Node g, final Node s, final Node p, final Node o) {
        final Set<TupleSlot> pattern = noneOf(TupleSlot.class);
        if (isConcrete(g)) pattern.add(GRAPH);
        if (isConcrete(s)) pattern.add(SUBJECT);
        if (isConcrete(p)) pattern.add(PREDICATE);
        if (isConcrete(o)) pattern.add(OBJECT);
        final QuadTableForm choice = chooseFrom(pattern);
        return indexBlock().get(choice).find(g, s, p, o);
    }

    private static boolean isConcrete(final Node n) {
        return nonNull(n) && n.isConcrete();
    }

    @Override
    public void add(final Quad q) {
        indexBlock().values().forEach(index -> index.add(q));
    }

    @Override
    public void delete(final Quad q) {
        indexBlock().values().forEach(index -> index.delete(q));
    }

    @Override
    public Stream<Node> listGraphNodes() {
        // GSPO is specially equipped with an efficient listGraphNodes().
        return indexBlock().get(GSPO).listGraphNodes();
    }

    @Override
    public Stream<Quad> findInUnionGraph(final Node s, final Node p, final Node o) {
        // we can use adjacency in SPOG to solve this problem without building up a set of already-seen triples.
        return indexBlock().get(QuadTableForm.SPOG).findInUnionGraph(s, p, o);
    }

    @Override
    public void begin(final ReadWrite rw) {
        indexBlock().values().forEach(table -> table.begin(rw));
    }

    @Override
    public void end() {
        indexBlock().values().forEach(QuadTable::end);
    }

    @Override
    public void commit() {
        indexBlock().values().forEach(QuadTable::commit);
    }

    @Override
    public void clear() {
        indexBlock().values().forEach(QuadTable::clear);
    }
}

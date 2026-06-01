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
import static org.apache.jena.sparql.core.mem.QuadTableForm.GSPO ;
import static org.apache.jena.sparql.core.mem.QuadTableForm.OPSG ;
import static org.apache.jena.sparql.core.mem.QuadTableForm.SPOG ;
import static org.apache.jena.sparql.core.mem.QuadTableForm.chooseFrom ;
import static org.apache.jena.sparql.core.mem.QuadTableForm.tableForms ;
import static org.apache.jena.sparql.core.mem.TupleSlot.GRAPH ;
import static org.apache.jena.sparql.core.mem.TupleSlot.OBJECT ;
import static org.apache.jena.sparql.core.mem.TupleSlot.PREDICATE ;
import static org.apache.jena.sparql.core.mem.TupleSlot.SUBJECT ;

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.jena.graph.Node;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.core.Quad;

/**
 * A six-way {@link QuadTable} using all of the available forms in {@link QuadTableForm}. This class binds together all
 * of the enumerated values in {@code enum QuadTableForm}, each of which implements {@link QuadTable}, into one
 * implementation of {@code QuadTable} that selects the most useful index form(s) for any given operation.
 *
 */
public class HexTable implements QuadTable {

    private final Map<QuadTableForm, QuadTable> indexBlock = new EnumMap<QuadTableForm, QuadTable>(
        tableForms().collect(toMap(x -> x, QuadTableForm::get)));

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
        // SPOG and OPSG have GRAPH as their innermost slot, so quads that project to the same triple
        // are produced adjacently and can be de-duplicated by adjacency, without building up a set of
        // already-seen triples. This is only valid when the concrete query slots form a prefix of the
        // chosen index's slot order; otherwise find(...) on that single index would not constrain the
        // non-prefix slots. For the remaining (rarer) patterns, fall back to set-based de-duplication.
        if (concreteIsPrefix(s, p, o))
            return indexBlock().get(SPOG).findInUnionGraph(s, p, o);
        if (concreteIsPrefix(o, p, s))
            return indexBlock().get(OPSG).findInUnionGraph(s, p, o);
        return QuadTable.super.findInUnionGraph(s, p, o);
    }

    /** Whether the concrete nodes form a prefix of this slot order (no concrete slot follows a wildcard). */
    private static boolean concreteIsPrefix(final Node... nodesInIndexOrder) {
        boolean wildcardSeen = false;
        for (final Node n : nodesInIndexOrder) {
            if (isConcrete(n)) {
                if (wildcardSeen) return false;
            } else
                wildcardSeen = true;
        }
        return true;
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

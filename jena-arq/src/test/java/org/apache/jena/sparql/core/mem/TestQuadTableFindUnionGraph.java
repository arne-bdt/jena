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

import static java.util.stream.Collectors.toSet;
import static org.apache.jena.graph.Node.ANY;
import static org.apache.jena.graph.NodeFactory.createURI;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;

/**
 * Tests for {@link QuadTable#findInUnionGraph} — in particular the adjacency-based,
 * de-duplicating implementations supplied by the {@link QuadTableForm#SPOG} and
 * {@link QuadTableForm#OPSG} index forms and selected by {@link HexTable}.
 * <p>
 * These were previously unreachable from the public API and threw a {@link NullPointerException}
 * on the first matching quad (the adjacency state started {@code null}).
 */
public class TestQuadTableFindUnionGraph {

    private static final Node s  = createURI("info:s");
    private static final Node s2 = createURI("info:s2");
    private static final Node p  = createURI("info:p");
    private static final Node o  = createURI("info:o");
    private static final Node o2 = createURI("info:o2");
    private static final Node g1 = createURI("info:g1");
    private static final Node g2 = createURI("info:g2");
    private static final Node g3 = createURI("info:g3");

    private static QuadTable loaded(Supplier<QuadTable> maker, Quad... quads) {
        QuadTable table = maker.get();
        table.begin(null);
        for ( Quad q : quads )
            table.add(q);
        table.commit();
        return table;
    }

    private static Set<Quad> unionQuads(QuadTable table, Node s, Node p, Node o) {
        table.begin(null);
        try {
            Set<Quad> quads = table.findInUnionGraph(s, p, o).collect(toSet());
            // Every result quad must be in the union graph.
            assertTrue(quads.stream().allMatch(q -> q.getGraph().equals(Quad.unionGraph)),
                       () -> "Not all quads are in the union graph: " + quads);
            return quads;
        } finally { table.end(); }
    }

    private static Set<Triple> unionTriples(QuadTable table, Node s, Node p, Node o) {
        return unionQuads(table, s, p, o).stream().map(Quad::asTriple).collect(toSet());
    }

    // The three QuadTable suppliers whose findInUnionGraph performs adjacency de-duplication.
    // HexTable routes findInUnionGraph to its SPOG index; SPOG and OPSG are tested directly.
    private void forEachTable(java.util.function.Consumer<Supplier<QuadTable>> check) {
        check.accept(HexTable::new);
        check.accept(QuadTableForm.SPOG::get);
        check.accept(QuadTableForm.OPSG::get);
    }

    /** A single quad: the exact case that used to throw NPE on the first element. */
    @Test
    public void unionGraph_singleQuad_noNPE() {
        forEachTable(maker -> {
            QuadTable table = loaded(maker, Quad.create(g1, s, p, o));
            assertEquals(Set.of(Triple.create(s, p, o)), unionTriples(table, ANY, ANY, ANY));
        });
    }

    /** Empty table: no elements, no NPE, empty result. */
    @Test
    public void unionGraph_empty() {
        forEachTable(maker -> assertEquals(Set.of(), unionQuads(loaded(maker), ANY, ANY, ANY)));
    }

    /** The same triple in several named graphs collapses to one union-graph triple. */
    @Test
    public void unionGraph_deduplicatesAcrossGraphs() {
        forEachTable(maker -> {
            QuadTable table = loaded(maker,
                    Quad.create(g1, s, p, o),
                    Quad.create(g2, s, p, o),
                    Quad.create(g3, s, p, o));
            Set<Quad> quads = unionQuads(table, ANY, ANY, ANY);
            assertEquals(Set.of(Quad.create(Quad.unionGraph, s, p, o)), quads);
        });
    }

    /** Distinct triples are all retained; duplicates of each are collapsed. */
    @Test
    public void unionGraph_distinctTriples() {
        forEachTable(maker -> {
            QuadTable table = loaded(maker,
                    Quad.create(g1, s, p, o),
                    Quad.create(g2, s, p, o),    // duplicate triple of the first
                    Quad.create(g1, s, p, o2),
                    Quad.create(g3, s2, p, o));
            Set<Triple> triples = unionTriples(table, ANY, ANY, ANY);
            assertEquals(Set.of(Triple.create(s, p, o),
                                Triple.create(s, p, o2),
                                Triple.create(s2, p, o)), triples);
        });
    }

    /**
     * Concrete and partial patterns restrict the union-graph result. Index selection (which slots
     * form a usable prefix for SPOG/OPSG, and the set-based fallback) is HexTable's responsibility,
     * so this exercises HexTable across patterns that hit all three branches.
     */
    @Test
    public void unionGraph_patterns_hexTable() {
        QuadTable table = loaded(HexTable::new,
                Quad.create(g1, s, p, o),
                Quad.create(g2, s, p, o),
                Quad.create(g1, s, p, o2),
                Quad.create(g3, s2, p, o));
        // Fully concrete pattern matching a triple held in two graphs -> single triple.
        assertEquals(Set.of(Triple.create(s, p, o)), unionTriples(table, s, p, o));
        // Subject prefix (SPOG): subject+predicate, any object.
        assertEquals(Set.of(Triple.create(s, p, o), Triple.create(s, p, o2)),
                     unionTriples(table, s, p, ANY));
        // Object prefix (OPSG): any subject, concrete object.
        assertEquals(Set.of(Triple.create(s, p, o), Triple.create(s2, p, o)),
                     unionTriples(table, ANY, ANY, o));
        // Predicate-only is not a prefix of SPOG or OPSG -> set-based fallback.
        assertEquals(Set.of(Triple.create(s, p, o), Triple.create(s, p, o2), Triple.create(s2, p, o)),
                     unionTriples(table, ANY, p, ANY));
        // Subject+object (not contiguous in either) -> set-based fallback.
        assertEquals(Set.of(Triple.create(s, p, o)), unionTriples(table, s, ANY, o));
    }

    /** Each adjacency form, queried only with patterns whose concrete slots are a valid prefix. */
    @Test
    public void unionGraph_prefixPatterns_perForm() {
        // SPOG: subject(-predicate(-object)) prefixes.
        QuadTable spog = loaded(QuadTableForm.SPOG::get,
                Quad.create(g1, s, p, o), Quad.create(g2, s, p, o), Quad.create(g1, s, p, o2), Quad.create(g3, s2, p, o));
        assertEquals(Set.of(Triple.create(s, p, o), Triple.create(s, p, o2)), unionTriples(spog, s, p, ANY));
        assertEquals(Set.of(Triple.create(s, p, o), Triple.create(s, p, o2)), unionTriples(spog, s, ANY, ANY));
        assertEquals(Set.of(Triple.create(s, p, o)), unionTriples(spog, s, p, o));

        // OPSG: object(-predicate(-subject)) prefixes.
        QuadTable opsg = loaded(QuadTableForm.OPSG::get,
                Quad.create(g1, s, p, o), Quad.create(g2, s, p, o), Quad.create(g1, s, p, o2), Quad.create(g3, s2, p, o));
        assertEquals(Set.of(Triple.create(s, p, o), Triple.create(s2, p, o)), unionTriples(opsg, ANY, ANY, o));
        assertEquals(Set.of(Triple.create(s, p, o), Triple.create(s2, p, o)), unionTriples(opsg, ANY, p, o));
    }

    /** findInUnionGraph only sees named graphs (HexTable keeps the default graph separately). */
    @Test
    public void unionGraph_count() {
        QuadTable table = loaded(HexTable::new,
                Quad.create(g1, s, p, o),
                Quad.create(g2, s, p, o),
                Quad.create(g2, s, p, o2),
                Quad.create(g3, s2, p, o));
        table.begin(null);
        try {
            long n = table.findInUnionGraph(ANY, ANY, ANY).count();
            assertEquals(3, n);
        } finally { table.end(); }
    }
}

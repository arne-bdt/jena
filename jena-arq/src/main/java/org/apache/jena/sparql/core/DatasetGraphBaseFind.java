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

package org.apache.jena.sparql.core;


import java.util.Iterator ;
import java.util.stream.Stream ;

import org.apache.jena.atlas.iterator.Iter ;
import org.apache.jena.graph.Node ;
import org.apache.jena.graph.Triple ;
import org.apache.jena.sparql.core.mem.DatasetGraphInMemory ;

/**
 * DatasetGraph framework.
 * This class contains a convenience implementation of find that maps to a split between
 * defaultGraph/named graphs.
 * @see DatasetGraphTriplesQuads
 * @see DatasetGraphCollection
 * @see DatasetGraphOne
 * @see DatasetGraphInMemory
 */
abstract public class DatasetGraphBaseFind extends DatasetGraphBase
{
    protected DatasetGraphBaseFind() {}

    /** Implementation of find based on splitting into triples (default graph) and quads (named graph) */
    @Override
    public Iterator<Quad> find(Node g, Node s, Node p, Node o) {
        if ( Quad.isDefaultGraph(g))
            return findInDftGraph(s, p, o) ;
        if ( ! isWildcard(g) )
            return findNG(g, s, p, o) ;
        return findAny(s, p, o) ;
    }

    @Override
    public Iterator<Quad> findNG(Node g, Node s, Node p , Node o) {
        Iterator<Quad> qIter ;
        if ( Quad.isUnionGraph(g))
            qIter = findQuadsInUnionGraph(s, p, o) ;
        else if ( isWildcard(g) )
            qIter = findInAnyNamedGraphs(s, p, o) ;
        else if ( Quad.isDefaultGraph(g) )
            qIter = findInDftGraph(s, p, o) ;
        else
            // Not wildcard, not union graph, not default graph.
            qIter = findInSpecificNamedGraph(g, s, p, o) ;
        if ( qIter == null )
            return Iter.nullIterator() ;
        return qIter ;
    }

    protected Iterator<Quad> findAny(Node s, Node p, Node o) {
        // Default graph
        Iterator<Quad> iter1 = findInDftGraph(s, p, o);
        if ( ! iter1.hasNext() )
            iter1 = null;
        Iterator<Quad> iter2 = findInAnyNamedGraphs(s, p, o);
        if ( ! iter2.hasNext() )
            iter2 = null;
        // Copes with null in either or both positions.
        return Iter.append(iter1, iter2);
    }

    /** Stream equivalent of {@link #find(Node, Node, Node, Node)}: splits into the default
     * graph and the named graphs, routing through the {@code streamInXxx} primitives so that
     * stream-native implementations stay on streams (no iterator round-trip). */
    @Override
    public Stream<Quad> stream(Node g, Node s, Node p, Node o) {
        if ( Quad.isDefaultGraph(g))
            return streamInDftGraph(s, p, o) ;
        if ( ! isWildcard(g) )
            return streamNG(g, s, p, o) ;
        return streamAny(s, p, o) ;
    }

    /** Stream equivalent of {@link #findNG(Node, Node, Node, Node)}. */
    public Stream<Quad> streamNG(Node g, Node s, Node p , Node o) {
        Stream<Quad> stream ;
        if ( Quad.isUnionGraph(g))
            stream = streamQuadsInUnionGraph(s, p, o) ;
        else if ( isWildcard(g) )
            stream = streamInAnyNamedGraphs(s, p, o) ;
        else if ( Quad.isDefaultGraph(g) )
            stream = streamInDftGraph(s, p, o) ;
        else
            // Not wildcard, not union graph, not default graph.
            stream = streamInSpecificNamedGraph(g, s, p, o) ;
        if ( stream == null )
            return Stream.empty() ;
        return stream ;
    }

    protected Stream<Quad> streamAny(Node s, Node p, Node o) {
        return Stream.concat(streamInDftGraph(s, p, o), streamInAnyNamedGraphs(s, p, o)) ;
    }

    /** Find matches in the default graph.
     *  Return as quads; the default graph is {@link Quad#defaultGraphIRI}
     *  To get Triples, use {@code DatasetGraph.getDefaultGraph().find(...)}.
     */
    protected abstract Iterator<Quad> findInDftGraph(Node s, Node p , Node o) ;

    /** Find matches in the notional union of all named graphs - return as triples.
     * No duplicates - the union graph is a <em>set</em> of triples.
     * See {@link #findInAnyNamedGraphs}, where there may be duplicates.
     * <p>
     * Implementations are encouraged to override {@link #streamUnionGraphTriples}.
     * For example, it may be possible to avoid "distinct".
     */
    public Iterator<Triple> findInUnionGraph(Node s, Node p , Node o) {
        return streamUnionGraphTriples(s,p,o).iterator() ;
    }

    /** Find matches in the notional union of all named graphs - return as quads.
     * No duplicates - the union graph is a <em>set</em> of triples.
     * See {@link #findInAnyNamedGraphs}, where there may be duplicates.
     * <p>
     * Implementations are encouraged to override {@link #streamQuadsInUnionGraph} or
     * {@link #streamUnionGraphTriples}. For example, it may be possible to avoid "distinct".
     */
    public Iterator<Quad> findQuadsInUnionGraph(Node s, Node p , Node o) {
        return streamQuadsInUnionGraph(s,p,o).iterator() ;
    }

    /** Stream matches in the notional union of all named graphs - return as triples.
     * No duplicates - the union graph is a <em>set</em> of triples.
     * See {@link #streamInAnyNamedGraphs}, where there may be duplicates.
     * <p>
     * Implementations are encouraged to override this method. For example, it
     * may be possible to avoid "distinct".
     */
    public Stream<Triple> streamUnionGraphTriples(Node s, Node p , Node o) {
        return streamInAnyNamedGraphs(s,p,o).map(Quad::asTriple).distinct() ;
    }

    /** Stream matches in the notional union of all named graphs - return as quads.
     * No duplicates - the union graph is a <em>set</em> of triples.
     * See {@link #streamInAnyNamedGraphs}, where there may be duplicates.
     * <p>
     * Implementations are encouraged to override this method or {@link #streamUnionGraphTriples}.
     * For example, it may be possible to avoid "distinct".
     */
    public Stream<Quad> streamQuadsInUnionGraph(Node s, Node p , Node o) {
        return streamUnionGraphTriples(s,p,o).map(t -> Quad.create(Quad.unionGraph, t)) ;
    }

    /** Find in a specific named graph - {@code g} is a ground term (IRI or bNode), not a wild card (or null). */
    protected abstract Iterator<Quad> findInSpecificNamedGraph(Node g, Node s, Node p , Node o) ;

    /** Find in any named graph - return quads.
     * If a triple matches in two different graph, return a quad for each.
     * See {@link #findInUnionGraph} for matching without duplicate triples.
     */
    protected abstract Iterator<Quad> findInAnyNamedGraphs(Node s, Node p , Node o) ;

    // Stream primitives mirroring the find primitives above.
    // The default implementations bridge from the iterator primitives, which is optimal for
    // iterator-native stores. Stream-native stores (e.g. DatasetGraphInMemory) and graph-backed
    // stores (which have Graph#stream) should override these to stay on streams.

    /** Stream equivalent of {@link #findInDftGraph}. */
    protected Stream<Quad> streamInDftGraph(Node s, Node p, Node o) {
        return Iter.asStream(findInDftGraph(s, p, o)) ;
    }

    /** Stream equivalent of {@link #findInSpecificNamedGraph}. */
    protected Stream<Quad> streamInSpecificNamedGraph(Node g, Node s, Node p, Node o) {
        return Iter.asStream(findInSpecificNamedGraph(g, s, p, o)) ;
    }

    /** Stream equivalent of {@link #findInAnyNamedGraphs}. */
    protected Stream<Quad> streamInAnyNamedGraphs(Node s, Node p, Node o) {
        return Iter.asStream(findInAnyNamedGraphs(s, p, o)) ;
    }
}

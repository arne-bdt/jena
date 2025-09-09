/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.cimxml.graph;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.compose.MultiUnion;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NullIterator;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Extends the Apache Jena MultiUnion but find does not elminiate duplicates
 * This is based on from https://github.com/apache/jena/blob/master/jena-core/src/main/java/org/apache/jena/graph/compose/MultiUnion.java
 */
public class DisjointMultiUnion extends MultiUnion {

    public DisjointMultiUnion() {
        super();
    }

    public DisjointMultiUnion(Graph... graphs) {
        super(graphs);
    }

    public DisjointMultiUnion(Iterator<Graph> graphs) {
        super(graphs);
    }

    /**
     * <p>
     * Answer an iterator over the triples in the union of the graphs in this composition. <b>Note</b>
     * that the requirement to remove duplicates from the union means that this will be an
     * expensive operation for large (and especially for persistent) graphs.
     * </p>
     *
     * @param t The matcher to match against
     *
     * @return An iterator of all triples matching t in the union of the graphs.
     */
    @Override
    public ExtendedIterator<Triple> graphBaseFind(Triple t) { // optimise the case where there's only one component graph.
        ExtendedIterator<Triple> iter = NullIterator.instance();
        for(var g: m_subGraphs) {
            iter = iter.andThen(g.find(t));
        }
        return iter;
    }

    @Override
    public ExtendedIterator<Triple> find() {
        ExtendedIterator<Triple> iter = NullIterator.instance();
        for(var g: m_subGraphs) {
            iter = iter.andThen(g.find());
        }
        return iter;
    }

    @Override
    public Stream<Triple> stream(Node s, Node p, Node o) {
        return m_subGraphs.stream()
                               .flatMap(g -> g.stream(s, p, o));
    }

    @Override
    public Stream<Triple> stream() {
        return m_subGraphs.stream()
                               .flatMap(Graph::stream);
    }

    @Override
    protected int graphBaseSize() {
        var size = 0;
        for(var g: m_subGraphs) {
            size += g.size();
        }
        return size;
    }
}

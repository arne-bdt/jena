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

package org.apache.jena.mem2;

import org.apache.jena.atlas.lib.Copyable;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.graph.impl.GraphWithPerform;
import org.apache.jena.mem2.store.TripleStore;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.stream.Stream;

/**
 * A graph that stores triples in memory. This class is not thread-safe.
 * All triples are stored in a {@link TripleStore}, which is provided by subclasses
 * to select the desired storage strategy (e.g. {@link GraphMemFast},
 * {@link GraphMemIndexedSet}).
 * <p>
 * Implementations always comply to term-equality semantics. {@code handlesLiteralTyping()}
 * returns {@code false} for every {@link GraphMem} subclass.
 */
public class GraphMem extends GraphBase implements GraphWithPerform, Copyable<GraphMem> {

    /**
     * The backing triple store. The concrete implementation is selected by
     * subclasses and determines the performance/memory characteristics of the graph.
     */
    final TripleStore tripleStore;

    /**
     * Constructs a graph backed by the given triple store.
     * Subclasses are expected to instantiate the appropriate {@link TripleStore}
     * implementation and pass it to this constructor.
     *
     * @param tripleStore the triple store that will hold the graph's triples
     */
    protected GraphMem(TripleStore tripleStore) {
        super();
        this.tripleStore = tripleStore;
    }

    /**
     * Remove all the statements from this graph.
     */
    @Override
    public void clear() {
        super.clear(); /* deletes all triples and sends notifications*/
        this.tripleStore.clear();
    }

    /**
     * Add a triple to the graph without firing change notifications.
     * The triple is delegated to the underlying {@link TripleStore}.
     * If the triple is already present, the store is left unchanged.
     *
     * @param t triple to add
     */
    @Override
    public void performAdd(final Triple t) {
        tripleStore.add(t);
    }

    /**
     * Remove a triple from the graph without firing change notifications.
     * The triple is delegated to the underlying {@link TripleStore}.
     * If the triple is not present, the store is left unchanged.
     *
     * @param t triple to delete
     */
    @Override
    public void performDelete(Triple t) {
        tripleStore.remove(t);
    }

    /**
     * Returns a {@link Stream} of all triples in the graph.
     * Note: {@link Stream#parallel()} is supported.
     *
     * @return a stream  of triples in this graph.
     */
    @Override
    public Stream<Triple> stream() {
        return this.tripleStore.stream();
    }

    /**
     * Returns a {@link Stream} of Triples matching a pattern.
     * Note: {@link Stream#parallel()} is supported.
     *
     * @param sm subject node match pattern
     * @param pm predicate node match pattern
     * @param om object node match pattern
     * @return a stream  of triples in this graph matching the pattern.
     */
    @Override
    public Stream<Triple> stream(final Node sm, final Node pm, final Node om) {
        return this.tripleStore.stream(Triple.createMatch(sm, pm, om));
    }

    /**
     * Returns an {@link ExtendedIterator} over all triples in this graph matching the
     * given triple pattern. The pattern may be either concrete or contain wildcards
     * (e.g. {@link org.apache.jena.graph.Node#ANY}).
     *
     * @param tripleMatch the triple pattern to match
     * @return an iterator over the matching triples
     */
    @Override
    public ExtendedIterator<Triple> graphBaseFind(Triple tripleMatch) {
        return this.tripleStore.find(tripleMatch);
    }

    /**
     * Answer {@code true} if this graph contains any triple matching {@code tripleMatch}.
     * The match is delegated to the underlying {@link TripleStore} which uses
     * the most efficient lookup available for the kind of pattern.
     *
     * @param tripleMatch triple match pattern (may contain wildcards)
     * @return {@code true} if at least one matching triple exists
     */
    @Override
    public boolean graphBaseContains(final Triple tripleMatch) {
        return this.tripleStore.contains(tripleMatch);
    }

    /**
     * Returns the number of triples currently stored in this graph.
     * The size is maintained by the underlying {@link TripleStore} and is therefore
     * an O(1) operation.
     *
     * @return the number of triples in the graph
     */
    @Override
    public int graphBaseSize() {
        return this.tripleStore.countTriples();
    }

    /**
     * Creates a copy of this graph.
     * Since the triples and nodes are immutable, the copy contains the same triples and nodes as this graph.
     * Modifications to the copy will not affect this graph.
     *
     * @return independent copy of the current graph
     */
    @Override
    public GraphMem copy() {
        return new GraphMem(this.tripleStore.copy());
    }
}

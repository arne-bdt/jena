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

package org.apache.jena.mem2.store;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.GraphMemUsingOneIndex;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * A store of triples used in the {@link GraphMemUsingOneIndex} implementation.
 *
 * @see GraphMemUsingOneIndex
 */
public interface TripleStore {

    /**
     * Add a triple to the map.
     *
     * @param triple to add
     */
    void add(final Triple triple);

    /**
     * Remove a triple from the map.
     *
     * @param triple to remove
     */
    void remove(final Triple triple);

    /**
     * Remove all triples from the map.
     */
    void clear();

    /**
     * Return the number of triples in the map.
     */
    int countTriples();

    /**
     * Return true if the map is empty.
     */
    boolean isEmpty();


    /**
     * Answer true if the graph contains any triple matching <code>t</code>.
     * The default implementation uses <code>find</code> and checks to see
     * if the iterator is non-empty.
     *
     * @param tripleMatch triple match pattern, which may be contained
     */
    boolean contains(final Triple tripleMatch);

    /**
     * Returns a {@link Stream} of all triples in the graph.
     * Note: {@link Stream#parallel()} is supported.
     *
     * @return a stream  of triples in this graph.
     */
    Stream<Triple> stream();


    /**
     * Returns a {@link Stream} of Triples matching a pattern.
     * Note: {@link Stream#parallel()} is supported.
     * @param sm subject node match pattern
     * @param pm predicate node match pattern
     * @param om object node match pattern
     * @return a stream  of triples in this graph matching the pattern.
     */
    @SuppressWarnings("java:S3776")
    Stream<Triple> stream(final Node sm, final Node pm, final Node om);
    /**
     * Returns an {@link ExtendedIterator} of all triples in the graph matching the given triple match.
     */
    Iterator<Triple> find(final Triple tripleMatch);
}
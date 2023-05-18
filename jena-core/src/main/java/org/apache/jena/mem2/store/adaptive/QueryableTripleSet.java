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

package org.apache.jena.mem2.store.adaptive;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.GraphMemWithAdaptiveTripleStore;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.stream.Stream;

/**
 *
 * @see GraphMemWithAdaptiveTripleStore
 */
public interface QueryableTripleSet {

    public Node getIndexingNode();

    public int countTriples();

    public int countIndexSize();

    public boolean isEmpty();

    public boolean isReadyForTransition();

    public QueryableTripleSet createTransition();

    public boolean addTriple(final TripleWithNodeHashes tripleWithHashes);

    public void addTripleUnchecked(final TripleWithNodeHashes tripleWithHashes);

    public boolean removeTriple(final TripleWithNodeHashes tripleWithHashes);

    public void removeTripleUnchecked(final TripleWithNodeHashes tripleWithHashes);

    /**
     * Answer true if the graph contains any triple matching <code>t</code>.
     * The default implementation uses <code>find</code> and checks to see
     * if the iterator is non-empty.
     *
     * @param tripleMatch triple match pattern, which may be contained
     */
    boolean containsMatch(final Triple tripleMatch);

    /**
     * Returns a {@link Stream} of Triples matching the given pattern.
     * Note: {@link Stream#parallel()} is supported.
     * @param tripleMatch triple match pattern
     * @return a stream  of triples in this graph matching the pattern.
     */
    Stream<Triple> streamTriples(final Triple tripleMatch);

    /**
     * Returns a {@link Stream} of all Triples.
     * Note: {@link Stream#parallel()} is supported.
     * @return a stream of all triples in this set.
     */
    Stream<Triple> streamTriples();

    /**
     * Returns an {@link ExtendedIterator} of all triples in the graph matching the given triple match.
     */
    ExtendedIterator<Triple> findTriples(final Triple tripleMatch);

    /**
     * Returns an {@link ExtendedIterator} of all triples in the graph.
     */
    ExtendedIterator<Triple> findAll();
}

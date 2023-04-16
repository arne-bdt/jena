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

package org.apache.jena.mem2.map;

import org.apache.jena.graph.Triple;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * A map of triples, indexed by the first and the second nodes in the triple.
 * The map is a tree of maps.
 * The first node is the key in the top level map.
 * The second node is the key in the second level map.
 * The second level map is a set of triples.
 */
public interface TriplesMapWithOneIndex {

    /**
     * Add a triple to the map.
     *
     * @param level0IndexingHashCode the hash code of the indexing value of the first indexing node in the triple
     * @param tripleHashCode the hash code of the triple
     * @param triple to add
     * @return true if the triple was added, false if it was already present.
     */
    boolean add(final int level0IndexingHashCode, final int tripleHashCode, final Triple triple);

    /**
     * Add a triple to the map without checking if it is already present.
     * This is used when the triple is known to be new.
     *
     * @param level0IndexingHashCode the hash code of the indexing value of the first indexing node in the triple
     * @param tripleHashCode the hash code of the triple
     * @param triple to add
     */
    void addWithoutChecking(final int level0IndexingHashCode,final int tripleHashCode, final Triple triple);

    /**
     * Remove a triple from the map.
     *
     * @param level0IndexingHashCode the hash code of the indexing value of the first indexing node in the triple
     * @param tripleHashCode the hash code of the triple
     * @param triple to remove
     * @return true if the triple was removed, false if it was not present.
     */
    boolean remove(final int level0IndexingHashCode, final int tripleHashCode, final Triple triple);

    /**
     * Remove a triple from the map without checking if it is present.
     * This is used when the triple is known to be present.
     *
     * @param level0IndexingHashCode the hash code of the indexing value of the first indexing node in the triple
     * @param tripleHashCode the hash code of the triple
     * @param triple to remove
     */
    void removeWithoutChecking(final int level0IndexingHashCode, final int tripleHashCode, final Triple triple);

    /**
     * Remove all triples from the map.
     */
    void clear();

    /**
     * Return the number of first nodes in the map.
     */
    int numberOfFirstIndices();

    /**
     * Return the number of triples in the map.
     */
    int countTriples();

    /**
     * Return true if the map is empty.
     */
    boolean isEmpty();


    /**
     * Return a stream of all the triples in the map.
     */
    Stream<Triple> stream();

    /**
     * Return a stream of all the triples in the map with the given level0IndexingHashCode, tripleHashCode and triple.
     */
    Stream<Triple> stream(final int level0IndexingHashCode, final int tripleHashCode, final Triple triple);


    /**
     * Return a stream of all the triples in the map with the given level0IndexingHashCode.
     */
    Stream<Triple> stream(final int level0IndexingHashCode);

    /**
     * Return an iterator over all the triples in the map.
     */
    Iterator<Triple> find();


    /**
     * Return an iterator over all the triples in the map with the given level0IndexingHashCode, tripleHashCode and triple.
     */
    Iterator<Triple> find(final int level0IndexingHashCode, final int tripleHashCode, final Triple triple);

    /**
     * Return an iterator over all the triples in the map with the given level0IndexingHashCode.
     */
    Iterator<Triple> find(final int level0IndexingHashCode);
}

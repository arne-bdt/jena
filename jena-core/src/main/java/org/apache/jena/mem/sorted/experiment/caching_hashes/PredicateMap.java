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
package org.apache.jena.mem.sorted.experiment.caching_hashes;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.sorted.experiment.GrowingCollection;

import java.util.*;
import java.util.stream.Stream;

class PredicateMap implements TripleCollection {

    protected final Map<Integer, Collection<TripleWithHashCodes>> map = new HashMap<>();


    protected Collection<TripleWithHashCodes> createTripleCollection() {
        return new GrowingCollection<>(2);
    }

    protected int getKey(final Triple t) {
        return t.getPredicate().getIndexingValue().hashCode();
    }

    protected int getKey(final TripleWithHashCodes thc) {
        return thc.hashCodeForPredicate;
    }

    public Collection<TripleWithHashCodes> get(final Node keyNode) {
        var list = map.get(keyNode.getIndexingValue().hashCode());
        return list == null ? null : list;
    }

    /**
     * Add with no checks
     *
     * @param thc
     */
    public void addDefinitetly(final TripleWithHashCodes thc) {
        map.compute(getKey(thc),
                (k, v) -> {
                    if(v == null) {
                        v = createTripleCollection();
                    }
//                    if(v.size() == SWITCH_TO_HASH_COLLECTION_THRESHOLD) {
//                        var newList = createTripleHashCollection();
//                        v.forEach(newList::add);
//                        v = newList;
//                    }
                    v.add(thc);
                    return v;
                });
    }

    /**
     * Remove existing triple, without any checks.
     *
     * @param thc
     */
    public void removeExisting(final TripleWithHashCodes thc) {
        var key = getKey(thc);
        var triples = map.get(key);
        if (triples.remove(thc)) {
            if (triples.isEmpty()) {
                map.remove(key);
            }
        }
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public void clear() {
        map.clear();
    }

    public int numberOfKeys() {
        return map.size();
    }

    public int size() {
        return map.values().stream().mapToInt(Collection::size).sum();
    }

    public Stream<Triple> stream() {
        return map.values().stream().flatMap(list -> list.stream().map(thc -> thc.triple));
    }

    public Iterator<Triple> iterator() {
        return new CollectionOfTriplesIterator(map.values().iterator());
    }

    private static class CollectionOfTriplesIterator implements Iterator<Triple> {

        private final Iterator<Collection<TripleWithHashCodes>> baseIterator;
        private Iterator<TripleWithHashCodes> subIterator;
        private boolean hasSubIterator = false;

        public CollectionOfTriplesIterator(Iterator<Collection<TripleWithHashCodes>> baseIterator) {

            this.baseIterator = baseIterator;
            if (baseIterator.hasNext()) {
                subIterator = baseIterator.next().iterator();
                hasSubIterator = true;
            }
        }

        /**
         * Returns {@code true} if the iteration has more elements.
         * (In other words, returns {@code true} if {@link #next} would
         * return an element rather than throwing an exception.)
         *
         * @return {@code true} if the iteration has more elements
         */
        @Override
        public boolean hasNext() {
            if(hasSubIterator) {
                if (subIterator.hasNext()) {
                    return true;
                }
                while (baseIterator.hasNext()) {
                    subIterator = baseIterator.next().iterator();
                    if (subIterator.hasNext()) {
                        return true;
                    }
                }
                hasSubIterator = false;
            }
            return false;
        }

        /**
         * Returns the next element in the iteration.
         *
         * @return the next element in the iteration
         * @throws NoSuchElementException if the iteration has no more elements
         */
        @Override
        public Triple next() {
            if (!hasSubIterator || !this.hasNext()) {
                throw new NoSuchElementException();
            }
            return subIterator.next().triple;
        }
    }
}

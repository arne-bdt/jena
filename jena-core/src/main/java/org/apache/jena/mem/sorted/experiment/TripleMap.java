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
package org.apache.jena.mem.sorted.experiment;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

abstract class TripleMap {

    protected final static int SWITCH_TO_HASH_COLLECTION_THRESHOLD = 39;

    protected final Map<Integer, Collection<Triple>> map = new HashMap<>();
    protected final Function<Triple, Node> keyNodeResolver;

    protected abstract Collection<Triple> createTripleCollection();
    protected abstract Collection<Triple> createTripleHashCollection();

    public static Supplier<TripleMap> forObjects = () -> new TripleMap(Triple::getObject) {
        @Override
        protected Collection<Triple> createTripleCollection() {
            return new GrowingCollection<>(2);
        }

        @Override
        protected Collection<Triple> createTripleHashCollection() {
            return CollectionUsingHashCodesBase.forObjects.get();
        }
    };

    public static Supplier<TripleMap> forPredicates = () -> new TripleMap(Triple::getPredicate) {
        @Override
        protected Collection<Triple> createTripleCollection() {
            return new GrowingCollection<>(2);
        }

        @Override
        protected Collection<Triple> createTripleHashCollection() {
            return CollectionUsingHashCodesBase.forPredicates.get();
        }
    };


    public TripleMap(final Function<Triple, Node> keyNodeResolver) {
        this.keyNodeResolver = keyNodeResolver;
    }

    protected int getKey(final Triple t) {
        return keyNodeResolver.apply(t).getIndexingValue().hashCode();
    }

    public Collection<Triple> get(final Node keyNode) {
        var list = map.get(keyNode.getIndexingValue().hashCode());
        return list == null ? null : list;
    }

    /**
     * Adds only if not already exists.
     *
     * @param t
     * @return
     */
    public boolean addIfNotExists(final Triple t) {
        var key = getKey(t);
        var list = map.get(key);
        if(list == null) {
            list = createTripleCollection();
            list.add(t);
            map.put(key, list);
            return true;
        }
        if(list.contains(t)) {
            return false;
        }
        if(list.size() == SWITCH_TO_HASH_COLLECTION_THRESHOLD) {
            var newList = createTripleHashCollection();
            list.forEach(newList::add);
            list = newList;
        }
        list.add(t);

        return true;
    }

    /**
     * Add with no checks
     *
     * @param t
     */
    public void addDefinitetly(final Triple t) {
        map.compute(getKey(t),
                (k, v) -> {
                    if(v == null) {
                        v = createTripleCollection();
                    }
                    if(v.size() == SWITCH_TO_HASH_COLLECTION_THRESHOLD) {
                        var newList = createTripleHashCollection();
                        v.forEach(newList::add);
                        v = newList;
                    }
                    v.add(t);
                    return v;
                });
    }

    /**
     * Removes only if exists.
     *
     * @param t
     * @return
     */
    public boolean removeIfExits(final Triple t) {
        var key = getKey(t);
        var triples = map.get(key);
        if (triples != null) {
            if (triples.remove(t)) {
                if (triples.isEmpty()) {
                    map.remove(key);
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Remove existing triple, without any checks.
     *
     * @param t
     */
    public void removeExisting(final Triple t) {
        var key = getKey(t);
        var triples = map.get(key);
        if (triples.remove(t)) {
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
        return map.values().stream().flatMap(Collection::stream);
    }

    public Iterator<Triple> iterator() {
        return new CollectionOfTriplesIterator(map.values().iterator());
    }

    private static class CollectionOfTriplesIterator implements Iterator<Triple> {

        private final Iterator<Collection<Triple>> baseIterator;
        private Iterator<Triple> subIterator;
        private boolean hasSubIterator = false;

        public CollectionOfTriplesIterator(Iterator<Collection<Triple>> baseIterator) {

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
            if (hasSubIterator) {
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
            return subIterator.next();
        }
    }
}

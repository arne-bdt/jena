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
import java.util.function.Predicate;
import java.util.stream.Stream;

class PredicateMap implements TripleCollection {

    protected final static int SWITCH_TO_SORTED_THRESHOLD = Integer.MAX_VALUE;

    protected final Map<Integer, Collection<Triple>> map = new HashMap<>();

    protected int getKey(final Triple t) {
        return t.getPredicate().getIndexingValue().hashCode();
    }

    public Collection<Triple> get(final Node keyNode) {
        var list = map.get(keyNode.getIndexingValue().hashCode());
        return list == null ? null : list;
    }

    protected static Comparator<Triple> comparator =
            Comparator.comparingInt((Triple t) -> t.getSubject().getIndexingValue().hashCode());


    protected Collection<Triple> createTripleCollection() {
        return new ArrayList<>(2);
    }

    protected Predicate<Triple> getMatcherForObject(Triple triple) {
        if(ObjectEqualizer.isEqualsForObjectOk(triple.getObject())) {
            return t -> triple.equals(t);
        }
        return t -> triple.matches(t);
    }

    public PredicateMap() {

    }

    /**
     * Add with no checks
     * @param t
     */
    public void addDefinitetly(final Triple t) {
        var list = (List) map
                .computeIfAbsent(getKey(t),
                        k -> (List<Triple>) createTripleCollection());
        if(list.size() < SWITCH_TO_SORTED_THRESHOLD) {
            list.add(t);
            if(list.size() == SWITCH_TO_SORTED_THRESHOLD) {
                list.sort(comparator);
            }
        } else {
            var index = Collections.binarySearch(list, t, comparator);
            // < 0 if element is not in the list, see Collections.binarySearch
            if (index < 0) {
                index = -(index + 1);
                list.add(index, t);
            } else {
                // Insertion index is index of existing element, to add new element
                // behind it increase index
                index++;
                list.add(index, t);
            }
        }
    }

    /**
     * Remove existing triple, without any checks.
     *
     * @param t
     */
    public void removeExisting(final Triple t) {
        var key = getKey(t);
        var list = (List<Triple>)map.get(key);
        if(list.size() < SWITCH_TO_SORTED_THRESHOLD) {
            if (list.remove(t)) {
                if (list.isEmpty()) {
                    map.remove(key);
                }
            }
        } else {
            var index = Collections.binarySearch(list, t, comparator);
            /*search forward*/
            for (var i = index; i < list.size(); i++) {
                var t1 = list.get(i);
                if (t.equals(t1)) {
                    list.remove(i);
                    if (list.isEmpty()) {
                        map.remove(key);
                    }
                    return;
                }
                if (0 != comparator.compare(t1, t)) {
                    break;
                }
            }
            if (index > 0) {
                /*search backward*/
                index--;
                for (var i = index; i >= 0; i--) {
                    var t1 = list.get(i);
                    if (t.equals(t1)) {
                        list.remove(i);
                        if (list.isEmpty()) {
                            map.remove(key);
                        }
                        return;
                    }
                    if (0 != comparator.compare(t1, t)) {
                        break;
                    }
                }
            }
        }
    }

    /**
     * Adds only if not already exists.
     *
     * @param t
     * @return
     */
     public TripleWithHashCodes addIfNotExists(final Triple t) {
        var key = getKey(t);
        var list = (List<Triple>)map.get(key);
        if(list != null) {
            if(list.size() < SWITCH_TO_SORTED_THRESHOLD) {
                if (list.contains(t)) {
                    return null; /*triple already exists*/
                }
                list.add(t);
                if(list.size() == SWITCH_TO_SORTED_THRESHOLD) {
                    list.sort(comparator);
                }
            } else {
                var index = Collections.binarySearch(list, t, comparator);
                // < 0 if element is not in the list, see Collections.binarySearch
                if (index < 0) {
                    index = -(index + 1);
                    list.add(index, t);
                }
                else {
                    /*search forward*/
                    for (var i = index; i < list.size(); i++) {
                        var t1 = list.get(i);
                        if (t.equals(t1)) {
                            return null;
                        }
                        if (0 != comparator.compare(t1, t)) {
                            break;
                        }
                    }
                    if(index > 0) {
                        /*search backward*/
                        index--;
                        for (var i = index; i >= 0; i--) {
                            var t1 = list.get(i);
                            if (t.equals(t1)) {
                                return null;
                            }
                            if (0 != comparator.compare(t1, t)) {
                                break;
                            }
                        }
                        index++;
                    }
                    // Insertion index is index of existing element, to add new element
                    // behind it increase index
                    index++;
                    list.add(index, t);
                }
            }
        } else {
            list = (List<Triple>) createTripleCollection();
            list.add(t);
            map.put(key, list);
        }
        return TripleWithHashCodes.bySubject(t, key);
    }

    /**
     * Removes only if exists.
     *
     * @param t
     * @return
     */
    public TripleWithHashCodes removeIfExits(final Triple t) {
        var key = getKey(t);
        var list = (List<Triple>)map.get(key);
        if (list != null) {
            if (list.size() < SWITCH_TO_SORTED_THRESHOLD) {
                if (list.remove(t)) {
                    if (list.isEmpty()) {
                        map.remove(key);
                    }
                    return TripleWithHashCodes.bySubject(t, key);
                }
            } else {
                var index = Collections.binarySearch(list, t, comparator);
                // < 0 if element is not in the list, see Collections.binarySearch
                if (index < 0) {
                    return null;
                } else {
                    /*search forward*/
                    for (var i = index; i < list.size(); i++) {
                        var t1 = list.get(i);
                        if (t.equals(t1)) {
                            list.remove(i);
                            if (list.isEmpty()) {
                                map.remove(key);
                            }
                            return TripleWithHashCodes.bySubject(t, key);
                        }
                        if (0 != comparator.compare(t1, t)) {
                            break;
                        }
                    }
                    if (index > 0) {
                        /*search backward*/
                        index--;
                        for (var i = index; i >= 0; i--) {
                            var t1 = list.get(i);
                            if (t.equals(t1)) {
                                list.remove(i);
                                if (list.isEmpty()) {
                                    map.remove(key);
                                }
                                return TripleWithHashCodes.bySubject(t, key);
                            }
                            if (0 != comparator.compare(t1, t)) {
                                break;
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     *
     * @param t triple with concrete key and concrete sort node
     * @return
     */
    public boolean contains(final Triple t) {
        var key = getKey(t);
        var list = (List<Triple>) map.get(key);
        if(list == null) {
            return false;
        }
        var matcher = getMatcherForObject(t);
        if(list.size() < SWITCH_TO_SORTED_THRESHOLD) {
            for (Triple triple : list) {
                if(matcher.test(triple)) {
                    return true;
                }
            }
        } else  {
            var index = Collections.binarySearch(list, t, comparator);
            if (index < 0) {
                return false;
            }
            /*search forward*/
            for (var i = index; i < list.size(); i++) {
                var t1 = list.get(i);
                if (matcher.test(t1)) {
                    return true;
                }
                if (0 != comparator.compare(t1, t)) {
                    break;
                }
            }
            if (index > 0) {
                /*search backward*/
                index--;
                for (var i = index; i > -1; i--) {
                    var t1 = list.get(i);
                    if (matcher.test(t1)) {
                        return true;
                    }
                    if (0 != comparator.compare(t1, t)) {
                        break;
                    }
                }
            }
        }
        return false;
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

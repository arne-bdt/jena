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

package org.apache.jena.mem.simple;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class SubjectPredicateObjectMap implements TripleMapWithTwoKeys {

    /**
     * Predicate to match two triples with conditions ordered to fail fast for the given
     * usage, where subject hashCode is already used as key.
     */
    private final static BiPredicate<Triple, Triple> matchesSOP =
            (t1, t2) -> t1.getPredicate().getIndexingValue().hashCode() == t2.getPredicate().getIndexingValue().hashCode()
                    && t1.getPredicate().equals(t2.getPredicate())
                    && t1.getObject().sameValueAs(t2.getObject())
                    && t1.getSubject().equals(t2.getSubject());

    private final static BiPredicate<Triple, Triple> matchesPSO =
            (t1, t2) -> t1.getObject().getIndexingValue().hashCode() == t2.getObject().getIndexingValue().hashCode()
                    && t1.getObject().sameValueAs(t2.getObject())
                    && t1.getSubject().equals(t2.getSubject())
                    && t1.getPredicate().equals(t2.getPredicate());

    private final static BiPredicate<Triple, Triple> matchesOPS =
            (t1, t2) -> t1.getSubject().getIndexingValue().hashCode() == t2.getSubject().getIndexingValue().hashCode()
                    && t1.getSubject().equals(t2.getSubject())
                    && t1.getObject().sameValueAs(t2.getObject())
                    && t1.getPredicate().equals(t2.getPredicate());



    private final static Supplier<TripleMapWithOneKey> smallObjectMapSupplier = () -> new ArrayTripleMap(Triple::getObject, matchesSOP);
    private final static Supplier<TripleMapWithOneKey> smallSubjectMapSupplier = () -> new ArrayTripleMap(Triple::getSubject, matchesPSO);
    private final static Supplier<TripleMapWithOneKey> smallPredicateMapSupplier = () -> new ArrayTripleMap(Triple::getPredicate, matchesOPS);

    private final static Function<TripleMapWithOneKey,TripleMapWithOneKey> largeObjectMapSupplier = (smallTripleMap) -> new HashTripleMap(smallTripleMap, Triple::getObject, matchesSOP);
    private final static Function<TripleMapWithOneKey,TripleMapWithOneKey> largeSubjectMapSupplier = (smallTripleMap) -> new HashTripleMap(smallTripleMap, Triple::getSubject, matchesPSO);
    private final static Function<TripleMapWithOneKey,TripleMapWithOneKey> largePredicateMapSupplier = (smallTripleMap) -> new HashTripleMap(smallTripleMap, Triple::getPredicate, matchesOPS);

    public static SubjectPredicateObjectMap forSubjects() {
        return new SubjectPredicateObjectMap(Triple::getSubject, smallObjectMapSupplier, largeObjectMapSupplier);
    }

    public static SubjectPredicateObjectMap forPredicates() {
        return new SubjectPredicateObjectMap(Triple::getPredicate, smallSubjectMapSupplier, largeSubjectMapSupplier);
    }

    public static SubjectPredicateObjectMap forObjects() {
        return new SubjectPredicateObjectMap(Triple::getObject, smallPredicateMapSupplier, largePredicateMapSupplier);
    }


    protected final static int SWITCH_TO_LARGE_NESTED_MAP_THRESHOLD = 9;

    private final Function<Triple, Node> keyNodeResolver;
    private final Supplier<TripleMapWithOneKey> smallNestedMapSupplier;
    private final Function<TripleMapWithOneKey,TripleMapWithOneKey> largeNestedMapSupplier;
    private final HashMap<Integer, TripleMapWithOneKey> map = new HashMap<>();

    public SubjectPredicateObjectMap(final Function<Triple, Node> keyNodeResolver,
                                     final Supplier<TripleMapWithOneKey> smallNestedMapSupplier,
                                     final Function<TripleMapWithOneKey,TripleMapWithOneKey> largeNestedMapSupplier) {
        this.keyNodeResolver = keyNodeResolver;
        this.smallNestedMapSupplier = smallNestedMapSupplier;
        this.largeNestedMapSupplier = largeNestedMapSupplier;
    }

    private int getKey(final Triple t) {
        return keyNodeResolver.apply(t).getIndexingValue().hashCode();
    }

    @Override
    public boolean addIfNotExists(Triple t) {
        var key = getKey(t);
        final boolean[] added = {false};
        map.compute(key, (k, v) -> {
            if (v == null) {
                v = smallNestedMapSupplier.get();
                v.addDefinitetly(t);
                added[0] = true;
            } else {
                if(v.size() == SWITCH_TO_LARGE_NESTED_MAP_THRESHOLD) {
                    v = largeNestedMapSupplier.apply(v);
                }
                if(v.addIfNotExists(t)) {
                    added[0] = true;
                }
            }
            return v;
        });
        return added[0];
    }

    @Override
    public void addDefinitetly(Triple t) {
        map.compute(getKey(t), (k, v) -> {
            if(v == null) {
                v = smallNestedMapSupplier.get();
            } else {
                if (v.size() == SWITCH_TO_LARGE_NESTED_MAP_THRESHOLD) {
                    v = largeNestedMapSupplier.apply(v);
                }
            }
            v.addDefinitetly(t);
            return v;
        });
    }

    @Override
    public boolean removeIfExits(Triple t) {
        var key = getKey(t);
        final boolean[] removed = {false};
        map.computeIfPresent(key, (k, v) -> {
            if(v.removeIfExits(t)) {
                removed[0] = true;
                return v.isEmpty() ? null : v;
            }
            return null;
        });
        return removed[0];
    }

    @Override
    public void removeExisting(Triple t) {
        var key = getKey(t);
        map.computeIfPresent(key, (k, v) -> {
            v.removeExisting(t);
            if(v.isEmpty()) {
                return null;
            }
            return v;
        });
    }

    @Override
    public boolean contains(Triple t) {
        var key = getKey(t);
        var nestedMap = map.get(key);
        if(nestedMap == null) {
            return false;
        }
        return nestedMap.contains(t);
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public void clear() {
        this.map.clear();
    }

    @Override
    public int numberOfKeys() {
        return map.size() * map.values().stream().mapToInt(TripleMapWithOneKey::size).sum();
    }

    @Override
    public int size() {
        return map.values().stream().mapToInt(TripleMapWithOneKey::size).sum();
    }

    @Override
    public Stream<Triple> stream() {
        return map.values().stream().flatMap(TripleMapWithOneKey::stream);
    }

    @Override
    public Stream<Triple> stream(Node firstKeyNode) {
        var nestedMap = map.get(firstKeyNode.getIndexingValue().hashCode());
        if(nestedMap == null) {
            return Stream.empty();
        }
        return nestedMap.stream();
    }

    @Override
    public Iterator<Triple> iterator() {
        return new NestedTriplesIterator(map.values().iterator());
    }

    @Override
    public Iterator<Triple> iterator(Node firstKeyNode) {
        var nestedMap = map.get(firstKeyNode.getIndexingValue().hashCode());
        if(nestedMap == null) {
            return null;
        }
        return nestedMap.iterator();
    }

    @Override
    public Stream<Triple> stream(Node firstKeyNode, Node secondKeyNode) {
        var nestedMap = map.get(firstKeyNode.getIndexingValue().hashCode());
        if(nestedMap == null) {
            return Stream.empty();
        }
        return nestedMap.stream(secondKeyNode);
    }

    @Override
    public Iterator<Triple> iterator(Node firstKeyNode, Node secondKeyNode) {
        var nestedMap = map.get(firstKeyNode.getIndexingValue().hashCode());
        if(nestedMap == null) {
            return null;
        }
        return nestedMap.iterator(secondKeyNode);
    }

    private static class NestedTriplesIterator implements Iterator<Triple> {

        private final Iterator<TripleMapWithOneKey> baseIterator;
        private Iterator<Triple> subIterator;
        private boolean hasSubIterator = false;

        public NestedTriplesIterator(Iterator<TripleMapWithOneKey> baseIterator) {

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

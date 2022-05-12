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

package org.apache.jena.mem.hybrid;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class HybridTripleMap implements TripleMapWithOneKey {

    protected final static int SWITCH_TO_MAP_THRESHOLD = 9;
    protected final static float HASH_MAP_LOAD_FACTOR = 0.5f;
    private final Function<Triple, Node> keyNodeResolver;
    private final BiPredicate<Triple, Triple> containsPredicate;
    private Triple[] list = new Triple[SWITCH_TO_MAP_THRESHOLD];
    private int pos = 0;
    private HashMap<Integer, Triple[]> map;
    private boolean isList = true;

    public HybridTripleMap(final Function<Triple, Node> keyNodeResolver,
                           final BiPredicate<Triple, Triple> containsPredicate) {
        this.keyNodeResolver = keyNodeResolver;
        this.containsPredicate = containsPredicate;
    }

    private int getKey(final Triple t) {
        return keyNodeResolver.apply(t).getIndexingValue().hashCode();
    }

    @Override
    public boolean addIfNotExists(Triple t) {
        var key = getKey(t);
        if(isList) {
            var i=0;
            while (i != pos) {
                if(containsPredicate.test(list[i], t)) {
                    return false;
                }
                i++;
            }
            if(pos == SWITCH_TO_MAP_THRESHOLD) {
                this.map = new HashMap<>(SWITCH_TO_MAP_THRESHOLD, HASH_MAP_LOAD_FACTOR);
                for (Triple triple : list) {
                    this.map.compute(getKey(triple), (k, v) -> {
                        if(v == null) {
                            return new Triple[] {triple};
                        }
                        v = Arrays.copyOf(v, v.length+1);
                        v[v.length-1] = triple;
                        return v;
                    });
                }
                this.map.compute(key, (k, v) -> {
                    if(v == null) {
                        return new Triple[] {t};
                    }
                    v = Arrays.copyOf(v, v.length+1);
                    v[v.length-1] = t;
                    return v;
                });
                this.isList = false;
                this.list = null;
            } else {
                list[pos++] = t;
            }
            return true;
        } else {
            final boolean[] added = {false};
            map.compute(key, (k, v) -> {
                if(v == null) {
                    added[0] = true;
                    return new Triple[] {t};
                }
                for (Triple triple : v) {
                    if(containsPredicate.test(triple, t)) {
                        return v;
                    }
                }
                v = Arrays.copyOf(v, v.length+1);
                v[v.length-1] = t;
                added[0] = true;
                return v;
            });
            return added[0];
        }
    }

    @Override
    public void addDefinitetly(Triple t) {
        var key = getKey(t);
        if(isList) {
            if(pos == SWITCH_TO_MAP_THRESHOLD) {
                this.map = new HashMap<>(SWITCH_TO_MAP_THRESHOLD, HASH_MAP_LOAD_FACTOR);
                for (Triple triple : list) {
                    this.map.compute(getKey(triple), (k, v) -> {
                        if(v == null) {
                            return new Triple[] {triple};
                        }
                        v = Arrays.copyOf(v, v.length+1);
                        v[v.length-1] = triple;
                        return v;
                    });
                }
                this.map.compute(key, (k, v) -> {
                    if(v == null) {
                        return new Triple[] {t};
                    }
                    v = Arrays.copyOf(v, v.length+1);
                    v[v.length-1] = t;
                    return v;
                });
                this.isList = false;
                this.list = null;
            } else {
                list[pos++] = t;
            }
        } else {
            map.compute(key, (k, v) -> {
                if(v == null) {
                    return new Triple[] {t};
                }
                v = Arrays.copyOf(v, v.length+1);
                v[v.length-1] = t;
                return v;
            });
        }
    }

    @Override
    public boolean removeIfExits(Triple t) {
        var key = getKey(t);
        if(isList) {
            var i=0;
            while (i != pos) {
                if(containsPredicate.test(list[i], t)) {
                    list[i] = null;
                    var newV = Arrays.copyOf(list, list.length-1);
                    System.arraycopy(list, i+1, newV, i, newV.length-i);
                    list = newV;
                    pos--;
                    return true;
                }
                i++;
            }
            return false;
        }
        final boolean[] removed = {false};
        map.computeIfPresent(key, (k, v) -> {
            for (int i=0; i<v.length; i++) {
                if(containsPredicate.test(v[i], t)) {
                    if(v.length == 1) {
                        return null;
                    }
                    var newV = Arrays.copyOf(v, v.length-1);
                    System.arraycopy(v, i+1, newV, i, newV.length-i);
                    removed[0] = true;
                    return newV;
                }
            }
            return v;
        });
        return removed[0];
    }

    @Override
    public void removeExisting(Triple t) {
        var key = getKey(t);
        if(isList) {
            var i=0;
            while (i != pos) {
                if(containsPredicate.test(list[i], t)) {
                    list[i] = null;
                    var newV = Arrays.copyOf(list, list.length-1);
                    System.arraycopy(list, i+1, newV, i, newV.length-i);
                    list = newV;
                    pos--;
                    return;
                }
                i++;
            }
        } else {
            map.computeIfPresent(key, (k, v) -> {
                for (int i = 0; i < v.length; i++) {
                    if (containsPredicate.test(v[i], t)) {
                        if (v.length == 1) {
                            return null;
                        }
                        var newV = Arrays.copyOf(v, v.length-1);
                        System.arraycopy(v, i+1, newV, i, newV.length-i);
                        return newV;
                    }
                }
                return v;
            });
        }
    }

    @Override
    public boolean contains(Triple t) {
        var key = getKey(t);
        if(isList) {
            var i=0;
            while (i != pos) {
                if(containsPredicate.test(list[i], t)) {
                    return true;
                }
                i++;
            }
        } else {
            var l = map.get(key);
            if(l == null) {
                return false;
            }
            for (Triple triple : l) {
                if(containsPredicate.test(triple, t)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean isEmpty() {
        return isList ? pos > 0 : map.isEmpty();
    }

    @Override
    public void clear() {
        list = new Triple[SWITCH_TO_MAP_THRESHOLD];
        isList = true;
        map = null;
    }

    @Override
    public int numberOfKeys() {
        return isList ? 0 : map.size();
    }

    @Override
    public int size() {
        return isList ? pos : map.values().stream().mapToInt(a -> a.length).sum();
    }

    @Override
    public Stream<Triple> stream() {
        if(isList) {
            if(pos == list.length) {
                return Arrays.stream(list);
            } else {
                return Arrays.stream(Arrays.copyOf(list, pos));
            }
        }
        return map.values().stream().flatMap(a -> Arrays.stream(a));
    }

    @Override
    public Stream<Triple> stream(Node firstKeyNode) {
        if(isList) {
            return Arrays.stream(list).filter(t -> t != null && getKey(t) == firstKeyNode.getIndexingValue().hashCode());
        }
        var l = map.get(firstKeyNode.getIndexingValue().hashCode());
        if(l == null) {
            return Stream.empty();
        }
        return Arrays.stream(l);
    }

    @Override
    public Iterator<Triple> iterator() {
        if(isList) {
            return new ArrayIterator(list, pos);
        }
        return new NestedTriplesIterator(map.values().iterator());
    }

    @Override
    public Iterator<Triple> iterator(Node firstKeyNode) {
        if(isList) {
            return new IteratorFiltering(list, pos, t -> getKey(t) == firstKeyNode.getIndexingValue().hashCode());
        }
        var l = map.get(firstKeyNode.getIndexingValue().hashCode());
        if(l == null) {
            return null;
        }
        return new ArrayIterator(l);
    }

    private static class ArrayIterator implements Iterator<Triple> {

        private final Triple[] triples;
        private final int length;
        private int pos = 0;

        private ArrayIterator(Triple[] triples) {
            this(triples, triples.length);
        }

        private ArrayIterator(Triple[] triples, int length) {
            this.triples = triples;
            this.length = length;
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
            return pos < length;
        }

        /**
         * Returns the next element in the iteration.
         *
         * @return the next element in the iteration
         * @throws NoSuchElementException if the iteration has no more elements
         */
        @Override
        public Triple next() {
            return triples[pos++];
        }
    }

    private static class NestedTriplesIterator implements Iterator<Triple> {

        private final Iterator<Triple[]> baseIterator;
        private Iterator<Triple> subIterator;
        private boolean hasSubIterator = false;

        public NestedTriplesIterator(Iterator<Triple[]> baseIterator) {

            this.baseIterator = baseIterator;
            if (baseIterator.hasNext()) {
                subIterator = new ArrayIterator(baseIterator.next());
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
                    subIterator = new ArrayIterator(baseIterator.next());
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

    /**
     * Basically the same as FilterIterator<> but with clear and simple implementation without inheriting possibly
     * strange behaviour from any of the base classes.
     * This Iterator also directly supports wrapWithRemoveSupport
     */
    private static class IteratorFiltering implements Iterator<Triple> {

        private final Predicate<Triple> filter;
        private boolean hasCurrent = false;

        private final Triple[] triples;
        private final int length;
        private int pos = 0;

        /**
         The remembered current triple.
         */
        private Triple current;

        /**
         * Initialise this wrapping with the given base iterator and remove-control.
         *
         * @param filter        the filter predicate for this iteration
         */
        protected IteratorFiltering(Triple[] triples, int length, Predicate<Triple> filter) {
            this.triples = triples;
            this.length = length;
            this.filter = filter;
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
            while(!this.hasCurrent && pos < length) {
                var candidate = triples[pos++];
                this.hasCurrent = filter.test(candidate);
                if(this.hasCurrent) {
                    this.current = candidate;
                }
            }
            return this.hasCurrent;
        }

        /**
         Answer the next object, remembering it in <code>current</code>.
         @see Iterator#next()
         */
        @Override
        public Triple next()
        {
            if (hasCurrent || hasNext())
            {
                hasCurrent = false;
                return current;
            }
            throw new NoSuchElementException();
        }
    }
}

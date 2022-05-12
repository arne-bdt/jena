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

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class ArrayTripleMap implements TripleMapWithOneKey {

    private static int INITIAL_SIZE = 5;
    private static int GROW_BY_TRIPLES = 4;
    private int size = 0;
    private Triple[] list = new Triple[INITIAL_SIZE];

    private final Function<Triple, Node> keyNodeResolver;
    private final BiPredicate<Triple, Triple> containsPredicate;


    public ArrayTripleMap(final Function<Triple, Node> keyNodeResolver,
                          final BiPredicate<Triple, Triple> containsPredicate) {
        this.keyNodeResolver = keyNodeResolver;
        this.containsPredicate = containsPredicate;
    }

    private int getKey(final Triple t) {
        return keyNodeResolver.apply(t).getIndexingValue().hashCode();
    }

    private void grow() {
        var newList = new Triple[size + GROW_BY_TRIPLES];
        System.arraycopy(list, 0, newList, 0, size);
        list = newList;
    }

    @Override
    public boolean addIfNotExists(Triple t) {
        var key = getKey(t);
        var i=size;
        while (i > 0) {
            if(containsPredicate.test(t, list[--i])) {
                return false;
            }
        }
        if(size == list.length) {
            grow();
        }
        list[size++] = t;
        return true;
    }

    @Override
    public void addDefinitetly(Triple t) {
        if(size == list.length) {
            grow();
        }
        list[size++] = t;
        return;
    }

    @Override
    public boolean removeIfExits(Triple t) {
        var key = getKey(t);
        var i=size;
        while (i > 0) {
            if(containsPredicate.test(t, list[--i])) {
                list[i] = list[--size];
                return true;
            }
        }
        return false;
    }

    @Override
    public void removeExisting(Triple t) {
        var key = getKey(t);
        var i=size;
        while (i > 0) {
            if(containsPredicate.test(t, list[--i])) {
                list[i] = list[--size];
                return;
            }
        }
    }

    @Override
    public boolean contains(Triple t) {
        var key = getKey(t);
        var i=size;
        while (i > 0) {
            if(containsPredicate.test(t, list[--i])) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public void clear() {
        list = new Triple[INITIAL_SIZE];
    }

    @Override
    public int numberOfKeys() {
        return 1;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Stream<Triple> stream() {
        return Arrays.stream(list, 0, size);
    }

    @Override
    public Stream<Triple> stream(Node firstKeyNode) {
        return this.stream()
                .filter(t -> t != null && getKey(t) == firstKeyNode.getIndexingValue().hashCode());
    }

    @Override
    public Iterator<Triple> iterator() {
        if(this.isEmpty()) {
            return null;
        }
        return new ArrayIterator(list, size);
    }

    @Override
    public Iterator<Triple> iterator(Node firstKeyNode) {
        if(this.isEmpty()) {
            return null;
        }
        return new ArrayIterator(list, size);
//        return new IteratorFiltering(list, size,
//                t -> getKey(t) == firstKeyNode.getIndexingValue().hashCode());
    }

    private static class ArrayIterator implements Iterator<Triple> {

        private final Triple[] triples;
        private final int size;
        private int pos = 0;

        private ArrayIterator(Triple[] triples) {
            this(triples, triples.length);
        }

        private ArrayIterator(Triple[] triples, int size) {
            this.triples = triples;
            this.size = size;
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
            return pos < size;
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
        private final int size;
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
        protected IteratorFiltering(Triple[] triples, int size, Predicate<Triple> filter) {
            this.triples = triples;
            this.size = size;
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
            while(!this.hasCurrent && pos < size) {
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

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

package org.apache.jena.mem.hash_no_entry;

import org.apache.jena.graph.Triple;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

public abstract class ArrayValueMap<T> implements ValueMap<T> {

    private static int INITIAL_SIZE = 2;
    private static int GROW_BY_TRIPLES = 3;
    private int size = 0;
    private Object[] entries = new Object[INITIAL_SIZE];
    protected abstract Predicate<Object> getContainsMatcherForObject(final T value);

    public static Supplier<ValueMap<Triple>> forTriples = () -> new ArrayValueMap<Triple>() {

        @Override
        protected Predicate<Object> getContainsMatcherForObject(Triple triple) {
            if(ObjectEqualizer.isEqualsForObjectOk(triple.getObject())) {
                return t -> triple.equals(t);
            }
            return t -> triple.matches((Triple)t);
        }
    };


    protected ArrayValueMap() {
    }

    private void grow() {
        var newList = new Object[size + GROW_BY_TRIPLES];
        System.arraycopy(entries, 0, newList, 0, size);
        entries = newList;
    }

    @Override
    public boolean addIfNotExists(T value) {
        var i=size;
        while (i > 0) {
            if(value.equals(entries[--i])) {
                return false;
            }
        }
        if(size == entries.length) {
            grow();
        }
        entries[size++] = value;
        return true;
    }

    @Override
    public void addDefinitetly(T value) {
        if(size == entries.length) {
            grow();
        }
        entries[size++] = value;
    }

    @Override
    public boolean removeIfExits(T value) {
        var i=size;
        while (i > 0) {
            if(value.equals(entries[--i])) {
                entries[i] = entries[--size];
                return true;
            }
        }
        return false;
    }

    @Override
    public void removeExisting(T value) {
        var i=size;
        while (i > 0) {
            if(value.equals(entries[--i])) {
                entries[i] = entries[--size];
                return;
            }
        }
    }

    @Override
    public boolean contains(T value) {
        var matcher = getContainsMatcherForObject(value);
        var i=size;
        while (i > 0) {
            if(matcher.test((T)entries[--i])) {
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
        entries = new Object[INITIAL_SIZE];
        size = 0;
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
    public Stream<T> stream() {
        return Arrays.stream(entries, 0, size).map(entry -> (T)entry);
    }

    @Override
    public Stream<T> stream(final T valueWithSameKey) {
        return stream();
    }

    @Override
    public Iterator<T> iterator() {
        if(this.isEmpty()) {
            return null;
        }
        return new ArrayIterator(entries, size);
    }

    @Override
    public Iterator<T> iterator(final T valueWithSameKey) {
        return iterator();
    }

    private static class ArrayIterator<T> implements Iterator<T> {

        private final Object[] entries;
        private final int size;
        private int pos = 0;

        private ArrayIterator(final Object[] entries, final int size) {
            this.entries = entries;
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
        public T next() {
            if(hasNext()) {
                return (T) entries[pos++];
            }
            throw new NoSuchElementException();
        }
    }

//    private static class ArrayIteratorFiltering<T> implements Iterator<T> {
//
//        private final Object[] entries;
//        private final int size;
//        private final int hashIndex;
//        private final int referenceHashCode;
//        private int pos = 0;
//
//        private boolean hasCurrent = false;
//        private T current;
//
//        private ArrayIteratorFiltering(final Object[] entries, final int size, final int hashIndex, final int referenceHashCode) {
//            this.entries = entries;
//            this.size = size;
//            this.hashIndex = hashIndex;
//            this.referenceHashCode = referenceHashCode;
//        }
//
//        /**
//         * Returns {@code true} if the iteration has more elements.
//         * (In other words, returns {@code true} if {@link #next} would
//         * return an element rather than throwing an exception.)
//         *
//         * @return {@code true} if the iteration has more elements
//         */
//        @Override
//        public boolean hasNext() {
//            while(!hasCurrent && pos < size) {
//                var candidate = ((MapEntry<T>)entries[pos++]);
//                if(candidate.hashes[hashIndex] == referenceHashCode) {
//                    this.current = candidate.triple;
//                    hasCurrent = true;
//                }
//            }
//            return hasCurrent;
//        }
//
//        /**
//         * Returns the next element in the iteration.
//         *
//         * @return the next element in the iteration
//         * @throws NoSuchElementException if the iteration has no more elements
//         */
//        @Override
//        public T next() {
//            if (hasCurrent || hasNext())
//            {
//                hasCurrent = false;
//                return current;
//            }
//            throw new NoSuchElementException();
//        }
//    }
}

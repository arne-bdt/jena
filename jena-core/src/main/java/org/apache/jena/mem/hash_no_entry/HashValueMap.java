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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class HashValueMap<T> implements ValueMap<T> {

    /*Idea from hashmap: improve hash code by (h = key.hashCode()) ^ (h >>> 16)*/
    private int calcIndex(final int hashCode) {
        return calcIndex(hashCode, entries.length);
    }

    private int calcIndex(final int hashCode, final int length) {
        return (hashCode ^ (hashCode >>> 16)) & (length-1);
    }

    /*Idea from hashmap: improve hash code by (h = key.hashCode()) ^ (h >>> 16)*/
    private int calcIndex(final T value) {
        return calcIndex(getHashCode(value), entries.length);
    }

    private int calcIndex(final T value, final int length) {
        return calcIndex(getHashCode(value), length);
    }

    private static int MINIMUM_SIZE = 16;
    private static float loadFactor = 0.75f;
    private int size = 0;
    private Object[] entries;

    protected int getHashCode(T value) {
        return value.hashCode();
    }
    protected Predicate<T> getMatcherForObject(final T value) {
        return other -> value.equals(other);
    }

    public static Supplier<ValueMap<Triple>> forSubject = () -> new HashValueMap<Triple>() {

        @Override
        protected int getHashCode(Triple value) {
            return value.getSubject().getIndexingValue().hashCode();
        }

        @Override
        protected Predicate<Triple> getMatcherForObject(final Triple triple) {
            if(ObjectEqualizer.isEqualsForObjectOk(triple.getObject())) {
                return t -> triple.equals(t);
            }
            return t -> triple.matches(t);
        }
    };

    public static Supplier<ValueMap<Triple>> forPredicate = () -> new HashValueMap<Triple>() {
        @Override
        protected int getHashCode(Triple value) {
            return value.getPredicate().getIndexingValue().hashCode();
        }

        @Override
        protected Predicate<Triple> getMatcherForObject(final Triple triple) {
            if(ObjectEqualizer.isEqualsForObjectOk(triple.getObject())) {
                return t -> triple.equals(t);
            }
            return t -> triple.matches(t);
        }
    };

    public static Supplier<ValueMap<Triple>> forObject = () -> new HashValueMap<Triple>() {
        @Override
        protected int getHashCode(Triple value) {
            return value.getObject().getIndexingValue().hashCode();
        }

        @Override
        protected Predicate<Triple> getMatcherForObject(final Triple triple) {
            if(ObjectEqualizer.isEqualsForObjectOk(triple.getObject())) {
                return t -> triple.equals(t);
            }
            return t -> triple.matches(t);
        }
    };

    public HashValueMap() {
        this.entries = new Object[MINIMUM_SIZE]; /*default capacity*/
    }

    private int calcNewSize() {
        if(size >= entries.length*loadFactor && entries.length <= 1 << 30) { /*grow*/
            return entries.length << 1;
        }
//        } else if (size < (entries.length >> 2)) { /*shrink*/
//            var new_size = entries.length >> 1;
//            if(new_size < MINIMUM_SIZE) {
//                new_size = MINIMUM_SIZE;
//            }
//            if(entries.length != new_size) {
//                return new_size;
//            }
//        }
        return -1;
    }

    private void grow() {
        final var newSize = calcNewSize();
        if(newSize < 0) {
            return;
        }
        final var newEntries = new Object[newSize];
        for(int i=0; i<entries.length; i++) {
            var entryToMove = (MapEntry<T>) entries[i];
            if(entryToMove == null) {
                continue;
            }
            do {
                var newIndex = calcIndex(entryToMove.value, newEntries.length);
                var entryInTarget = (MapEntry<T>)newEntries[newIndex];
                var copyOfNext = entryToMove.next;
                /*insert as first element --> reversing order*/
                entryToMove.next = entryInTarget;
                newEntries[newIndex] = entryToMove;
                entryToMove = copyOfNext;
            } while (entryToMove != null);
        }
        entries = newEntries;
    }

//    private int countValues(Object[] entries) {
//        return Arrays.stream(entries)
//                .filter(entry -> entry != null)
//                .map(entry -> (MapEntry<T>) entry)
//                .mapToInt(entry -> {
//                    var size = 0;
//                    while (entry != null) {
//                        size++;
//                        entry = entry.nextEntries[getHashIndex()];
//                    }
//                    return size;
//                }).sum();
//    }

    @Override
    public boolean addIfNotExists(T value) {
        final var index = calcIndex(value);
        var existingEntry = (MapEntry<T>)entries[index];
        if(existingEntry == null) {
            entries[index] = new MapEntry(value);
            size++;
            grow();
            return true;
        }
        if(value.equals(existingEntry.value)) {
            return false;
        }
        while(existingEntry.next != null) {
            existingEntry = existingEntry.next;
            if(value.equals(existingEntry.value)) {
                return false;
            }
        }
        /*insert as last element*/
        existingEntry.next = new MapEntry(value);
        size++;
        grow();
        return true;
    }

    @Override
    public void addDefinitetly(T value) {
        final var newEntry = new MapEntry<>(value);
        final var index = calcIndex(value);
        var existingEntry = (MapEntry<T>)entries[index];
        if(existingEntry == null) {
            entries[index] = newEntry;
            size++;
            grow();
            return;
        }
        /*insert as first element*/
        newEntry.next = existingEntry;
        entries[index] = newEntry;
        size++;
        grow();
    }

    @Override
    public boolean removeIfExits(T value) {
        var index = calcIndex(value);
        var existingEntry = (MapEntry<T>)entries[index];
        if(existingEntry == null) {
            return false;
        }
        if(value.equals(existingEntry.value)) {
            entries[index] = existingEntry.next;
            size--;
            grow();
            return true;
        }
        var previousEntry = existingEntry;
        while(existingEntry.next != null) {
            existingEntry = existingEntry.next;
            if(value.equals(existingEntry.value)) {
                previousEntry.next = existingEntry.next;
                size--;
                grow();
                return true;
            }
            previousEntry = existingEntry;
        }
        return false;
    }

    @Override
    public void removeExisting(T value) {
        var index = calcIndex(value);
        var existingEntry = (MapEntry<T>)entries[index];
        if(value.equals(existingEntry.value)) {
            entries[index] = existingEntry.next;
            size--;
            grow();
            return;
        }
        var previousEntry = existingEntry;
        while(existingEntry.next != null) {
            existingEntry = existingEntry.next;
            if(value.equals(existingEntry.value)) {
                previousEntry.next = existingEntry.next;
                size--;
                grow();
                return;
            }
            previousEntry = existingEntry;
        }
    }

    @Override
    public boolean contains(T value) {
        var index = calcIndex(value);
        var existingEntry = (MapEntry<T>)entries[index];
        if(existingEntry == null) {
           return false;
        }
        var matcher = getMatcherForObject(value);
        if(matcher.test(existingEntry.value)) {
            return true;
        }
        while(existingEntry.next != null) {
            existingEntry = existingEntry.next;
            if(matcher.test(existingEntry.value)) {
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
        entries = new Object[MINIMUM_SIZE];
        size = 0;
    }

    @Override
    public int numberOfKeys() {
        return size;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Stream<T> stream() {
        return StreamSupport.stream(new MapEntriesSpliterator(entries), false);
    }

    @Override
    public Stream<T> stream(final T valueWithSameKey) {
        final var hashCode = getHashCode(valueWithSameKey);
        var index = calcIndex(hashCode);
        var existingEntry = (MapEntry<T>)entries[index];
        while(existingEntry != null && hashCode != getHashCode(existingEntry.value)) {
            existingEntry = existingEntry.next;
        }
        if(existingEntry == null) {
            return Stream.empty();
        }
        return StreamSupport.stream(new MapEntrySpliterator(existingEntry), false);
    }

    @Override
    public Iterator<T> iterator() {
        if(this.isEmpty()) {
            return null;
        }
        return new MapEntriesIterator(entries);
    }

    @Override
    public Iterator<T> iterator(final T valueWithSameKey) {
        final var hashCode = getHashCode(valueWithSameKey);
        var index = calcIndex(hashCode);
        var existingEntry = (MapEntry<T>)entries[index];
        while(existingEntry != null && hashCode != getHashCode(existingEntry.value)) {
            existingEntry = existingEntry.next;
        }
        if(existingEntry == null) {
            return null;
        }
        return new MapEntryIterator(existingEntry);
    }

    private static class MapEntryIterator<T> implements Iterator<T> {

        private MapEntry<T> mapEntry;

        public MapEntryIterator(MapEntry<T> mapEntry) {
            this.mapEntry = mapEntry;
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
            return this.mapEntry != null;
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
                var next = this.mapEntry.value;
                this.mapEntry = this.mapEntry.next;
                return next;
            }
            throw new NoSuchElementException();
        }
    }

    private static class MapEntriesIterator<T> implements Iterator<T> {

        private final Object[] entries;
        private Iterator<T> currentIterator;
        private int pos = 0;
        private boolean hasNext = false;

        public MapEntriesIterator(Object[] entries) {
            this.entries = entries;
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
            if(!hasNext) {
                if(currentIterator != null && currentIterator.hasNext()) {
                    hasNext = true;
                    return true;
                }
                while(!hasNext && pos < entries.length) {
                    var mapEntry = (MapEntry<T>)entries[pos++];
                    if(mapEntry != null) {
                        currentIterator = new MapEntryIterator<>(mapEntry);
                        if(currentIterator.hasNext()) {
                            hasNext = true;
                        }
                    }
                }
            }
            return hasNext;
        }

        /**
         * Returns the next element in the iteration.
         *
         * @return the next element in the iteration
         * @throws NoSuchElementException if the iteration has no more elements
         */
        @Override
        public T next() {
            if (hasNext || hasNext())
            {
                hasNext = false;
                return currentIterator.next();
            }
            throw new NoSuchElementException();
        }
    }

    /*TODO: Implement trySplit to support parallel processing*/
    private static class MapEntriesSpliterator<T> implements Spliterator<T> {

        private MapEntriesIterator<T> entriesIterator;

        public MapEntriesSpliterator(Object[] mapEntries) {
            this.entriesIterator = new MapEntriesIterator<>(mapEntries);
        }


        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            if(!entriesIterator.hasNext()) {
                return false;
            }
            action.accept(entriesIterator.next());
            return true;
        }


        @Override
        public Spliterator<T> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }


        @Override
        public int characteristics() {
            return DISTINCT | NONNULL;
        }
    }

    private static class MapEntrySpliterator<T> implements Spliterator<T> {

        private MapEntry<T> mapEntry;

        public MapEntrySpliterator(MapEntry<T> mapEntry) {
            this.mapEntry = mapEntry;
        }


        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            if(mapEntry == null) {
                return false;
            }
            action.accept(mapEntry.value);
            mapEntry = mapEntry.next;
            return true;
        }


        @Override
        public Spliterator<T> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }


        @Override
        public int characteristics() {
            return DISTINCT | NONNULL;
        }
    }
}

package org.apache.jena.mem.hash;

import org.apache.jena.graph.Triple;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class HashValueMap<T> implements ValueMap<T> {

    /*Idea from hashmap: improve hash code by (h = key.hashCode()) ^ (h >>> 16)*/
    private int calcIndex(final int hashCode) {
        return (hashCode ^ (hashCode >>> 16)) & (entries.length-1);
    }

    private int calcIndex(final int hashCode, final int length) {
        return (hashCode ^ (hashCode >>> 16)) & (length-1);
    }

    private static int MINIMUM_SIZE = 2;
    private static float loadFactor = 2.5f;
    private int size = 0;
    private Object[] entries;

    protected abstract int getHashCode(T value);
    protected abstract MapEntry<T> createEntry(T value, int hashCode);
    protected abstract MapEntry<T> createEntry(T value);
    protected abstract int getHashIndex();
    protected abstract boolean matches(final T value1, final T value2);
    protected abstract boolean matches(final MapEntry<T> entry1, final MapEntry<T> entry2);

    private static boolean compareHashes(int[] hashes1, int[] hashes2) {
        for(int i=0; i<hashes1.length; i++) {
            if(hashes1[i] != hashes2[i]) {
                return false;
            }
        }
        return true;
    }

    public static Supplier<ValueMap<Triple>> forSubject = () -> new HashValueMap<Triple>() {

        @Override
        protected int getHashCode(Triple value) {
            return value.getSubject().getIndexingValue().hashCode();
        }

        @Override
        protected MapEntry<Triple> createEntry(Triple value) {
            var hashes = new int[] {
                    value.getSubject().getIndexingValue().hashCode(),
                    value.getPredicate().getIndexingValue().hashCode(),
                    value.getObject().getIndexingValue().hashCode()};
            return new MapEntry<Triple>(value, hashes);
        }

        @Override
        protected MapEntry<Triple> createEntry(Triple value, int hashCode) {
            var hashes = new int[] {
                    hashCode,
                    value.getPredicate().getIndexingValue().hashCode(),
                    value.getObject().getIndexingValue().hashCode()};
            return new MapEntry<Triple>(value, hashes);
        }

        @Override
        protected int getHashIndex() {
            return 0;
        }

        @Override
        protected boolean matches(final Triple value1, final Triple value2) {
            return value1.getSubject().equals(value2.getSubject())
                    && value1.getObject().sameValueAs(value2.getObject())
                    && value1.getPredicate().equals(value2.getPredicate());
        }

        @Override
        protected boolean matches(final MapEntry<Triple> entry1, final MapEntry<Triple> entry2) {
            return compareHashes(entry1.hashes, entry2.hashes)
                    && entry1.value.getSubject().equals(entry2.value.getSubject())
                    && entry1.value.getObject().sameValueAs(entry2.value.getObject())
                    && entry1.value.getPredicate().equals(entry2.value.getPredicate());
        }
    };

    public static Supplier<ValueMap<Triple>> forPredicate = () -> new HashValueMap<Triple>() {
        @Override
        protected int getHashCode(Triple value) {
            return value.getPredicate().getIndexingValue().hashCode();
        }

        @Override
        protected MapEntry<Triple> createEntry(Triple value) {
            var hashes = new int[] {
                    value.getSubject().getIndexingValue().hashCode(),
                    value.getPredicate().getIndexingValue().hashCode(),
                    value.getObject().getIndexingValue().hashCode()};
            return new MapEntry<Triple>(value, hashes);
        }

        @Override
        protected MapEntry<Triple> createEntry(Triple value, int hashCode) {
            var hashes = new int[] {
                    value.getSubject().getIndexingValue().hashCode(),
                    hashCode,
                    value.getObject().getIndexingValue().hashCode()};
            return new MapEntry<Triple>(value, hashes);
        }

        @Override
        protected int getHashIndex() {
            return 1;
        }

        @Override
        protected boolean matches(final Triple value1, final Triple value2) {
            return value1.getSubject().equals(value2.getSubject())
                    && value1.getPredicate().equals(value2.getPredicate())
                    && value1.getObject().sameValueAs(value2.getObject());
        }

        @Override
        protected boolean matches(final MapEntry<Triple> entry1, final MapEntry<Triple> entry2) {
            return compareHashes(entry1.hashes, entry2.hashes)
                    && entry1.value.getSubject().equals(entry2.value.getSubject())
                    && entry1.value.getObject().sameValueAs(entry2.value.getObject())
                    && entry1.value.getPredicate().equals(entry2.value.getPredicate());
        }
    };

    public static Supplier<ValueMap<Triple>> forObject = () -> new HashValueMap<Triple>() {
        @Override
        protected int getHashCode(Triple value) {
            return value.getObject().getIndexingValue().hashCode();
        }

        @Override
        protected MapEntry<Triple> createEntry(Triple value) {
            var hashes = new int[] {
                    value.getSubject().getIndexingValue().hashCode(),
                    value.getPredicate().getIndexingValue().hashCode(),
                    value.getObject().getIndexingValue().hashCode()};
            return new MapEntry<Triple>(value, hashes);
        }

        @Override
        protected MapEntry<Triple> createEntry(Triple value, int hashCode) {
            var hashes = new int[] {
                    value.getSubject().getIndexingValue().hashCode(),
                    value.getPredicate().getIndexingValue().hashCode(),
                    hashCode};
            return new MapEntry<Triple>(value, hashes);
        }

        @Override
        protected int getHashIndex() {
            return 2;
        }

        @Override
        protected boolean matches(final Triple value1, final Triple value2) {
            return value1.getObject().sameValueAs(value2.getObject())
                    && value1.getPredicate().equals(value2.getPredicate())
                    && value1.getSubject().equals(value2.getSubject());
        }

        @Override
        protected boolean matches(final MapEntry<Triple> entry1, final MapEntry<Triple> entry2) {
            return compareHashes(entry1.hashes, entry2.hashes)
                    && entry1.value.getObject().sameValueAs(entry2.value.getObject())
                    && entry1.value.getSubject().equals(entry2.value.getSubject())
                    && entry1.value.getPredicate().equals(entry2.value.getPredicate());
        }
    };

    protected HashValueMap() {
        this.entries = new Object[MINIMUM_SIZE]; /*default capacity*/
    }

    private int calcNewSize() {
        if(size >= entries.length*loadFactor && entries.length <= 1 << 30) { /*grow*/
            return entries.length << 1;
        } else if (size < (entries.length >> 2)) { /*shrink*/
            var new_size = entries.length >> 1;
            if(new_size < MINIMUM_SIZE) {
                new_size = MINIMUM_SIZE;
            }
            if(entries.length != new_size) {
                return new_size;
            }
        }
        return -1;
    }

    private void growOrShrink() {
        final var newSize = calcNewSize();
        if(newSize < 0) {
            return;
        }
        final var hashIndex = getHashIndex();
        final var newEntries = new Object[newSize];
        for(int i=0; i<entries.length; i++) {
            var entryToMove = (MapEntry<T>) entries[i];
            if(entryToMove == null) {
                continue;
            }
            do {
                var newIndex = calcIndex(entryToMove.hashes[hashIndex], newEntries.length);
                var entryInTarget = (MapEntry<T>)newEntries[newIndex];
                var copyOfNext = entryToMove.nextEntries[hashIndex];
                /*insert as first element --> reversing order*/
                entryToMove.nextEntries[hashIndex] = entryInTarget;
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
    public MapEntry<T> addIfNotExists(T value) {
        final var newEntry = createEntry(value);
        final var hashIndex = getHashIndex();
        final var index = calcIndex(newEntry.hashes[hashIndex]);
        var existingEntry = (MapEntry<T>)entries[index];
        if(existingEntry == null) {
            entries[index] = newEntry;
            size++;
            growOrShrink();
            return newEntry;
        }
        if(matches(existingEntry, newEntry)) {
            return null;
        }
        while(existingEntry.nextEntries[hashIndex] != null) {
            existingEntry = existingEntry.nextEntries[hashIndex];
            if(matches(existingEntry, newEntry)) {
                return null;
            }
        }
        /*insert as last element*/
        existingEntry.nextEntries[hashIndex] = newEntry;
        size++;
        growOrShrink();
        return newEntry;
    }

    @Override
    public MapEntry<T> addDefinitetly(T value) {
        final var newEntry = createEntry(value);
        final var hashIndex = getHashIndex();
        final var index = calcIndex(newEntry.hashes[hashIndex]);
        var existingEntry = (MapEntry<T>)entries[index];
        if(existingEntry == null) {
            entries[index] = newEntry;
            size++;
            growOrShrink();
            return newEntry;
        }
        /*insert as first element*/
        newEntry.nextEntries[hashIndex] = existingEntry;
        entries[index] = newEntry;
        size++;
        growOrShrink();
        return newEntry;
    }

    @Override
    public void addDefinitetly(MapEntry<T> entry) {
        final var hashIndex = getHashIndex();
        final var index = calcIndex(entry.hashes[hashIndex]);
        var existingEntry = (MapEntry<T>)entries[index];
        if(existingEntry == null) {
            entries[index] = entry;
            size++;
            growOrShrink();
            return;
        }
        /*insert as first element*/
        entry.nextEntries[hashIndex] = existingEntry;
        entries[index] = entry;
        size++;
        growOrShrink();
    }

    @Override
    public MapEntry<T> removeIfExits(T value) {
        var entry = createEntry(value);
        final var hashIndex = getHashIndex();
        var index = calcIndex(entry.hashes[hashIndex]);
        var existingEntry = (MapEntry<T>)entries[index];
        if(existingEntry == null) {
            return null;
        }
        if(matches(existingEntry, entry)) {
            entries[index] = existingEntry.nextEntries[hashIndex];
            size--;
            growOrShrink();
            return existingEntry;
        }
        var previousEntry = existingEntry;
        while(existingEntry.nextEntries[hashIndex] != null) {
            existingEntry = existingEntry.nextEntries[hashIndex];
            if(matches(existingEntry, entry)) {
                previousEntry.nextEntries[hashIndex] = existingEntry.nextEntries[hashIndex];
                size--;
                growOrShrink();
                return existingEntry;
            }
            previousEntry = existingEntry;
        }
        return null;
    }

    @Override
    public void removeExisting(MapEntry<T> entry) {
        final var hashIndex = getHashIndex();
        var index = calcIndex(entry.hashes[hashIndex]);
        var existingEntry = (MapEntry<T>)entries[index];
        if(matches(existingEntry, entry)) {
            entries[index] = existingEntry.nextEntries[hashIndex];
            size--;
            growOrShrink();
            return;
        }
        var previousEntry = existingEntry;
        while(existingEntry.nextEntries[hashIndex] != null) {
            existingEntry = existingEntry.nextEntries[hashIndex];
            if(matches(existingEntry, entry)) {
                previousEntry.nextEntries[hashIndex] = existingEntry.nextEntries[hashIndex];
                size--;
                growOrShrink();
                return;
            }
            previousEntry = existingEntry;
        }
    }

    @Override
    public boolean contains(T value) {
        final var hashCode = getHashCode(value);
        final var hashIndex = getHashIndex();
        var index = calcIndex(hashCode);
        var existingEntry = (MapEntry<T>)entries[index];
        if(existingEntry == null) {
           return false;
        }
        var entry = createEntry(value, hashCode);
        if(matches(existingEntry, entry)) {
            return true;
        }
        while(existingEntry.nextEntries[hashIndex] != null) {
            existingEntry = existingEntry.nextEntries[hashIndex];
            if(matches(existingEntry, entry)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean contains(final MapEntry<T> entry) {
        final var hashIndex = getHashIndex();
        var index = calcIndex(entry.hashes[hashIndex]);
        var existingEntry = (MapEntry<T>)entries[index];
        if(existingEntry == null) {
            return false;
        }
        if(matches(existingEntry, entry)) {
            return true;
        }
        while(existingEntry.nextEntries[hashIndex] != null) {
            existingEntry = existingEntry.nextEntries[hashIndex];
            if(matches(existingEntry, entry)) {
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
        return StreamSupport.stream(new MapEntriesSpliterator(entries, getHashIndex()), false);
    }

    @Override
    public Stream<T> stream(final T valueWithSameKey) {
        final var hashCode = getHashCode(valueWithSameKey);
        var index = calcIndex(hashCode);
        var existingEntry = (MapEntry<T>)entries[index];
        var hashIndex = getHashIndex();
        while(existingEntry != null && existingEntry.hashes[hashIndex] != hashCode) {
            existingEntry = existingEntry.nextEntries[hashIndex];
        }
        if(existingEntry == null) {
            return Stream.empty();
        }
        return StreamSupport.stream(new MapEntrySpliterator(existingEntry, getHashIndex()), false);
    }

    @Override
    public Iterator<T> iterator() {
        if(this.isEmpty()) {
            return null;
        }
        return new MapEntriesIterator(entries, getHashIndex());
    }

    @Override
    public Iterator<T> iterator(final T valueWithSameKey) {
        final var hashCode = getHashCode(valueWithSameKey);
        var index = calcIndex(hashCode);
        var existingEntry = (MapEntry<T>)entries[index];
        var hashIndex = getHashIndex();
        while(existingEntry != null && existingEntry.hashes[hashIndex] != hashCode) {
            existingEntry = existingEntry.nextEntries[hashIndex];
        }
        if(existingEntry == null) {
            return null;
        }
        return new MapEntryIterator(existingEntry, getHashIndex());
    }

    private static class MapEntryIterator<T> implements Iterator<T> {

        private MapEntry<T> mapEntry;
        private final int hashIndex;

        public MapEntryIterator(MapEntry<T> mapEntry, int hashIndex) {
            this.mapEntry = mapEntry;
            this.hashIndex = hashIndex;
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
                this.mapEntry = this.mapEntry.nextEntries[hashIndex];
                return next;
            }
            throw new NoSuchElementException();
        }
    }

    private static class MapEntriesIterator<T> implements Iterator<T> {

        private final Object[] entries;
        private Iterator<T> currentIterator;
        private final int hashIndex;
        private int pos = 0;
        private boolean hasNext = false;

        public MapEntriesIterator(Object[] entries, int hashIndex) {
            this.entries = entries;
            this.hashIndex = hashIndex;
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
                        currentIterator = new MapEntryIterator<>(mapEntry, hashIndex);
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

        public MapEntriesSpliterator(Object[] mapEntries, int hashIndex) {
            this.entriesIterator = new MapEntriesIterator<>(mapEntries, hashIndex);
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
        private final int hashIndex;

        public MapEntrySpliterator(MapEntry<T> mapEntry, int hashIndex) {
            this.mapEntry = mapEntry;
            this.hashIndex = hashIndex;
        }


        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            if(mapEntry == null) {
                return false;
            }
            action.accept(mapEntry.value);
            mapEntry = mapEntry.nextEntries[hashIndex];
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

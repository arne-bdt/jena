package org.apache.jena.mem.hash;

import org.apache.jena.graph.Triple;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class HashKeyMap<T> implements ValueMap<T> {

    /*Idea from hashmap: improve hash code by (h = key.hashCode()) ^ (h >>> 16)*/

    private float loadFactor = 0.5f;
    private static int MINIMUM_SIZE = 16;
    private int mapSize = 0;
    private Object[] entries;

    protected abstract int getHashCode(T value);
    protected abstract int getHashCode(MapEntry<T> value);
    protected abstract KeyEntry<T> createEntry(int hashCode);
    protected abstract boolean matches(final T value1, final T value2);
    protected abstract boolean containsPrimaryIndex(final T value);
    protected abstract boolean containsSecondaryIndex(final T value);

    private static boolean compareHashes(int[] hashes1, int[] hashes2) {
        for(int i=0; i<hashes1.length; i++) {
            if(hashes1[i] != hashes2[i]) {
                return false;
            }
        }
        return true;
    }

    public static Supplier<ValueMap<Triple>> forSubject = () -> new HashKeyMap<Triple>() {

        @Override
        protected int getHashCode(Triple value) {
            return value.getSubject().getIndexingValue().hashCode();
        }

        @Override
        protected int getHashCode(MapEntry<Triple> value) {
            return value.hashes[0];
        }

        @Override
        protected boolean containsPrimaryIndex(final Triple value) {
            return value.getSubject().isConcrete();
        }

        @Override
        protected boolean containsSecondaryIndex(final Triple value) {
            return value.getObject().isConcrete();
        }

        @Override
        protected KeyEntry<Triple> createEntry(int hashCode) {
            var map = HashValueMap.forObject.get(); /*nested map has object as key*/
            return new KeyEntry<Triple>(hashCode, map);
        }

        @Override
        protected boolean matches(final Triple value1, final Triple value2) {
            return value1.getSubject().equals(value2.getSubject())
                    && value1.getObject().sameValueAs(value2.getObject())
                    && value1.getPredicate().equals(value2.getPredicate());
        }
    };

    public static Supplier<ValueMap<Triple>> forPredicate = () -> new HashKeyMap<Triple>() {
        @Override
        protected int getHashCode(Triple value) {
            return value.getPredicate().getIndexingValue().hashCode();
        }

        @Override
        protected int getHashCode(MapEntry<Triple> value) {
            return value.hashes[1];
        }

        @Override
        protected boolean containsPrimaryIndex(final Triple value) {
            return value.getPredicate().isConcrete();
        }

        @Override
        protected boolean containsSecondaryIndex(Triple value) {
            return value.getSubject().isConcrete();
        }

        @Override
        protected KeyEntry<Triple> createEntry(int hashCode) {
            var map = HashValueMap.forSubject.get(); /*nested map has subject as key*/
            return new KeyEntry<Triple>(hashCode, map);
        }

        @Override
        protected boolean matches(final Triple value1, final Triple value2) {
            return value1.getSubject().equals(value2.getSubject())
                    && value1.getPredicate().equals(value2.getPredicate())
                    && value1.getObject().sameValueAs(value2.getObject());
        }
    };

    public static Supplier<ValueMap<Triple>> forObject = () -> new HashKeyMap<Triple>() {
        @Override
        protected int getHashCode(Triple value) {
            return value.getObject().getIndexingValue().hashCode();
        }

        @Override
        protected int getHashCode(MapEntry<Triple> value) {
            return value.hashes[2];
        }

        @Override
        protected boolean containsPrimaryIndex(final Triple value) {
            return value.getObject().isConcrete();
        }

        @Override
        protected boolean containsSecondaryIndex(Triple value) {
            return value.getPredicate().isConcrete();
        }

        @Override
        protected KeyEntry<Triple> createEntry(int hashCode) {
            var map = HashValueMap.forPredicate.get(); /*nested map has predicate as key*/
            return new KeyEntry<Triple>(hashCode, map);
        }

        @Override
        protected boolean matches(final Triple value1, final Triple value2) {
            return value1.getObject().sameValueAs(value2.getObject())
                    && value1.getPredicate().equals(value2.getPredicate())
                    && value1.getSubject().equals(value2.getSubject());
        }
    };

    protected HashKeyMap() {
        this.entries = new Object[MINIMUM_SIZE]; /*default capacity*/
    }

    private int calcNewSize() {
        if(mapSize >= entries.length*loadFactor && entries.length <= 1 << 30) { /*grow*/
            return entries.length << 1;
        } else if (mapSize < (entries.length >> 2)) { /*shrink*/
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
        var countBefore = countValues(entries);
        final var newEntries = new Object[newSize];
        for(int i=0; i<entries.length; i++) {
            var entryToMove = (KeyEntry<T>) entries[i];
            if(entryToMove == null) {
                continue;
            }
            do {
                var newIndex = entryToMove.hash & (newEntries.length - 1);
                var entryInTarget = (KeyEntry<T>)newEntries[newIndex];
                var copyOfNext = entryToMove.next;
                /*insert as first element --> reversing order*/
                entryToMove.next = entryInTarget;
                newEntries[newIndex] = entryToMove;
                entryToMove = copyOfNext;
            } while (entryToMove != null);
        }
        var countAfter = countValues(newEntries);
        if(countBefore != countAfter) {
            int i=0;
        }
        entries = newEntries;
    }
    private int countValues(Object[] entries) {
        return Arrays.stream(entries)
                .filter(entry -> entry != null)
                .map(entry -> (KeyEntry<T>) entry)
                .mapToInt(entry -> {
                    var size = 0;
                    while (entry != null) {
                        size += entry.value.size();
                        entry = entry.next;
                    }
                    return size;
                }).sum();
    }

    @Override
    public MapEntry<T> addIfNotExists(T value) {
        final var hashCode = getHashCode(value);
        final var index = hashCode & (entries.length-1);
        var existingKeyEntry = (KeyEntry<T>)entries[index];
        if(existingKeyEntry == null) {
            var newKeyEntry = createEntry(hashCode);
            entries[index] = newKeyEntry;
            mapSize++;
            growOrShrink();
            return newKeyEntry.value.addDefinitetly(value);
        }
        if(existingKeyEntry.hash == hashCode) {
            return existingKeyEntry.value.addIfNotExists(value);
        }
        while(existingKeyEntry.next != null) {
            existingKeyEntry = existingKeyEntry.next;
            if(existingKeyEntry.hash == hashCode) {
                return existingKeyEntry.value.addIfNotExists(value);
            }
        }
        var newKeyEntry = createEntry(hashCode);
        /*insert as last element*/
        existingKeyEntry.next = newKeyEntry;
        mapSize++;
        growOrShrink();
        return newKeyEntry.value.addDefinitetly(value);
    }

    @Override
    public MapEntry<T> addDefinitetly(T value) {
        final var hashCode = getHashCode(value);
        final var index = hashCode & (entries.length-1);
        var existingKeyEntry = (KeyEntry<T>)entries[index];
        if(existingKeyEntry == null) {
            var newKeyEntry = createEntry(hashCode);
            entries[index] = newKeyEntry;
            mapSize++;
            growOrShrink();
            return newKeyEntry.value.addDefinitetly(value);
        }
        if(existingKeyEntry.hash == hashCode) {
            return existingKeyEntry.value.addDefinitetly(value);
        }
        while(existingKeyEntry.next != null) {
            existingKeyEntry = existingKeyEntry.next;
            if(existingKeyEntry.hash == hashCode) {
                return existingKeyEntry.value.addDefinitetly(value);
            }
        }
        var newKeyEntry = createEntry(hashCode);
        /*insert as last element*/
        existingKeyEntry.next = newKeyEntry;
        mapSize++;
        growOrShrink();
        return newKeyEntry.value.addDefinitetly(value);
    }

    @Override
    public void addDefinitetly(MapEntry<T> entry) {
        final var hashCode = getHashCode(entry);
        final var index = hashCode & (entries.length-1);
        var existingKeyEntry = (KeyEntry<T>)entries[index];
        if(existingKeyEntry == null) {
            var newKeyEntry = createEntry(hashCode);
            entries[index] = newKeyEntry;
            mapSize++;
            growOrShrink();
            newKeyEntry.value.addDefinitetly(entry);
            return;
        }
        if(existingKeyEntry.hash == hashCode) {
            existingKeyEntry.value.addDefinitetly(entry);
            return;
        }
        while(existingKeyEntry.next != null) {
            existingKeyEntry = existingKeyEntry.next;
            if(existingKeyEntry.hash == hashCode) {
                existingKeyEntry.value.addDefinitetly(entry);
                return;
            }
        }
        var newKeyEntry = createEntry(hashCode);
        /*insert as last element*/
        existingKeyEntry.next = newKeyEntry;
        mapSize++;
        growOrShrink();
        newKeyEntry.value.addDefinitetly(entry);
    }

    @Override
    public MapEntry<T> removeIfExits(T value) {
        final var hashCode = getHashCode(value);
        final var index = hashCode & (entries.length-1);
        var existingKeyEntry = (KeyEntry<T>)entries[index];
        if(existingKeyEntry == null) {
            return null;
        }
        if(existingKeyEntry.hash == hashCode) {
            var entry = existingKeyEntry.value.removeIfExits(value);
            if(entry != null) {
                if(existingKeyEntry.value.size() == 0) {
                    entries[index] = existingKeyEntry.next;
                    mapSize--;
                    growOrShrink();
                }
            }
            return entry;
        }
        while(existingKeyEntry.next != null) {
            existingKeyEntry = existingKeyEntry.next;
            if(existingKeyEntry.hash == hashCode) {
                var entry = existingKeyEntry.value.removeIfExits(value);
                if(entry != null) {
                    if(existingKeyEntry.value.size() == 0) {
                        entries[index] = existingKeyEntry.next;
                        mapSize--;
                        growOrShrink();
                    }
                }
                return entry;
            }
        }
        return null;
    }

    @Override
    public void removeExisting(MapEntry<T> entry) {
        final var hashCode = getHashCode(entry);
        final var index = hashCode & (entries.length-1);
        var existingKeyEntry = (KeyEntry<T>)entries[index];
        if(existingKeyEntry == null) {
            return;
        }
        if(existingKeyEntry.hash == hashCode) {
            existingKeyEntry.value.removeExisting(entry);
            if(existingKeyEntry.value.size() == 0) {
                entries[index] = existingKeyEntry.next;
                mapSize--;
                growOrShrink();
            }
            return;
        }
        while(existingKeyEntry.next != null) {
            existingKeyEntry = existingKeyEntry.next;
            if(existingKeyEntry.hash == hashCode) {
                existingKeyEntry.value.removeExisting(entry);
                if(existingKeyEntry.value.size() == 0) {
                    entries[index] = existingKeyEntry.next;
                    mapSize--;
                    growOrShrink();
                }
                return;
            }
        }
    }

    @Override
    public boolean contains(T value) {
        final var hashCode = getHashCode(value);
        var index = hashCode & (entries.length-1);
        var existingEntry = (KeyEntry<T>)entries[index];
        if(existingEntry == null) {
           return false;
        }
        if(existingEntry.hash == hashCode) {
            return existingEntry.value.contains(value);
        }
        while(existingEntry.next != null) {
            existingEntry = existingEntry.next;
            if(existingEntry.hash == hashCode) {
                return existingEntry.value.contains(value);
            }
        }
        return false;
    }

    @Override
    public boolean contains(final MapEntry<T> entry) {
        final var hashCode = getHashCode(entry);
        var index = hashCode & (entries.length-1);
        var existingEntry = (KeyEntry<T>)entries[index];
        if(existingEntry == null) {
            return false;
        }
        if(existingEntry.hash == hashCode) {
            return existingEntry.value.contains(entry);
        }
        while(existingEntry.next != null) {
            existingEntry = existingEntry.next;
            if(existingEntry.hash == hashCode) {
                return existingEntry.value.contains(entry);
            }
        }
        return false;
    }

    @Override
    public boolean isEmpty() {
        return mapSize == 0;
    }

    @Override
    public void clear() {
        entries = new Object[MINIMUM_SIZE];
        mapSize = 0;
    }

    @Override
    public int numberOfKeys() {
        return mapSize;
    }

    @Override
    public int size() {
        return Arrays.stream(entries)
                .filter(entry -> entry != null)
                .map(entry -> (KeyEntry<T>) entry)
                .mapToInt(entry -> {
                    var size = 0;
                    while (entry != null) {
                        size += entry.value.size();
                        entry = entry.next;
                    }
                    return size;
                }).sum();
    }

    @Override
    public Stream<T> stream() {
        return StreamSupport.stream(new KeyEntriesSpliterator(entries), false);
    }

    @Override
    public Stream<T> stream(final T valueWithSameKey) {
        if(!containsPrimaryIndex(valueWithSameKey)) {
            return stream();
        }
        final var hashCode = getHashCode(valueWithSameKey);
        var index = hashCode & (entries.length-1);
        var existingEntry = (KeyEntry<T>)entries[index];
        while(existingEntry != null && existingEntry.hash != hashCode) {
            existingEntry = existingEntry.next;
        }
        if(existingEntry == null) {
            return Stream.empty();
        }
        if(containsSecondaryIndex(valueWithSameKey)) {
            return existingEntry.value.stream(valueWithSameKey);
        } else {
            return existingEntry.value.stream();
        }
    }

    @Override
    public Iterator<T> iterator() {
        if(this.isEmpty()) {
            return null;
        }
        return new KeyEntriesIterator<>(entries);
    }

    @Override
    public Iterator<T> iterator(final T valueWithSameKey) {
        if(!containsPrimaryIndex(valueWithSameKey)) {
            return iterator();
        }
        final var hashCode = getHashCode(valueWithSameKey);
        var index = hashCode & (entries.length-1);
        var existingEntry = (KeyEntry<T>)entries[index];
        while(existingEntry != null && existingEntry.hash != hashCode) {
            existingEntry = existingEntry.next;
        }
        if(existingEntry == null) {
            return null;
        }
        if(containsSecondaryIndex(valueWithSameKey)) {
            return existingEntry.value.iterator(valueWithSameKey);
        } else {
            return existingEntry.value.iterator();
        }
    }

    private static class KeyEntryIterator<T> implements Iterator<T> {

        private KeyEntry<T> keyEntry;
        private Iterator<T> currentIterator;
        private boolean hasNext = false;


        public KeyEntryIterator(KeyEntry<T> keyEntry) {
            this.keyEntry = keyEntry;
            this.currentIterator = keyEntry.value.iterator();
            if(currentIterator != null && currentIterator.hasNext()) {
                hasNext = true;
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
            if(!hasNext) {
                if(currentIterator != null && currentIterator.hasNext()) {
                    hasNext = true;
                    return true;
                }
                while(!hasNext && keyEntry != null) {
                    keyEntry = keyEntry.next;
                    if(keyEntry != null) {
                        currentIterator = keyEntry.value.iterator();
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

    private static class KeyEntriesIterator<T> implements Iterator<T> {

        private final Object[] entries;
        private Iterator<T> currentIterator;
        private int pos = 0;
        private boolean hasNext = false;

        public KeyEntriesIterator(Object[] entries) {
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
                    var keyEntry = (KeyEntry<T>)entries[pos++];
                    if(keyEntry != null) {
                        currentIterator = new KeyEntryIterator<>(keyEntry);
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
    private static class KeyEntriesSpliterator<T> implements Spliterator<T> {

        private Iterator<T> iterator;

        public KeyEntriesSpliterator(Object[] keyEntries) {
            this.iterator = new KeyEntriesIterator<>(keyEntries);
        }


        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            if(!iterator.hasNext()) {
                return false;
            }
            action.accept(iterator.next());
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

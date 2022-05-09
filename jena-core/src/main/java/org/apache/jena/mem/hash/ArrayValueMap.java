package org.apache.jena.mem.hash;

import org.apache.jena.graph.Triple;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import java.util.stream.Stream;

public abstract class ArrayValueMap<T> implements ValueMap<T> {

    private static int INITIAL_SIZE = 2;
    private static int GROW_BY_TRIPLES = 3;
    private int size = 0;
    private Object[] entries = new Object[INITIAL_SIZE];

    protected abstract int getHashIndex();
    protected abstract int getHashCode(T value);
    protected abstract MapEntry<T> createEntry(T value);
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

    public static Supplier<ValueMap<Triple>> forSubject = () -> new ArrayValueMap<Triple>() {
        @Override
        protected int getHashIndex() {
            return 0;
        }

        @Override
        protected int getHashCode(Triple value) {
            return value.getSubject().getIndexingValue().hashCode();
        }

        @Override
        protected MapEntry<Triple> createEntry(Triple value) {
            return MapEntry.fromTriple(value);
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

    public static Supplier<ValueMap<Triple>> forPredicate = () -> new ArrayValueMap<Triple>() {
        @Override
        protected int getHashIndex() {
            return 1;
        }

        @Override
        protected int getHashCode(Triple value) {
            return value.getPredicate().getIndexingValue().hashCode();
        }

        @Override
        protected MapEntry<Triple> createEntry(Triple value) {
            return MapEntry.fromTriple(value);
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

    public static Supplier<ValueMap<Triple>> forObject = () -> new ArrayValueMap<Triple>() {
        @Override
        protected int getHashIndex() {
            return 2;
        }

        @Override
        protected int getHashCode(Triple value) {
            return value.getObject().getIndexingValue().hashCode();
        }

        @Override
        protected MapEntry<Triple> createEntry(Triple value) {
            return MapEntry.fromTriple(value);
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

    protected ArrayValueMap() {
    }

    private void grow() {
        var newList = new Object[size + GROW_BY_TRIPLES];
        System.arraycopy(entries, 0, newList, 0, size);
        entries = newList;
    }

    @Override
    public MapEntry<T> addIfNotExists(T value) {
        var i=size;
        while (i > 0) {
            if(matches(value, ((MapEntry<T>) entries[--i]).value)) {
                return null;
            }
        }
        if(size == entries.length) {
            grow();
        }
        var entry = createEntry(value);
        entries[size++] = entry;
        return entry;
    }

    @Override
    public MapEntry<T> addDefinitetly(T value) {
        if(size == entries.length) {
            grow();
        }
        var entry = createEntry(value);
        entries[size++] = entry;
        return entry;
    }

    @Override
    public void addDefinitetly(MapEntry<T> entry) {
        if(size == entries.length) {
            grow();
        }
        entries[size++] = entry;
        return;
    }

    @Override
    public MapEntry<T> removeIfExits(T value) {
        var i=size;
        while (i > 0) {
            if(matches(value, ((MapEntry<T>) entries[--i]).value)) {
                var copyOfDeletedEntry = (MapEntry<T>) entries[i];
                entries[i] = entries[--size];
                return copyOfDeletedEntry;
            }
        }
        return null;
    }

    @Override
    public void removeExisting(MapEntry<T> entry) {
        var i=size;
        while (i > 0) {
            if(matches(entry, (MapEntry<T>) entries[--i])) {
                entries[i] = entries[--size];
                return;
            }
        }
    }

    @Override
    public boolean contains(T t) {
        var i=size;
        while (i > 0) {
            if(matches(t, ((MapEntry<T>)entries[--i]).value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean contains(final MapEntry<T> entry) {
        var i=size;
        while (i > 0) {
            if(matches(entry, ((MapEntry<T>)entries[--i]))) {
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
        return Arrays.stream(entries, 0, size).map(entry -> ((MapEntry<T>)entry).value);
    }

    @Override
    public Stream<T> stream(final T valueWithSameKey) {
        var hashCode = getHashCode(valueWithSameKey);
        var hashIndex = getHashIndex();
        return Arrays.stream(entries, 0, size)
                .map(entry -> ((MapEntry<T>)entry))
                .filter(entry -> entry.hashes[hashIndex] == hashCode)
                .map(entry -> entry.value);
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
        if(this.isEmpty()) {
            return null;
        }
        return new ArrayIteratorFiltering<>(entries, size, getHashIndex(), getHashCode(valueWithSameKey));
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
                return ((MapEntry<T>) entries[pos++]).value;
            }
            throw new NoSuchElementException();
        }
    }

    private static class ArrayIteratorFiltering<T> implements Iterator<T> {

        private final Object[] entries;
        private final int size;
        private final int hashIndex;
        private final int referenceHashCode;
        private int pos = 0;

        private boolean hasCurrent = false;
        private T current;

        private ArrayIteratorFiltering(final Object[] entries, final int size, final int hashIndex, final int referenceHashCode) {
            this.entries = entries;
            this.size = size;
            this.hashIndex = hashIndex;
            this.referenceHashCode = referenceHashCode;
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
            while(!hasCurrent && pos < size) {
                var candidate = ((MapEntry<T>)entries[pos++]);
                if(candidate.hashes[hashIndex] == referenceHashCode) {
                    this.current = candidate.value;
                    hasCurrent = true;
                }
            }
            return hasCurrent;
        }

        /**
         * Returns the next element in the iteration.
         *
         * @return the next element in the iteration
         * @throws NoSuchElementException if the iteration has no more elements
         */
        @Override
        public T next() {
            if (hasCurrent || hasNext())
            {
                hasCurrent = false;
                return current;
            }
            throw new NoSuchElementException();
        }
    }
}

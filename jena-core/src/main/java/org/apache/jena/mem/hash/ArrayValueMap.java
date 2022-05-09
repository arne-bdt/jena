package org.apache.jena.mem.hash;

import org.apache.jena.graph.Triple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;
import java.util.stream.Stream;

public abstract class ArrayValueMap<T> implements ValueMap<T> {

    private static int INITIAL_SIZE = 5;
    private static int GROW_BY_TRIPLES = 4;
    private int size = 0;
    private Object[] entries = new Object[INITIAL_SIZE];

    protected abstract MapEntry<T> createEntry(T value);
    protected abstract boolean matches(final T value1, final T value2);
    protected abstract boolean matches(final MapEntry<T> entry1, final MapEntry<T> entry2);

    private boolean compareHashes(int[] hashes1, int[] hashes2) {
        for(int i=0; i<hashes1.length; i++) {
            if(hashes1[i] != hashes2[i]) {
                return false;
            }
        }
        return true;
    }

    public ArrayValueMap forSubject = new ArrayValueMap<Triple>() {
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

    public ArrayValueMap forPredicate = new ArrayValueMap<Triple>() {
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

    public ArrayValueMap forObject = new ArrayValueMap<Triple>() {
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
        var newList = new Triple[size + GROW_BY_TRIPLES];
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
        entries[size++] = createEntry(value);
        return null;
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
        return this.stream();
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
        return new ArrayIterator(entries, size);
    }

    private static class ArrayIterator<T> implements Iterator<T> {

        private final Object[] entries;
        private final int size;
        private int pos = 0;

        private ArrayIterator(Object[] entries) {
            this(entries, entries.length);
        }

        private ArrayIterator(Object[] entries, int size) {
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
            return ((MapEntry<T>)entries[pos++]).value;
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

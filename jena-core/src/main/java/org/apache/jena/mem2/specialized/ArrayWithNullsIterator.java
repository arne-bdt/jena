package org.apache.jena.mem2.specialized;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class ArrayWithNullsIterator<E> implements Iterator<E> {

    private final E[] entries;
    private int remaining;
    private int pos = -1;

    ArrayWithNullsIterator(final E[] entries, final int size) {
        this.entries = entries;
        this.remaining = size;
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
        return 0 < remaining;
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    @Override
    public E next() {
        if (0 < remaining--) {
            while (entries[++pos] == null) ;
            return entries[pos];
        }
        throw new NoSuchElementException();
    }
}

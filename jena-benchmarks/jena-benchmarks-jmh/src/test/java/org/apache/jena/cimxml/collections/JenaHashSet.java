package org.apache.jena.cimxml.collections;

import org.apache.jena.mem2.collection.FastHashSet;

public class JenaHashSet<E> extends FastHashSet<E> {

    public JenaHashSet(int initialSize) {
        super(initialSize);
    }

    public JenaHashSet() {
        super();
    }

    public E getMatchingKey(E key) {
        final var pIndex = findPosition(key, key.hashCode());
        if (pIndex < 0) {
            return null;
        } else {
            return keys[positions[pIndex]];
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected E[] newKeysArray(int size) {
        return (E[]) new Object[size];
    }

}

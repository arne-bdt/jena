package org.apache.jena.cimxml.collections;

import org.apache.jena.mem2.collection.FastHashSet;

public class JenaHashSet<E> extends FastHashSet<E> {

    public JenaHashSet(int initialSize) {
        super(initialSize);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected E[] newKeysArray(int size) {
        return (E[]) new Object[size];
    }

}

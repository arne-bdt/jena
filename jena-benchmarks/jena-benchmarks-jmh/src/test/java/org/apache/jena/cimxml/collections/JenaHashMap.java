package org.apache.jena.cimxml.collections;

import org.apache.jena.mem2.collection.FastHashMap;

public class JenaHashMap<K, V> extends FastHashMap<K, V> {

    public JenaHashMap(int initialSize) {
        super(initialSize);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected K[] newKeysArray(int size) {
        return (K[]) new Object[size];
    }

    @Override
    @SuppressWarnings("unchecked")
    protected V[] newValuesArray(int size) {
        return (V[]) new Object[size];
    }
}

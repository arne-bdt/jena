package org.apache.jena.mem2.store.legacy.collection;

import org.apache.jena.graph.Triple;

public class HashCommonTripleSet extends HashCommonSet<Triple> {
    public HashCommonTripleSet() {
        super(10);
    }

    public HashCommonTripleSet(int initialCapacity) {
        super(initialCapacity);
    }

    @Override
    public void clear() {
        super.clear(10);
    }

    @Override
    protected Triple[] newKeysArray(int size) {
        return new Triple[size];
    }
}

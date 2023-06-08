package org.apache.jena.mem.set.triple;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.collection.HashCommonSet;

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

package org.apache.jena.mem2.collection;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.store.legacy.collection.HashCommonSet;

public class TripleSet extends HashCommonSet<Triple> {

    /**
     * Initialise this hashed thingy to have <code>initialCapacity</code> as its
     * capacity and the corresponding threshold. All the key elements start out
     * null.
     *
     * @param initialCapacity
     */
    public TripleSet(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * Initialise this hashed thingy to have <code>10</code> as its
     * capacity and the corresponding threshold. All the key elements start out
     * null.
     */
    public TripleSet() {
        this(10);
    }

    @Override
    protected Triple[] newKeysArray(int size) {
        return new Triple[size];
    }

    @Override
    public void clear() {
        super.clear(10);
    }
}

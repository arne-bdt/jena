package org.apache.jena.mem2.collection.discarded;

import org.apache.jena.graph.Triple;

public class FastTripleSet extends AbstractFastHashedSet<Triple> {

    /**
     * Initialise this hashed thingy to have <code>initialCapacity</code> as its
     * capacity and the corresponding threshold. All the key elements start out
     * null.
     *
     * @param initialCapacity
     */
    public FastTripleSet(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * Initialise this hashed thingy to have <code>10</code> as its
     * capacity and the corresponding threshold. All the key elements start out
     * null.
     */
    public FastTripleSet() {
        this(10);
    }

    @Override
    protected Triple[] newKeysArray(int size) {
        return new Triple[size];
    }
}

package org.apache.jena.mem2.collection;

import org.apache.jena.graph.Triple;

public class TripleSet extends AbstractHashedSet<Triple> {

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
    protected Triple[] newKeyArray(int size) {
        return new Triple[size];
    }
}

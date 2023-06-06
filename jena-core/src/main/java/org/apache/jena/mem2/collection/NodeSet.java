package org.apache.jena.mem2.collection;

import org.apache.jena.graph.Node;

public class NodeSet extends AbstractHashedSet<Node> {

    /**
     * Initialise this hashed thingy to have <code>initialCapacity</code> as its
     * capacity and the corresponding threshold. All the key elements start out
     * null.
     *
     * @param initialCapacity
     */
    public NodeSet(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * Initialise this hashed thingy to have <code>10</code> as its
     * capacity and the corresponding threshold. All the key elements start out
     * null.
     */
    public NodeSet() {
        this(10);
    }

    @Override
    protected Node[] newKeyArray(int size) {
        return new Node[size];
    }
}

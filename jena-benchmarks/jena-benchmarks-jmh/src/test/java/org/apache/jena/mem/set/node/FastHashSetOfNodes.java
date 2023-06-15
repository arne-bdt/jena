package org.apache.jena.mem.set.node;

import org.apache.jena.graph.Node;
import org.apache.jena.mem2.collection.FastHashSet;

public class FastHashSetOfNodes extends FastHashSet<Node> {

    public FastHashSetOfNodes() {
        super();
    }

    public FastHashSetOfNodes(int initialCapacity) {
        super(initialCapacity);
    }

    @Override
    protected Node[] newKeysArray(int size) {
        return new Node[size];
    }
}

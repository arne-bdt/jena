package org.apache.jena.mem.set.node;

import org.apache.jena.graph.Node;
import org.apache.jena.mem2.collection.FastHashSet;
import org.apache.jena.mem2.collection.HashCommonSet;

public class FastHashSetOfNodes extends FastHashSet<Node> {

    public FastHashSetOfNodes() {
        super();
    }

    @Override
    protected Node[] newKeysArray(int size) {
        return new Node[size];
    }

    public FastHashSetOfNodes(int initialCapacity) {
        super(initialCapacity);
    }
}

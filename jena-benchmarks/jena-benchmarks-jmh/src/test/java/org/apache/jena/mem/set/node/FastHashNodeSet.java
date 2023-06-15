package org.apache.jena.mem.set.node;

import org.apache.jena.graph.Node;
import org.apache.jena.mem2.collection.FastHashSet;

public class FastHashNodeSet extends FastHashSet<Node> {

    public FastHashNodeSet() {
        super();
    }

    public FastHashNodeSet(int initialCapacity) {
        super(initialCapacity);
    }

    @Override
    protected Node[] newKeysArray(int size) {
        return new Node[size];
    }
}

package org.apache.jena.mem.set.node;

import org.apache.jena.graph.Node;
import org.apache.jena.mem2.collection.FastHashSet;

public class FastHashSetOfNodes2 extends FastHashSet<Node> {

    public FastHashSetOfNodes2() {
        super();
    }

    public FastHashSetOfNodes2(int initialCapacity) {
        super(initialCapacity);
    }

    @Override
    protected Node[] newKeysArray(int size) {
        return new Node[size];
    }
}

package org.apache.jena.mem.set.triple;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.collection.FastHashSet;

public class FastHashSetOfTriples2 extends FastHashSet<Triple> {

    public FastHashSetOfTriples2() {
        super();
    }

    public FastHashSetOfTriples2(int initialCapacity) {
        super(initialCapacity);
    }


    @Override
    protected Triple[] newKeysArray(int size) {
        return new Triple[size];
    }
}

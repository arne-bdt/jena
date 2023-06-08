package org.apache.jena.mem.set.triple;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.collection.FastHashSet;

public class FastHashSetOfTriples extends FastHashSet<Triple> {

    public FastHashSetOfTriples() {
        super();
    }

    public FastHashSetOfTriples(int initialCapacity) {
        super(initialCapacity);
    }


    @Override
    protected Triple[] newKeysArray(int size) {
        return new Triple[size];
    }
}

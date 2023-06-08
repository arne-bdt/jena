package org.apache.jena.mem2.store.fast;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.collection.FastHashSet;
import org.apache.jena.mem2.collection.JenaSet;

public class FastHashedTripleBunch extends FastHashSet<Triple> implements FastTripleBunch {
    protected FastHashedTripleBunch(final JenaSet<Triple> b) {
        super(b.size());
        b.keyIterator().forEachRemaining(t -> this.addUnchecked(t));
    }

    @Override
    protected Triple[] newKeysArray(int size) {
        return new Triple[size];
    }

    @Override
    public boolean isHashed() {
        return true;
    }
}

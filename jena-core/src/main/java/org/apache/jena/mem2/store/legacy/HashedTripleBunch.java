package org.apache.jena.mem2.store.legacy;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.collection.HashCommonSet;
import org.apache.jena.mem2.collection.JenaSet;

public class HashedTripleBunch extends HashCommonSet<Triple> implements TripleBunch {
    protected HashedTripleBunch(final JenaSet<Triple> b) {
        super(nextSize((int) (b.size() / loadFactor)));
        b.keyIterator().forEachRemaining(t -> this.addUnchecked(t));
    }

    @Override
    protected Triple[] newKeysArray(int size) {
        return new Triple[size];
    }

    @Override
    public void clear() {
        super.clear(10);
    }

    @Override
    public boolean isHashed() {
        return true;
    }
}

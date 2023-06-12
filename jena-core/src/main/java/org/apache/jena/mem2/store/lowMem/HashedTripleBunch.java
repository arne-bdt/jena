package org.apache.jena.mem2.store.lowMem;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.collection.HashCommonSet;

public class HashedTripleBunch extends HashCommonSet<Triple> implements TripleBunch {

    private final Node indexingNode;

    protected HashedTripleBunch(final TripleBunch b) {
        super(nextSize((int) (b.size() / loadFactor)));
        b.keyIterator().forEachRemaining(t -> this.addUnchecked(t));
        this.indexingNode = b.getIndexingNode();
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
    public Node getIndexingNode() {
        return indexingNode;
    }

    @Override
    public boolean isHashed() {
        return true;
    }
}

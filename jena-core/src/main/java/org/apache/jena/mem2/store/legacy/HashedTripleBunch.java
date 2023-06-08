package org.apache.jena.mem2.store.legacy;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.collection.HashCommonSet;
import org.apache.jena.mem2.collection.JenaSet;

import java.util.function.Predicate;

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

    @Override
    public boolean containsWithOptimizedEqualsReplacement(Triple t, Predicate<Triple> predicateReplacingEquals) {
        final int hash = t.hashCode();
        int index = initialIndexFor(hash);
        while (true) {
            final Triple current = keys[index];
            if (current == null) return false;
            if (predicateReplacingEquals.test(current)) return true;
            if (--index < 0) index += keys.length;
        }
    }
}

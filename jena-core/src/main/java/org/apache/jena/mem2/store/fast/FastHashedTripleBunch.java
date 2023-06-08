package org.apache.jena.mem2.store.fast;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.collection.FastHashSet;
import org.apache.jena.mem2.collection.JenaSet;

import java.util.function.Predicate;

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

    @Override
    public boolean containsWithOptimizedEqualsReplacement(Triple t, Predicate<Triple> predicateReplacingEquals) {
        final int hashCode = t.hashCode();
        var pIndex = calcStartIndexByHashCode(hashCode);
        while (true) {
            if (0 == positions[pIndex]) {
                return false;
            } else {
                final var eIndex = ~positions[pIndex];
                if (hashCode == hashCodesOrDeletedIndices[eIndex] && predicateReplacingEquals.test(keys[eIndex])) {
                    return true;
                } else if (--pIndex < 0) {
                    pIndex += positions.length;
                }
            }
        }
    }
}

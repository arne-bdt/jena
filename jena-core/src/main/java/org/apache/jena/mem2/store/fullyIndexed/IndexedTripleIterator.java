package org.apache.jena.mem2.store.fullyIndexed;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.collection.FastHashSet;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.Iterator;
import java.util.function.Consumer;

public class IndexedTripleIterator extends NiceIterator<Triple> {
    private final Iterator<Integer> indexIterator;
    private final FastHashSet<Triple> triples;

    public IndexedTripleIterator(Iterator<Integer> indexIterator, FastHashSet<Triple> triples) {
        this.indexIterator = indexIterator;
        this.triples = triples;
    }

    @Override
    public boolean hasNext() {
        return this.indexIterator.hasNext();
    }

    @Override
    public Triple next() {
        return triples.getKeyAt(this.indexIterator.next());
    }

    @Override
    public void forEachRemaining(Consumer<? super Triple> action) {
        indexIterator.forEachRemaining(index
                -> action.accept(triples.getKeyAt(index)));
    }
}

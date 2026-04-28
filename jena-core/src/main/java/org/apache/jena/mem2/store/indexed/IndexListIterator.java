package org.apache.jena.mem2.store.indexed;

import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class IndexListIterator extends NiceIterator<Triple> {

    private final TripleSet triples;
    private final int sizeOfSetAtStart;
    private final int[] indices;
    private int pos;

    public IndexListIterator(final TripleSet triples, final IndexList indexList) {
        this.triples = triples;
        indices = indexList.getIndices();
        pos = indexList.getCurrentPosition();
        this.sizeOfSetAtStart = triples.size();
    }

    @Override
    public boolean hasNext() {
        return -1 < pos;
    }

    @Override
    public Triple next() {
        if (sizeOfSetAtStart != triples.size()) throw new ConcurrentModificationException();
        if(!hasNext()) {
            throw new NoSuchElementException();
        }
        return triples.getKeyAt(indices[pos--]);
    }

    @Override
    public void forEachRemaining(Consumer<? super Triple> action) {
        while (-1 < pos) {
            action.accept(triples.getKeyAt(indices[pos--]));
        }
        if (sizeOfSetAtStart != triples.size()) throw new ConcurrentModificationException();
    }
}

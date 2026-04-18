package org.apache.jena.mem2.store.roaring;

import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class IndexListIterator extends NiceIterator<Triple> {

    private final Triple[] triples;
    private final int[] indices;
    private final Runnable checkForConcurrentModification;
    private int pos;

    public IndexListIterator(final TripleSet tripleSet, final IndexList indexList, final Runnable checkForConcurrentModification) {
        triples = tripleSet.getTriples();
        indices = indexList.getIndices();
        pos = indexList.getCurrentPosition();
        this.checkForConcurrentModification = checkForConcurrentModification;
    }

    @Override
    public boolean hasNext() {
        return -1 < pos;
    }

    @Override
    public Triple next() {
        checkForConcurrentModification.run();
        if(!hasNext()) {
            throw new NoSuchElementException();
        }
        return triples[indices[pos--]];
    }

    @Override
    public void forEachRemaining(Consumer<? super Triple> action) {
        while (-1 < pos) {
            action.accept(triples[indices[pos--]]);
        }
        checkForConcurrentModification.run();
    }
}

package org.apache.jena.mem2.store.roaring;

import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.function.Consumer;

public class IndexListIterator extends NiceIterator<Triple> {

    private final Triple[] triples;
    private final int[] indices;
    private int pos;

    public IndexListIterator(final TripleSet tripleSet, final IndexList indexList) {
        triples = tripleSet.getTriples();
        indices = indexList.getIndices();
        pos = indexList.getCurrentPosition();
    }

    @Override
    public boolean hasNext() {
        return -1 < pos;
    }

    @Override
    public Triple next() {
        return triples[indices[pos--]];
    }

    @Override
    public void forEachRemaining(Consumer<? super Triple> action) {
        while (-1 < pos) {
            action.accept(triples[indices[pos--]]);
        }
    }
}

package org.apache.jena.mem2.store.roaring2;

import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class IndexListsIterator extends NiceIterator<Triple> {

    private final TripleSet triples;
    private final int sizeOfSetAtStart;
    private final int[] indicesSmaller;
    private final int[] indicesLarger;
    private final int[] reverseIndicesLarger;
    private int pos;
    private int tripleIndex;
    final int indicesLargerSize;
    private boolean hasNext = false;

    public IndexListsIterator(final TripleSet triples,
                              final IndexList indexListA, final int[] reverseIndicesA,
                              final IndexList indexListB, final int[] reverseIndicesB) {
        this.triples = triples;
        this.sizeOfSetAtStart = triples.size();
        if(indexListA.size() < indexListB.size()) {
            indicesSmaller = indexListA.getIndices();
            indicesLarger = indexListB.getIndices();
            reverseIndicesLarger = reverseIndicesB;
            pos = indexListA.lastPos();
            indicesLargerSize = indexListB.size();
        } else {
            indicesSmaller = indexListB.getIndices();
            indicesLarger = indexListA.getIndices();
            reverseIndicesLarger = reverseIndicesA;
            pos = indexListB.lastPos();
            indicesLargerSize = indexListA.size();
        }
    }

    @Override
    public boolean hasNext() {
        if(hasNext)
            return true;

        while(-1 < pos)  {
            tripleIndex = indicesSmaller[pos--];
            final var posLarger = reverseIndicesLarger[tripleIndex];

            if(posLarger < indicesLargerSize
                    && indicesLarger[posLarger] == tripleIndex) {
                return hasNext = true;
            }
        }
        return false;
    }

    @Override
    public Triple next() {
        if (sizeOfSetAtStart != triples.size()) throw new ConcurrentModificationException();
        if(hasNext || hasNext()) {
            hasNext = false;
            return triples.getKeyAt(tripleIndex);
        }
        throw new NoSuchElementException();
    }

    @Override
    public void forEachRemaining(Consumer<? super Triple> action) {
        while (-1 < pos) {
            tripleIndex = indicesSmaller[pos--];
            final var posLarger = reverseIndicesLarger[tripleIndex];
            if(posLarger < indicesLargerSize
                    && indicesLarger[posLarger] == tripleIndex) {
                action.accept(triples.getKeyAt(tripleIndex));
            }
        }
        if (sizeOfSetAtStart != triples.size()) throw new ConcurrentModificationException();
    }
}

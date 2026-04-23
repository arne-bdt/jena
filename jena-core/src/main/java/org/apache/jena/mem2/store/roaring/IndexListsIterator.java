package org.apache.jena.mem2.store.roaring;

import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class IndexListsIterator extends NiceIterator<Triple> {

    private final Triple[] triples;
    private final int[] indicesSmaller;
    private final int[] indicesLarger;
    private final int[] positionsLarger;
    private final Runnable checkForConcurrentModification;
    private int pos;
    private int tripleIndex;
    final int indicesLargerSize;
    private boolean hasNext = false;

    public IndexListsIterator(final TripleSet tripleSet,
                              final IndexList indexListA, final int[] spoIndexA,
                              final IndexList indexListB, final int[] spoIndexB,
                              final Runnable checkForConcurrentModification) {
        triples = tripleSet.getTriples();
        if(indexListA.size() < indexListB.size()) {
            indicesSmaller = indexListA.getIndices();
            indicesLarger = indexListB.getIndices();
            positionsLarger = spoIndexB;
            pos = indexListA.lastPos();
            indicesLargerSize = indexListB.size();
        } else {
            indicesSmaller = indexListB.getIndices();
            indicesLarger = indexListA.getIndices();
            positionsLarger = spoIndexA;
            pos = indexListB.lastPos();
            indicesLargerSize = indexListA.size();
        }
        this.checkForConcurrentModification = checkForConcurrentModification;
    }

    @Override
    public boolean hasNext() {
        if(hasNext) {
            return true;
        }
        while(-1 < pos)  {
            tripleIndex = indicesSmaller[pos--];
            final var posLarger = positionsLarger[tripleIndex];

            if(posLarger < indicesLargerSize
                    && indicesLarger[posLarger] == tripleIndex) {
                return hasNext = true;
            }
        }
        return hasNext = false;
    }

    @Override
    public Triple next() {
        checkForConcurrentModification.run();
        if(hasNext || hasNext()) {
            hasNext = false;
            return triples[tripleIndex];
        }
        throw new NoSuchElementException();
    }

    @Override
    public void forEachRemaining(Consumer<? super Triple> action) {
        while (-1 < pos) {
            tripleIndex = indicesSmaller[pos--];
            final var posLarger = positionsLarger[tripleIndex];
            if(posLarger < indicesLargerSize
                    && indicesLarger[posLarger] == tripleIndex) {
                action.accept(triples[tripleIndex]);
            }
        }
        checkForConcurrentModification.run();
    }
}

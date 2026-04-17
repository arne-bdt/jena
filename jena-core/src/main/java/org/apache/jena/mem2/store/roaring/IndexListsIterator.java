package org.apache.jena.mem2.store.roaring;

import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.function.Consumer;

public class IndexListsIterator extends NiceIterator<Triple> {

    private final Triple[] triples;
    private final int[] indicesSmaller;
    private final int[] indicesLarger;
    private final int[] positionsLarger;
    private int pos;
    private int tripleIndex;
    final int indicesLargerSize;

    public IndexListsIterator(final TripleSet tripleSet, final IndexList indexListA, final int spoIndexA, final IndexList indexListB, final int spoIndexB) {
        triples = tripleSet.getTriples();
        if(indexListA.size() < indexListB.size()) {
            indicesSmaller = indexListA.getIndices();
            indicesLarger = indexListB.getIndices();
            positionsLarger = tripleSet.getListPositions(spoIndexB);
            pos = indexListA.lastPos();
            indicesLargerSize = indexListB.size();
        } else {
            indicesSmaller = indexListB.getIndices();
            indicesLarger = indexListA.getIndices();
            positionsLarger = tripleSet.getListPositions(spoIndexA);
            pos = indexListB.lastPos();
            indicesLargerSize = indexListA.size();
        }
    }

    @Override
    public boolean hasNext() {
        while(-1 < pos)  {
            tripleIndex = indicesSmaller[pos--];
            final var posLarger = positionsLarger[tripleIndex];

            if(posLarger < indicesLargerSize
                    && indicesLarger[posLarger] == tripleIndex) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Triple next() {
        return triples[tripleIndex];
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
    }
}

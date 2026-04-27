package org.apache.jena.mem2.store.roaring2;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.store.roaring.BlockSet;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class IndexListsIterator extends NiceIterator<Triple> {

    private final BlockSet triples;
    private final int[] indicesSmaller;
    private final int[] indicesLarger;
    private final IndexList.IndexLookup spoIndexLarger;
    private final Runnable checkForConcurrentModification;
    private int pos;
    private int tripleIndex;
    final int indicesLargerSize;
    private boolean hasNext = false;

    public IndexListsIterator(final BlockSet blockSet,
                              final IndexList indexListA, final IndexList.IndexLookup spoIndexA,
                              final IndexList indexListB, final IndexList.IndexLookup spoIndexB,
                              final Runnable checkForConcurrentModification) {
        triples = blockSet;
        if(indexListA.size() < indexListB.size()) {
            indicesSmaller = indexListA.getIndices();
            indicesLarger = indexListB.getIndices();
            spoIndexLarger = spoIndexB;
            pos = indexListA.lastPos();
            indicesLargerSize = indexListB.size();
        } else {
            indicesSmaller = indexListB.getIndices();
            indicesLarger = indexListA.getIndices();
            spoIndexLarger = spoIndexA;
            pos = indexListB.lastPos();
            indicesLargerSize = indexListA.size();
        }
        this.checkForConcurrentModification = checkForConcurrentModification;
    }

    @Override
    public boolean hasNext() {
        if(hasNext)
            return true;

        while(-1 < pos)  {
            tripleIndex = indicesSmaller[pos--];
            final var posLarger = spoIndexLarger.get(tripleIndex);

            if(posLarger < indicesLargerSize
                    && indicesLarger[posLarger] == tripleIndex) {
                return hasNext = true;
            }
        }
        return false;
    }

    @Override
    public Triple next() {
        checkForConcurrentModification.run();
        if(hasNext || hasNext()) {
            hasNext = false;
            return triples.getTriple(tripleIndex);
        }
        throw new NoSuchElementException();
    }

    @Override
    public void forEachRemaining(Consumer<? super Triple> action) {
        while (-1 < pos) {
            tripleIndex = indicesSmaller[pos--];
            final var posLarger = spoIndexLarger.get(tripleIndex);
            if(posLarger < indicesLargerSize
                    && indicesLarger[posLarger] == tripleIndex) {
                action.accept(triples.getTriple(tripleIndex));
            }
        }
        checkForConcurrentModification.run();
    }
}

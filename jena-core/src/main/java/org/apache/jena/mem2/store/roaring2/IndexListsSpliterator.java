package org.apache.jena.mem2.store.roaring2;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.store.roaring.BlockSet;

import java.util.Spliterator;
import java.util.function.Consumer;

public class IndexListsSpliterator implements Spliterator<Triple> {

    private final BlockSet triples;
    private final int[] indicesSmaller;
    private final int[] indicesLarger;
    private final IndexList.IndexLookup spoIndexLarger;
    private final Runnable checkForConcurrentModification;
    private final int toPositionExclusive;
    private int pos;
    final int indicesLargerSize;

    public IndexListsSpliterator(final BlockSet blockSet,
                                 final IndexList indexListA, final IndexList.IndexLookup spoIndexA,
                                 final IndexList indexListB, final IndexList.IndexLookup spoIndexB,
                                 final Runnable checkForConcurrentModification) {
        triples = blockSet;
        if(indexListA.size() < indexListB.size()) {
            indicesSmaller = indexListA.getIndices();
            indicesLarger = indexListB.getIndices();
            spoIndexLarger = spoIndexB;
            toPositionExclusive = indexListA.size();
            pos = 0;
            indicesLargerSize = indexListB.size();
        } else {
            indicesSmaller = indexListB.getIndices();
            indicesLarger = indexListA.getIndices();
            spoIndexLarger = spoIndexA;
            toPositionExclusive = indexListB.size();
            pos = 0;
            indicesLargerSize = indexListA.size();
        }
        this.checkForConcurrentModification = checkForConcurrentModification;
    }

    public IndexListsSpliterator(final BlockSet triples,
                                 final int[] indicesSmaller,
                                 final int[] indicesLarger, final int indicesLargerSize,
                                 final IndexList.IndexLookup spoIndexLarger,
                                 final int from, final int toExclusive,
                                 final Runnable checkForConcurrentModification) {
        this.triples = triples;
        this.indicesSmaller = indicesSmaller;
        this.indicesLarger = indicesLarger;
        this.spoIndexLarger = spoIndexLarger;
        this.pos = from;
        this.toPositionExclusive = toExclusive;
        this.checkForConcurrentModification = checkForConcurrentModification;
        this.indicesLargerSize = indicesLargerSize;
    }


    @Override
    public boolean tryAdvance(Consumer<? super Triple> action) {
        checkForConcurrentModification.run();
        while (pos < toPositionExclusive) {
            final var tripleIndex = indicesSmaller[pos++];
            final var posLarger = spoIndexLarger.get(tripleIndex);
            if(posLarger < indicesLargerSize
                    && indicesLarger[posLarger] == tripleIndex) {
                action.accept(triples.getTriple(tripleIndex));
                return true;
            }
        }
        return false;
    }

    @Override
    public void forEachRemaining(Consumer<? super Triple> action) {
        while (pos < toPositionExclusive) {
            final var tripleIndex = indicesSmaller[pos++];
            final var posLarger = spoIndexLarger.get(tripleIndex);
            if(posLarger < indicesLargerSize
                    && indicesLarger[posLarger] == tripleIndex) {
                action.accept(triples.getTriple(tripleIndex));
            }
        }
        checkForConcurrentModification.run();
    }

    @Override
    public Spliterator<Triple> trySplit() {
        final var remaining = toPositionExclusive - pos;
        if (remaining < 2) {
            return null;
        }
        final var oldPos = pos;
        this.pos = pos + (remaining >>> 1);
        return new IndexListsSpliterator(triples,
                indicesSmaller, indicesLarger, indicesLargerSize,
                spoIndexLarger,
                oldPos, this.pos,
                checkForConcurrentModification);
    }

    @Override
    public long estimateSize() {
        return toPositionExclusive - pos;
    }

    @Override
    public long getExactSizeIfKnown() {
        return -1;
    }

    @Override
    public int characteristics() {
        return DISTINCT | NONNULL | IMMUTABLE;
    }
}

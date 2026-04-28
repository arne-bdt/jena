package org.apache.jena.mem2.store.roaring2;

import org.apache.jena.graph.Triple;

import java.util.ConcurrentModificationException;
import java.util.Spliterator;
import java.util.function.Consumer;

public class IndexListsSpliterator implements Spliterator<Triple> {

    private final TripleSet triples;
    private final int sizeOfSetAtStart;
    private final int[] indicesSmaller;
    private final int[] indicesLarger;
    private final int[] reverseIndicesLarger;
    private final int toPositionExclusive;
    private int pos;
    final int indicesLargerSize;

    public IndexListsSpliterator(final TripleSet triples,
                                 final IndexList indexListA, final int[] reverseIndicesA,
                                 final IndexList indexListB, final int[] reverseIndicesB) {
        this.triples = triples;
        this.sizeOfSetAtStart = triples.size();
        if(indexListA.size() < indexListB.size()) {
            indicesSmaller = indexListA.getIndices();
            indicesLarger = indexListB.getIndices();
            reverseIndicesLarger = reverseIndicesB;
            toPositionExclusive = indexListA.size();
            pos = 0;
            indicesLargerSize = indexListB.size();
        } else {
            indicesSmaller = indexListB.getIndices();
            indicesLarger = indexListA.getIndices();
            reverseIndicesLarger = reverseIndicesA;
            toPositionExclusive = indexListB.size();
            pos = 0;
            indicesLargerSize = indexListA.size();
        }
    }

    private IndexListsSpliterator(final TripleSet triples,
                                  final int sizeOfSetAtStart,
                                  final int[] indicesSmaller,
                                  final int[] indicesLarger, final int indicesLargerSize,
                                  final int[] reverseIndicesLarger,
                                  final int from, final int toExclusive) {
        this.triples = triples;
        this.sizeOfSetAtStart = sizeOfSetAtStart;
        this.indicesSmaller = indicesSmaller;
        this.indicesLarger = indicesLarger;
        this.reverseIndicesLarger = reverseIndicesLarger;
        this.pos = from;
        this.toPositionExclusive = toExclusive;
        this.indicesLargerSize = indicesLargerSize;
    }


    @Override
    public boolean tryAdvance(Consumer<? super Triple> action) {
        if (sizeOfSetAtStart != triples.size()) throw new ConcurrentModificationException();
        while (pos < toPositionExclusive) {
            final var tripleIndex = indicesSmaller[pos++];
            final var posLarger = reverseIndicesLarger[tripleIndex];
            if(posLarger < indicesLargerSize
                    && indicesLarger[posLarger] == tripleIndex) {
                action.accept(triples.getKeyAt(tripleIndex));
                return true;
            }
        }
        return false;
    }

    @Override
    public void forEachRemaining(Consumer<? super Triple> action) {
        while (pos < toPositionExclusive) {
            final var tripleIndex = indicesSmaller[pos++];
            final var posLarger = reverseIndicesLarger[tripleIndex];
            if(posLarger < indicesLargerSize
                    && indicesLarger[posLarger] == tripleIndex) {
                action.accept(triples.getKeyAt(tripleIndex));
            }
        }
        if (sizeOfSetAtStart != triples.size()) throw new ConcurrentModificationException();
    }

    @Override
    public Spliterator<Triple> trySplit() {
        final var remaining = toPositionExclusive - pos;
        if (remaining < 2) {
            return null;
        }
        final var oldPos = pos;
        this.pos = pos + (remaining >>> 1);
        return new IndexListsSpliterator(triples, sizeOfSetAtStart,
                indicesSmaller, indicesLarger, indicesLargerSize,
                reverseIndicesLarger,
                oldPos, this.pos);
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

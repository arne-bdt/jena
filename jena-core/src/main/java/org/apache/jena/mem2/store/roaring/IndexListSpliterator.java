package org.apache.jena.mem2.store.roaring;

import org.apache.jena.graph.Triple;

import java.util.Spliterator;
import java.util.function.Consumer;

public class IndexListSpliterator implements Spliterator<Triple> {

    private final BlockSet triples;
    private final int[] indices;
    private final Runnable checkForConcurrentModification;
    private final int toPositionExclusive;
    private int pos;

    public IndexListSpliterator(final BlockSet blockSet, final IndexList indexList, final Runnable checkForConcurrentModification) {
        this(blockSet,
                indexList.getIndices(),
                0, indexList.size(),
                checkForConcurrentModification);
    }

    public IndexListSpliterator(final BlockSet triples, final int[] indices, final int from, final int toExclusive, final Runnable checkForConcurrentModification) {
        this.triples = triples;
        this.indices = indices;
        this.pos = from;
        this.toPositionExclusive = toExclusive;
        this.checkForConcurrentModification = checkForConcurrentModification;
    }

    @Override
    public boolean tryAdvance(Consumer<? super Triple> action) {
        checkForConcurrentModification.run();
        if (pos < toPositionExclusive) {
            action.accept(triples.getTriple(indices[pos++]));
            return true;
        }
        return false;
    }

    @Override
    public void forEachRemaining(Consumer<? super Triple> action) {
        while (pos < toPositionExclusive) {
            action.accept(triples.getTriple(indices[pos++]));
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
        return new IndexListSpliterator(triples, indices,
                oldPos, this.pos,
                checkForConcurrentModification);
    }

    @Override
    public long estimateSize() {
        return toPositionExclusive - pos;
    }

    @Override
    public long getExactSizeIfKnown() {
        return toPositionExclusive - pos;
    }

    @Override
    public int characteristics() {
        return DISTINCT | SIZED | SUBSIZED | NONNULL | IMMUTABLE;
    }
}

package org.apache.jena.mem2.store.indexed;

import org.apache.jena.graph.Triple;

import java.util.ConcurrentModificationException;
import java.util.Spliterator;
import java.util.function.Consumer;

public class IndexListSpliterator implements Spliterator<Triple> {

    private final TripleSet triples;
    private final int sizeOfSetAtStart;
    private final int[] indices;
    private final int toPositionExclusive;
    private int pos;

    public IndexListSpliterator(final TripleSet triples, final IndexList indexList) {
        this(triples,
                indexList.getIndices(),
                0, indexList.size());
    }

    public IndexListSpliterator(final TripleSet triples, final int[] indices, final int from, final int toExclusive) {
        this.triples = triples;
        this.sizeOfSetAtStart = triples.size();
        this.indices = indices;
        this.pos = from;
        this.toPositionExclusive = toExclusive;
    }

    @Override
    public boolean tryAdvance(Consumer<? super Triple> action) {
        if (sizeOfSetAtStart != triples.size()) throw new ConcurrentModificationException();
        if (pos < toPositionExclusive) {
            action.accept(triples.getKeyAt(indices[pos++]));
            return true;
        }
        return false;
    }

    @Override
    public void forEachRemaining(Consumer<? super Triple> action) {
        while (pos < toPositionExclusive) {
            action.accept(triples.getKeyAt(indices[pos++]));
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
        return new IndexListSpliterator(triples, indices,
                oldPos, this.pos);
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

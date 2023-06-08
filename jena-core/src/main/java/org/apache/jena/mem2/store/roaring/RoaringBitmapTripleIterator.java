package org.apache.jena.mem2.store.roaring;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.collection.FastHashSet;
import org.apache.jena.util.iterator.NiceIterator;
import org.roaringbitmap.BatchIterator;

import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class RoaringBitmapTripleIterator extends NiceIterator<Triple> {
    private final BatchIterator iterator;
    private final FastHashSet<Triple> triples;
    private final int[] buffer = new int[64];
    private int bufferIndex = -1;

    public RoaringBitmapTripleIterator(BatchIterator iterator, FastHashSet<Triple> triples) {
        this.iterator = iterator;
        this.triples = triples;
    }

    @Override
    public boolean hasNext() {
        if (bufferIndex > 0)
            return true;
        return this.iterator.hasNext();
    }

    @Override
    public Triple next() {
        if (bufferIndex > 0)
            return triples.getKeyAt(buffer[--bufferIndex]);

        if (!iterator.hasNext()) {
            throw new NoSuchElementException();
        }
        bufferIndex = iterator.nextBatch(buffer);
        return triples.getKeyAt(buffer[--bufferIndex]);
    }

    @Override
    public void forEachRemaining(Consumer<? super Triple> action) {
        if (bufferIndex > 0) {
            for (int i = bufferIndex - 1; i >= 0; i--) {
                action.accept(triples.getKeyAt(buffer[i]));
            }
        }
        while (iterator.hasNext()) {
            bufferIndex = iterator.nextBatch(buffer);
            for (int i = bufferIndex - 1; i >= 0; i--) {
                action.accept(triples.getKeyAt(buffer[i]));
            }
        }
    }
}

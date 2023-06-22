/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jena.mem2.store.roaring;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.collection.FastHashSet;
import org.apache.jena.util.iterator.NiceIterator;
import org.roaringbitmap.BatchIterator;

import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class RoaringBitmapTripleIterator extends NiceIterator<Triple> {
    protected static final int BUFFER_SIZE = 64;
    private final BatchIterator iterator;
    private final FastHashSet<Triple> triples;
    private final int initialSize;

    private final int[] buffer = new int[BUFFER_SIZE];
    private int bufferIndex = -1;

    public RoaringBitmapTripleIterator(BatchIterator iterator, FastHashSet<Triple> triples) {
        this.iterator = iterator;
        this.triples = triples;
        this.initialSize = triples.size();
    }

    @Override
    public boolean hasNext() {
        if (bufferIndex > 0)
            return true;
        return this.iterator.hasNext();
    }

    @Override
    public Triple next() {
        if (triples.size() != initialSize) throw new ConcurrentModificationException();
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
        bufferIndex = 0;
        if (triples.size() != initialSize) throw new ConcurrentModificationException();
    }
}

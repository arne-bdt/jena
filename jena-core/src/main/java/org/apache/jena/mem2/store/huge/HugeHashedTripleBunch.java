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
package org.apache.jena.mem2.store.huge;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.collection.FastHashBase;
import org.apache.jena.mem2.collection.FastHashMap;
import org.apache.jena.mem2.collection.FastHashSet;
import org.apache.jena.mem2.collection.JenaSet;
import org.apache.jena.mem2.store.fast.FastHashedTripleBunch;
import org.apache.jena.mem2.store.roaring.RoaringBitmapTripleIterator;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NullIterator;
import org.roaringbitmap.RoaringBitmap;

import java.util.Spliterator;
import java.util.function.Predicate;
import java.util.stream.Stream;

public abstract class HugeHashedTripleBunch implements HudeTripleBunch {

    private final NodeBitmapsMap indexNodeBitmaps = new NodeBitmapsMap();
    private final FastHashedTripleBunch triples;

    protected HugeHashedTripleBunch(final JenaSet<Triple> b) {
        this.triples = new FastHashedTripleBunch(b.size());
        b.keyIterator().forEachRemaining(t -> this.addUnchecked(t));
    }

    protected abstract Node getIndexingNode(Triple t);

    @Override
    public void clear() {
        triples.clear();
        indexNodeBitmaps.clear();
    }

    @Override
    public int size() {
        return triples.size();
    }

    @Override
    public boolean isEmpty() {
        return triples.isEmpty();
    }

    @Override
    public boolean containsKey(Triple triple) {
        return triples.containsKey(triple);
    }

    @Override
    public boolean anyMatch(Predicate<Triple> predicate) {
        return triples.anyMatch(predicate);
    }

    @Override
    public boolean tryRemove(Triple triple) {
        return tryRemove(triple, triple.hashCode());
    }

    @Override
    public void removeUnchecked(Triple triple) {
        removeUnchecked(triple, triple.hashCode());
    }

    @Override
    public boolean tryAdd(Triple key) {
        return tryAdd(key, key.hashCode());
    }

    @Override
    public void addUnchecked(Triple key) {
        addUnchecked(key, key.hashCode());
    }

    @Override
    public ExtendedIterator<Triple> keyIterator() {
        return triples.keyIterator();
    }

    @Override
    public Spliterator<Triple> keySpliterator() {
        return triples.keySpliterator();
    }

    @Override
    public boolean tryAdd(Triple key, int hashCode) {
        final var index = triples.addAndGetIndex(key, hashCode);
        if(index < 0)
            return false;

        indexNodeBitmaps.computeIfAbsent(getIndexingNode(key),
                        () -> new RoaringBitmap())
                .add(index);
        return true;
    }

    @Override
    public void addUnchecked(Triple key, int hashCode) {
        indexNodeBitmaps.computeIfAbsent(getIndexingNode(key),
                        () -> new RoaringBitmap())
                .add(triples.addAndGetIndex(key, hashCode));
    }

    @Override
    public boolean tryRemove(Triple key, int hashCode) {
        final var index = triples.removeAndGetIndex(key, hashCode);
        if(index < 0)
            return false;
        final var indexNode = getIndexingNode(key);
        final var bitmap = indexNodeBitmaps.get(indexNode);
        bitmap.remove(index);
        if(bitmap.isEmpty()) {
            indexNodeBitmaps.removeUnchecked(indexNode);
        }
        return true;
    }

    @Override
    public void removeUnchecked(Triple key, int hashCode) {
        final var indexNode = getIndexingNode(key);
        final var bitmap = indexNodeBitmaps.get(indexNode);
        bitmap.remove(triples.removeAndGetIndex(key, hashCode));
        if(bitmap.isEmpty()) {
            indexNodeBitmaps.removeUnchecked(indexNode);
        }
    }

    @Override
    public boolean isHashed() {
        return true;
    }

    @Override
    public ExtendedIterator<Triple> keyIterator(Node node) {
        final var bitmap = indexNodeBitmaps.get(node);
        if(bitmap == null)
            return NullIterator.emptyIterator();
        return new RoaringBitmapTripleIterator(bitmap.getBatchIterator(), triples);
    }

    @Override
    public Stream<Triple> keyStream(Node node) {
        final var bitmap = indexNodeBitmaps.get(node);
        if(bitmap == null)
            return Stream.empty();
        return bitmap.stream().mapToObj(triples::getKeyAt);
    }

    @Override
    public boolean containsNode(Node node) {
        return indexNodeBitmaps.containsKey(node);
    }
}

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

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.store.legacy.collection.HashCommonMap;
import org.apache.jena.mem2.collection.FastTripleHashSet2;
import org.apache.jena.mem2.pattern.MatchPattern;
import org.apache.jena.mem2.pattern.PatternClassifier;
import org.apache.jena.mem2.store.TripleStore;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.ImmutableBitmapDataProvider;
import org.roaringbitmap.RoaringBitmap;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class RoaringTripleStore implements TripleStore {

    private class NodesToBitmapsMap extends HashCommonMap<Node, RoaringBitmap> {

        public NodesToBitmapsMap() {
            super(10);
        }

        @Override
        protected RoaringBitmap[] newValueArray(int size) {
            return new RoaringBitmap[size];
        }

        @Override
        protected Node[] newKeyArray(int size) {
            return new Node[size];
        }
    }

    NodesToBitmapsMap subjectBitmaps = new NodesToBitmapsMap();
    NodesToBitmapsMap predicateBitmaps = new NodesToBitmapsMap();
    NodesToBitmapsMap objectBitmaps = new NodesToBitmapsMap();
    FastTripleHashSet2 triples = new FastTripleHashSet2(); // We use a list here to maintain the order of triples

    public RoaringTripleStore() {
        this.clear();
    }


    @Override
    public void add(final Triple triple) {
        final var index = triples.addAndGetIndex(triple);
        if(index < 0) { /*triple already exists*/
            return;
        }

        this.subjectBitmaps.computeIfAbsent(triple.getSubject(), () -> new RoaringBitmap())
                .add(index);
        this.predicateBitmaps.computeIfAbsent(triple.getPredicate(), () -> new RoaringBitmap())
                .add(index);
        this.objectBitmaps.computeIfAbsent(triple.getObject(), () -> new RoaringBitmap())
                .add(index);
    }

    private void removeIndex(final NodesToBitmapsMap map, final Node node, final int index) {
        final var bitmap = map.get(node);
        bitmap.remove(index);
        if(bitmap.isEmpty()) {
            map.tryRemove(node);
        }
    }

    @Override
    public void remove(final Triple triple) {
        final var index = triples.removeAndGetIndex(triple);
        if(index < 0) { /*triple does not exist*/
            return;
        }
        removeIndex(this.subjectBitmaps, triple.getSubject(), index);
        removeIndex(this.predicateBitmaps, triple.getPredicate(), index);
        removeIndex(this.objectBitmaps, triple.getObject(), index);
    }

    @Override
    public void clear() {
        this.subjectBitmaps.clear(10);
        this.predicateBitmaps.clear(10);
        this.objectBitmaps.clear(10);
        this.triples.clear();
    }

    @Override
    public int countTriples() {
        return this.triples.size();
    }

    @Override
    public boolean isEmpty() {
        return this.triples.isEmpty();
    }

    @Override
    public boolean contains(Triple tripleMatch) {
        final var matchPattern = PatternClassifier.classify(tripleMatch);
        switch (matchPattern) {

            case S__:
                return this.subjectBitmaps.contains(tripleMatch.getSubject());
            case _P_:
                return this.predicateBitmaps.contains(tripleMatch.getPredicate());
            case __O:
                return this.objectBitmaps.contains(tripleMatch.getObject());

            case SP_:
            case _PO:
            case S_O:
            case SPO:
                return !getBitmapForMatch(tripleMatch, matchPattern).isEmpty();

            case ___:
                return !this.isEmpty();

            default:
                throw new IllegalStateException("Unknown pattern classifier: " + PatternClassifier.classify(tripleMatch));
        }
    }


    private static RoaringBitmap EMPTY_BITMAP = new RoaringBitmap();

    private ImmutableBitmapDataProvider getBitmapForMatch(final Triple tripleMatch, final MatchPattern matchPattern) {
        switch (matchPattern) {

            case S__:
                return this.subjectBitmaps.getOrDefault(tripleMatch.getSubject(), EMPTY_BITMAP);
            case _P_:
                return this.predicateBitmaps.getOrDefault(tripleMatch.getPredicate(), EMPTY_BITMAP);
            case __O:
                return this.objectBitmaps.getOrDefault(tripleMatch.getObject(), EMPTY_BITMAP);

            case SP_:
            {
                final var subjectBitmap = this.subjectBitmaps.get(tripleMatch.getSubject());
                if(null == subjectBitmap)
                    return EMPTY_BITMAP;

                final var predicateBitmap = this.predicateBitmaps.get(tripleMatch.getPredicate());
                if(null == predicateBitmap)
                    return EMPTY_BITMAP;

                return RoaringBitmap.and(subjectBitmap, predicateBitmap);
            }

            case _PO:
            {
                final var predicateBitmap = this.predicateBitmaps.get(tripleMatch.getPredicate());
                if(null == predicateBitmap)
                    return EMPTY_BITMAP;

                final var objectBitmap = this.objectBitmaps.get(tripleMatch.getObject());
                if(null == objectBitmap)
                    return EMPTY_BITMAP;

                return RoaringBitmap.and(objectBitmap, predicateBitmap);
            }

            case S_O:
            {
                final var subjectBitmap = this.subjectBitmaps.get(tripleMatch.getSubject());
                if(null == subjectBitmap)
                    return EMPTY_BITMAP;

                final var objectBitmap = this.objectBitmaps.get(tripleMatch.getObject());
                if(null == objectBitmap)
                    return EMPTY_BITMAP;

                return RoaringBitmap.and(subjectBitmap, objectBitmap);
            }

            case SPO:
            {
                final var subjectBitmap = this.subjectBitmaps.get(tripleMatch.getSubject());
                if (null == subjectBitmap)
                    return EMPTY_BITMAP;

                final var predicateBitmap = this.predicateBitmaps.get(tripleMatch.getPredicate());
                if (null == predicateBitmap)
                    return EMPTY_BITMAP;

                final var objectBitmap = this.objectBitmaps.get(tripleMatch.getObject());
                if(null == objectBitmap)
                    return EMPTY_BITMAP;

                return FastAggregation.and(subjectBitmap, objectBitmap, predicateBitmap);
            }

            case ___:
                throw new IllegalArgumentException("Cannot get bitmap for match pattern ___");

            default:
                throw new IllegalStateException("Unknown pattern classifier: " + PatternClassifier.classify(tripleMatch));
        }
    }

    @Override
    public Stream<Triple> stream() {
        return this.triples.stream();
    }

    @Override
    public Stream<Triple> stream(Triple tripleMatch) {
        var pattern = PatternClassifier.classify(tripleMatch);
        if(pattern == MatchPattern.___)
            return this.stream();

        var bitmap = this.getBitmapForMatch(tripleMatch, pattern);
        return bitmap.stream().mapToObj(this.triples::get);
    }

    @Override
    public ExtendedIterator<Triple> find(Triple tripleMatch) {
        var pattern = PatternClassifier.classify(tripleMatch);
        if(pattern == MatchPattern.___)
            return triples.iterator();

        var bitmap = this.getBitmapForMatch(tripleMatch, pattern);
        return new NiceIterator<>() {
            private final BatchIterator iterator = bitmap.getBatchIterator();
            private int[] buffer = new int[64];
            private int bufferIndex = -1;

            @Override
            public boolean hasNext() {
                if(bufferIndex > 0)
                    return true;
                return this.iterator.hasNext();
            }

            @Override
            public Triple next() {
                if(bufferIndex > 0)
                    return triples.get(buffer[--bufferIndex]);

                if(!iterator.hasNext()) {
                    throw new NoSuchElementException();
                }
                bufferIndex = iterator.nextBatch(buffer);
                return triples.get(buffer[--bufferIndex]);
            }

            @Override
            public void forEachRemaining(Consumer<? super Triple> action) {
                if(bufferIndex > 0) {
                    for(int i = bufferIndex - 1; i >= 0; i--) {
                        action.accept(triples.get(buffer[i]));
                    }
                }
                while (iterator.hasNext()) {
                    bufferIndex = iterator.nextBatch(buffer);
                    for(int i = bufferIndex - 1; i >= 0; i--) {
                        action.accept(triples.get(buffer[i]));
                    }
                }
            }
        };
    }
}

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
import org.apache.jena.mem2.pattern.MatchPattern;
import org.apache.jena.mem2.pattern.PatternClassifier;
import org.apache.jena.mem2.store.TripleStore;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.FilterIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.ImmutableBitmapDataProvider;
import org.roaringbitmap.RoaringBitmap;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class RoaringTripleStore implements TripleStore {

    Map<Node, RoaringBitmap> subjectBitmaps = new HashMap<>();
    Map<Node, RoaringBitmap> predicateBitmaps = new HashMap<>();
    Map<Node, RoaringBitmap> objectBitmaps = new HashMap<>();
    List<Triple> tripleList = new ArrayList<>(); // We use a list here to maintain the order of triples

    ArrayDeque<Integer> freeIndicesFromRemovedTriples = new ArrayDeque<>();

    public RoaringTripleStore() {
        this.clear();
    }


    @Override
    public void add(final Triple triple) {
        final var subjectBitmap = this.subjectBitmaps.computeIfAbsent(triple.getSubject(), n -> new RoaringBitmap());
        final var predicateBitmap = this.predicateBitmaps.computeIfAbsent(triple.getPredicate(), n -> new RoaringBitmap());
        final var objectBitmap = this.objectBitmaps.computeIfAbsent(triple.getObject(), n -> new RoaringBitmap());

        if(!FastAggregation.and(subjectBitmap, objectBitmap, predicateBitmap).isEmpty())
            return;

        final int index;
        if(freeIndicesFromRemovedTriples.isEmpty()) {
            tripleList.add(triple);
            index = tripleList.size() - 1;
        } else {
            index = freeIndicesFromRemovedTriples.pop();
            tripleList.set(index, triple);
        }

        subjectBitmap.add(index);
//        subjectBitmap.runOptimize();

        predicateBitmap.add(index);
//        predicateBitmap.runOptimize();

        objectBitmap.add(index);
//        objectBitmap.runOptimize();
    }

    @Override
    public void remove(final Triple triple) {
        final var subjectBitmap = this.subjectBitmaps.get(triple.getSubject());
        if(null == subjectBitmap)
            return;

        final var predicateBitmap = this.predicateBitmaps.get(triple.getPredicate());
        if(null == predicateBitmap)
            return;

        final var objectBitmap = this.objectBitmaps.get(triple.getObject());
        if(null == objectBitmap)
            return;

        final var bitmap = FastAggregation.and(subjectBitmap, objectBitmap, predicateBitmap);

        if(bitmap.isEmpty())
            return;

        final var index = bitmap.first();
        subjectBitmap.remove(index);
        predicateBitmap.remove(index);
        objectBitmap.remove(index);
        tripleList.set(index, null);

        if(subjectBitmap.isEmpty()) {
            subjectBitmaps.remove(triple.getSubject());
        }
        else {
            subjectBitmap.trim();
//            subjectBitmap.runOptimize();
        }

        if(predicateBitmap.isEmpty()) {
            predicateBitmaps.remove(triple.getPredicate());
        }
        else {
            predicateBitmap.trim();
//            predicateBitmap.runOptimize();
        }

        if(objectBitmap.isEmpty()) {
            objectBitmaps.remove(triple.getObject());
        }
        else {
            objectBitmap.trim();
//            objectBitmap.runOptimize();
        }
        freeIndicesFromRemovedTriples.push(index);
    }

    @Override
    public void clear() {
        this.subjectBitmaps.clear();
        this.predicateBitmaps.clear();
        this.objectBitmaps.clear();
        this.tripleList.clear();
        this.freeIndicesFromRemovedTriples.clear();
    }

    @Override
    public int countTriples() {
        return this.tripleList.size() - this.freeIndicesFromRemovedTriples.size();
    }

    @Override
    public boolean isEmpty() {
        return this.countTriples() == 0;
    }

    @Override
    public boolean contains(Triple tripleMatch) {
        final var matchPattern = PatternClassifier.classify(tripleMatch);
        switch (matchPattern) {

            case S__:
                return this.subjectBitmaps.containsKey(tripleMatch.getSubject());
            case _P_:
                return this.predicateBitmaps.containsKey(tripleMatch.getPredicate());
            case __O:
                return this.objectBitmaps.containsKey(tripleMatch.getObject());

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
        return this.tripleList.stream().filter(Objects::nonNull);
    }

    @Override
    public Stream<Triple> stream(Triple tripleMatch) {
        var pattern = PatternClassifier.classify(tripleMatch);
        if(pattern == MatchPattern.___)
            return this.stream();

        var bitmap = this.getBitmapForMatch(tripleMatch, pattern);
        return bitmap.stream().mapToObj(this.tripleList::get);
    }

    @Override
    public ExtendedIterator<Triple> find(Triple tripleMatch) {
        var pattern = PatternClassifier.classify(tripleMatch);
        if(pattern == MatchPattern.___)
            return new FilterIterator<>(Objects::nonNull, this.tripleList.iterator());

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
                    return tripleList.get(buffer[--bufferIndex]);

                if(!iterator.hasNext()) {
                    throw new NoSuchElementException();
                }
                bufferIndex = iterator.nextBatch(buffer);
                return tripleList.get(buffer[--bufferIndex]);
            }

            @Override
            public void forEachRemaining(Consumer<? super Triple> action) {
                if(bufferIndex > 0) {
                    for(int i = bufferIndex - 1; i >= 0; i--) {
                        action.accept(tripleList.get(buffer[i]));
                    }
                }
                while (iterator.hasNext()) {
                    bufferIndex = iterator.nextBatch(buffer);
                    for(int i = bufferIndex - 1; i >= 0; i--) {
                        action.accept(tripleList.get(buffer[i]));
                    }
                }
            }
        };
    }
}
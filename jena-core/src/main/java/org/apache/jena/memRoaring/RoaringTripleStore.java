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

package org.apache.jena.memRoaring;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.*;
import org.roaringbitmap.*;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class RoaringTripleStore implements TripleStore {

    Map<Node, RoaringBitmap> subjectBitmaps = new HashMap<>();
    Map<Node, RoaringBitmap> predicateBitmaps = new HashMap<>();
    Map<Node, RoaringBitmap> objectBitmaps = new HashMap<>();
    List<Triple> tripleList = new ArrayList<>(); // We use a list here to maintain the order of triples

    java.util.ArrayDeque<Integer> freeIndicesFromRemovedTriples = new java.util.ArrayDeque<>();

    public RoaringTripleStore() {
        this.clear();
    }


    @Override
    public void add(final Triple triple) {
        final var subjectBitmap = this.subjectBitmaps.computeIfAbsent(triple.getSubject(), n -> new RoaringBitmap());
        final var predicateBitmap = this.predicateBitmaps.computeIfAbsent(triple.getPredicate(), n -> new RoaringBitmap());
        final var objectBitmap = this.objectBitmaps.computeIfAbsent(triple.getObject(), n -> new RoaringBitmap());

        final var bitmap = subjectBitmap.clone();
        bitmap.and(predicateBitmap);
        bitmap.and(objectBitmap);

        if(!bitmap.isEmpty())
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
        predicateBitmap.add(index);
        objectBitmap.add(index);
//
//        if(subjectBitmap.getCardinality() % 500 == 0) {
//            subjectBitmap.runOptimize();
//        }
//        if(predicateBitmap.getCardinality() % 500 == 0) {
//            predicateBitmap.runOptimize();
//        }
//        if(objectBitmap.getCardinality() % 500 == 0) {
//            objectBitmap.runOptimize();
//        }
    }

    @Override
    public void remove(final Triple triple) {
        var subjectBitmap = this.subjectBitmaps.getOrDefault(triple.getSubject(), new RoaringBitmap());
        var predicateBitmap = this.predicateBitmaps.getOrDefault(triple.getPredicate(), new RoaringBitmap());
        var objectBitmap = this.objectBitmaps.getOrDefault(triple.getObject(), new RoaringBitmap());

        var bitmap = subjectBitmap.clone();
        bitmap.and(predicateBitmap);
        bitmap.and(objectBitmap);

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
//        else if (subjectBitmap.getCardinality() % 500 == 0) {
//            subjectBitmap.trim();
//            subjectBitmap.runOptimize();
//        }
        if(predicateBitmap.isEmpty()) {
            predicateBitmaps.remove(triple.getPredicate());
        }
//        else if (predicateBitmap.getCardinality() % 500 == 0) {
//            predicateBitmap.trim();
//            predicateBitmap.runOptimize();
//        }
        if(objectBitmap.isEmpty()) {
            objectBitmaps.remove(triple.getObject());
        }
//        else if (objectBitmap.getCardinality() % 500 == 0) {
//            objectBitmap.trim();
//            objectBitmap.runOptimize();
//        }
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
        switch (PatternClassifier.classify(tripleMatch)) {

            case S__:
                return this.subjectBitmaps.containsKey(tripleMatch.getSubject());
            case _P_:
                return this.predicateBitmaps.containsKey(tripleMatch.getPredicate());
            case __O:
                return this.objectBitmaps.containsKey(tripleMatch.getObject());

            case SP_:
                {
                    final var subjectBitmap = this.subjectBitmaps.get(tripleMatch.getSubject());
                    if(null == subjectBitmap)
                        return false;
                    final var predicateBitmap = this.predicateBitmaps.get(tripleMatch.getPredicate());
                    if(null == predicateBitmap)
                        return false;
                    final var bitmap = subjectBitmap.clone();
                    bitmap.and(predicateBitmap);
                    return !bitmap.isEmpty();
                }

            case _PO:
                {
                    final var predicateBitmap = this.predicateBitmaps.get(tripleMatch.getPredicate());
                    if(null == predicateBitmap)
                        return false;
                    final var objectBitmap = this.objectBitmaps.get(tripleMatch.getObject());
                    if(null == objectBitmap)
                        return false;
                    final var bitmap = predicateBitmap.clone();
                    bitmap.and(objectBitmap);
                    return !bitmap.isEmpty();
                }

            case S_O:
                {
                    final var subjectBitmap = this.subjectBitmaps.get(tripleMatch.getSubject());
                    if(null == subjectBitmap)
                        return false;
                    final var objectBitmap = this.objectBitmaps.get(tripleMatch.getObject());
                    if(null == objectBitmap)
                        return false;
                    final var bitmap = subjectBitmap.clone();
                    bitmap.and(objectBitmap);
                    return !bitmap.isEmpty();
                }

            case SPO:
                {
                    final var subjectBitmap = this.subjectBitmaps.get(tripleMatch.getSubject());
                    if (null == subjectBitmap)
                        return false;
                    final var predicateBitmap = this.predicateBitmaps.get(tripleMatch.getPredicate());
                    if (null == predicateBitmap)
                        return false;

                    final var bitmap = subjectBitmap.clone();
                    bitmap.and(predicateBitmap);

                    if(bitmap.isEmpty())
                        return false;

                    final var objectBitmap = this.objectBitmaps.get(tripleMatch.getObject());
                    if(null == objectBitmap)
                        return false;

                    //bitmap.runOptimize();
                    bitmap.and(objectBitmap);

                    return !bitmap.isEmpty();
                }

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
                final var bitmap = subjectBitmap.clone();
                bitmap.and(predicateBitmap);
                //bitmap.runOptimize();
                return bitmap;
            }

            case _PO:
            {
                final var predicateBitmap = this.predicateBitmaps.get(tripleMatch.getPredicate());
                if(null == predicateBitmap)
                    return EMPTY_BITMAP;
                final var objectBitmap = this.objectBitmaps.get(tripleMatch.getObject());
                if(null == objectBitmap)
                    return EMPTY_BITMAP;
                final var bitmap = predicateBitmap.clone();
                bitmap.and(objectBitmap);
                //bitmap.runOptimize();
                return bitmap;
            }

            case S_O:
            {
                final var subjectBitmap = this.subjectBitmaps.get(tripleMatch.getSubject());
                if(null == subjectBitmap)
                    return EMPTY_BITMAP;
                final var objectBitmap = this.objectBitmaps.get(tripleMatch.getObject());
                if(null == objectBitmap)
                    return EMPTY_BITMAP;
                final var bitmap = subjectBitmap.clone();
                bitmap.and(objectBitmap);
                //bitmap.runOptimize();
                return bitmap;
            }

            case SPO:
            {
                final var subjectBitmap = this.subjectBitmaps.get(tripleMatch.getSubject());
                if (null == subjectBitmap)
                    return EMPTY_BITMAP;
                final var predicateBitmap = this.predicateBitmaps.get(tripleMatch.getPredicate());
                if (null == predicateBitmap)
                    return EMPTY_BITMAP;

                final var bitmap = subjectBitmap.clone();
                bitmap.and(predicateBitmap);

                if(bitmap.isEmpty())
                    return EMPTY_BITMAP;

                final var objectBitmap = this.objectBitmaps.get(tripleMatch.getObject());
                if(null == objectBitmap)
                    return EMPTY_BITMAP;

                bitmap.and(objectBitmap);

                //bitmap.runOptimize();
                return bitmap;
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
        if(pattern ==MatchPattern.___)
            return this.stream();

        var bitmap = this.getBitmapForMatch(tripleMatch, pattern);
        return bitmap.stream().mapToObj(this.tripleList::get);
    }

    @Override
    public ExtendedIterator<Triple> find(Triple tripleMatch) {
        var pattern = PatternClassifier.classify(tripleMatch);
        if(pattern ==MatchPattern.___)
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

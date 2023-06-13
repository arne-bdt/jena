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
import org.apache.jena.mem2.collection.FastHashMap;
import org.apache.jena.mem2.collection.FastHashSet;
import org.apache.jena.mem2.pattern.MatchPattern;
import org.apache.jena.mem2.pattern.PatternClassifier;
import org.apache.jena.mem2.store.TripleStore;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.SingletonIterator;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.ImmutableBitmapDataProvider;
import org.roaringbitmap.RoaringBitmap;

import java.util.stream.Stream;

public class RoaringTripleStore implements TripleStore {

    private static final RoaringBitmap EMPTY_BITMAP = new RoaringBitmap();
    NodesToBitmapsMap subjectBitmaps = new NodesToBitmapsMap();
    NodesToBitmapsMap predicateBitmaps = new NodesToBitmapsMap();
    NodesToBitmapsMap objectBitmaps = new NodesToBitmapsMap();
    TripleSet triples = new TripleSet(); // We use a list here to maintain the order of triples

    public RoaringTripleStore() {

    }

    private static void addIndex(final NodesToBitmapsMap map, final Node node, final int index) {
        final var bitmap = map.computeIfAbsent(node, () -> new RoaringBitmap());
        bitmap.add(index);
        //bitmap.runOptimize();
    }

    private static void removeIndex(final NodesToBitmapsMap map, final Node node, final int index) {
        final var bitmap = map.get(node);
        bitmap.remove(index);
        //bitmap.trim();
        if (bitmap.isEmpty()) {
            map.removeUnchecked(node);
        }
    }

    @Override
    public void add(final Triple triple) {
        final var index = triples.addAndGetIndex(triple);
        if (index < 0) { /*triple already exists*/
            return;
        }
        addIndex(this.subjectBitmaps, triple.getSubject(), index);
        addIndex(this.predicateBitmaps, triple.getPredicate(), index);
        addIndex(this.objectBitmaps, triple.getObject(), index);
    }

    @Override
    public void remove(final Triple triple) {
        final var index = triples.removeAndGetIndex(triple);
        if (index < 0) { /*triple does not exist*/
            return;
        }
        removeIndex(this.subjectBitmaps, triple.getSubject(), index);
        removeIndex(this.predicateBitmaps, triple.getPredicate(), index);
        removeIndex(this.objectBitmaps, triple.getObject(), index);
    }

    @Override
    public void clear() {
        this.subjectBitmaps.clear();
        this.predicateBitmaps.clear();
        this.objectBitmaps.clear();
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
                return this.subjectBitmaps.containsKey(tripleMatch.getSubject());
            case _P_:
                return this.predicateBitmaps.containsKey(tripleMatch.getPredicate());
            case __O:
                return this.objectBitmaps.containsKey(tripleMatch.getObject());

            case SP_:
            case _PO:
            case S_O:
                return hasMatchInBitmaps(tripleMatch, matchPattern);

            case SPO:
                return this.triples.containsKey(tripleMatch);

            case ___:
                return !this.isEmpty();

            default:
                throw new IllegalStateException("Unknown pattern classifier: " + PatternClassifier.classify(tripleMatch));
        }
    }

    private ImmutableBitmapDataProvider getBitmapForMatch(final Triple tripleMatch, final MatchPattern matchPattern) {
        switch (matchPattern) {

            case S__:
                return this.subjectBitmaps.getOrDefault(tripleMatch.getSubject(), EMPTY_BITMAP);
            case _P_:
                return this.predicateBitmaps.getOrDefault(tripleMatch.getPredicate(), EMPTY_BITMAP);
            case __O:
                return this.objectBitmaps.getOrDefault(tripleMatch.getObject(), EMPTY_BITMAP);

            case SP_: {
                final var subjectBitmap = this.subjectBitmaps.get(tripleMatch.getSubject());
                if (null == subjectBitmap)
                    return EMPTY_BITMAP;

                final var predicateBitmap = this.predicateBitmaps.get(tripleMatch.getPredicate());
                if (null == predicateBitmap)
                    return EMPTY_BITMAP;

                return RoaringBitmap.and(subjectBitmap, predicateBitmap);
            }

            case _PO: {
                final var predicateBitmap = this.predicateBitmaps.get(tripleMatch.getPredicate());
                if (null == predicateBitmap)
                    return EMPTY_BITMAP;

                final var objectBitmap = this.objectBitmaps.get(tripleMatch.getObject());
                if (null == objectBitmap)
                    return EMPTY_BITMAP;

                return FastAggregation.naive_and(predicateBitmap, objectBitmap);
            }

            case S_O: {
                final var subjectBitmap = this.subjectBitmaps.get(tripleMatch.getSubject());
                if (null == subjectBitmap)
                    return EMPTY_BITMAP;

                final var objectBitmap = this.objectBitmaps.get(tripleMatch.getObject());
                if (null == objectBitmap)
                    return EMPTY_BITMAP;

                return RoaringBitmap.and(subjectBitmap, objectBitmap);
            }

            case SPO:
                throw new IllegalArgumentException("Getting bitmap for match pattern SPO ist not supported because it is not efficient");

            case ___:
                throw new IllegalArgumentException("Cannot get bitmap for match pattern ___");

            default:
                throw new IllegalStateException("Unknown pattern classifier: " + PatternClassifier.classify(tripleMatch));
        }
    }

    private boolean hasMatchInBitmaps(final Triple tripleMatch, final MatchPattern matchPattern) {
        switch (matchPattern) {

            case S__:
                return this.subjectBitmaps.containsKey(tripleMatch.getSubject());
            case _P_:
                return this.predicateBitmaps.containsKey(tripleMatch.getPredicate());
            case __O:
                return this.objectBitmaps.containsKey(tripleMatch.getObject());

            case SP_: {
                final var subjectBitmap = this.subjectBitmaps.get(tripleMatch.getSubject());
                if (null == subjectBitmap)
                    return false;

                final var predicateBitmap = this.predicateBitmaps.get(tripleMatch.getPredicate());
                if (null == predicateBitmap)
                    return false;

                return RoaringBitmap.intersects(subjectBitmap, predicateBitmap);
            }

            case _PO: {
                final var predicateBitmap = this.predicateBitmaps.get(tripleMatch.getPredicate());
                if (null == predicateBitmap)
                    return false;

                final var objectBitmap = this.objectBitmaps.get(tripleMatch.getObject());
                if (null == objectBitmap)
                    return false;

                return RoaringBitmap.intersects(objectBitmap, predicateBitmap);
            }

            case S_O: {
                final var subjectBitmap = this.subjectBitmaps.get(tripleMatch.getSubject());
                if (null == subjectBitmap)
                    return false;

                final var objectBitmap = this.objectBitmaps.get(tripleMatch.getObject());
                if (null == objectBitmap)
                    return false;

                return RoaringBitmap.intersects(subjectBitmap, objectBitmap);
            }

            case SPO:
                throw new IllegalArgumentException("Getting bitmap for match pattern SPO ist not supported because it is not efficient");

            case ___:
                throw new IllegalArgumentException("Cannot get bitmap for match pattern ___");

            default:
                throw new IllegalStateException("Unknown pattern classifier: " + PatternClassifier.classify(tripleMatch));
        }
    }

    @Override
    public Stream<Triple> stream() {
        return this.triples.keyStream();
    }

    @Override
    public Stream<Triple> stream(Triple tripleMatch) {
        var pattern = PatternClassifier.classify(tripleMatch);
        switch (pattern) {

            case SPO:
                return this.triples.containsKey(tripleMatch) ? Stream.of(tripleMatch) : Stream.empty();

            case SP_:
            case S_O:
            case S__:
            case _PO:
            case _P_:
            case __O:
                final var bitmap = this.getBitmapForMatch(tripleMatch, pattern);
                return bitmap.stream().mapToObj(this.triples::getKeyAt);

            case ___:
                return this.stream();

            default:
                throw new IllegalStateException("Unknown pattern classifier: " + PatternClassifier.classify(tripleMatch));
        }
    }

    @Override
    public ExtendedIterator<Triple> find(Triple tripleMatch) {
        var pattern = PatternClassifier.classify(tripleMatch);
        switch (pattern) {

            case SPO:
                return this.triples.containsKey(tripleMatch) ? new SingletonIterator(tripleMatch) : NiceIterator.emptyIterator();

            case SP_:
            case S_O:
            case S__:
            case _PO:
            case _P_:
            case __O:
                final var bitmap = this.getBitmapForMatch(tripleMatch, pattern);
                return new RoaringBitmapTripleIterator(bitmap.getBatchIterator(), this.triples);

            case ___:
                return this.triples.keyIterator();

            default:
                throw new IllegalStateException("Unknown pattern classifier: " + PatternClassifier.classify(tripleMatch));
        }
    }

    private class TripleSet extends FastHashSet<Triple> {

        @Override
        protected Triple[] newKeysArray(int size) {
            return new Triple[size];
        }
    }

    private class NodesToBitmapsMap extends FastHashMap<Node, RoaringBitmap> {

        @Override
        protected Node[] newKeysArray(int size) {
            return new Node[size];
        }

        @Override
        protected RoaringBitmap[] newValuesArray(int size) {
            return new RoaringBitmap[size];
        }
    }
}

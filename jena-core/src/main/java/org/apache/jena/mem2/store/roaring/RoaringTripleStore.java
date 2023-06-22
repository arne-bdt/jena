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

    private static final String UNKNOWN_PATTERN_CLASSIFIER = "Unknown pattern classifier: %s";
    private static final RoaringBitmap EMPTY_BITMAP = new RoaringBitmap();
    final NodesToBitmapsMap subjectBitmaps = new NodesToBitmapsMap();
    final NodesToBitmapsMap predicateBitmaps = new NodesToBitmapsMap();
    final NodesToBitmapsMap objectBitmaps = new NodesToBitmapsMap();
    final TripleSet triples = new TripleSet(); // We use a list here to maintain the order of triples

    private static void addIndex(final NodesToBitmapsMap map, final Node node, final int index) {
        final var bitmap = map.computeIfAbsent(node, RoaringBitmap::new);
        bitmap.add(index);
    }

    private static void removeIndex(final NodesToBitmapsMap map, final Node node, final int index) {
        final var bitmap = map.get(node);
        bitmap.remove(index);
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

            case SUB_ANY_ANY:
            case ANY_PRE_ANY:
            case ANY_ANY_OBJ:
            case SUB_PRE_ANY:
            case ANY_PRE_OBJ:
            case SUB_ANY_OBJ:
                return hasMatchInBitmaps(tripleMatch, matchPattern);

            case SUB_PRE_OBJ:
                return this.triples.containsKey(tripleMatch);

            case ANY_ANY_ANY:
                return !this.isEmpty();

            default:
                throw new IllegalStateException(String.format(UNKNOWN_PATTERN_CLASSIFIER, PatternClassifier.classify(tripleMatch)));
        }
    }

    private ImmutableBitmapDataProvider getBitmapForMatch(final Triple tripleMatch, final MatchPattern matchPattern) {
        switch (matchPattern) {

            case SUB_ANY_ANY:
                return this.subjectBitmaps.getOrDefault(tripleMatch.getSubject(), EMPTY_BITMAP);
            case ANY_PRE_ANY:
                return this.predicateBitmaps.getOrDefault(tripleMatch.getPredicate(), EMPTY_BITMAP);
            case ANY_ANY_OBJ:
                return this.objectBitmaps.getOrDefault(tripleMatch.getObject(), EMPTY_BITMAP);

            case SUB_PRE_ANY: {
                final var subjectBitmap = this.subjectBitmaps.get(tripleMatch.getSubject());
                if (null == subjectBitmap)
                    return EMPTY_BITMAP;

                final var predicateBitmap = this.predicateBitmaps.get(tripleMatch.getPredicate());
                if (null == predicateBitmap)
                    return EMPTY_BITMAP;

                return RoaringBitmap.and(subjectBitmap, predicateBitmap);
            }

            case ANY_PRE_OBJ: {
                final var predicateBitmap = this.predicateBitmaps.get(tripleMatch.getPredicate());
                if (null == predicateBitmap)
                    return EMPTY_BITMAP;

                final var objectBitmap = this.objectBitmaps.get(tripleMatch.getObject());
                if (null == objectBitmap)
                    return EMPTY_BITMAP;

                return FastAggregation.naive_and(predicateBitmap, objectBitmap);
            }

            case SUB_ANY_OBJ: {
                final var subjectBitmap = this.subjectBitmaps.get(tripleMatch.getSubject());
                if (null == subjectBitmap)
                    return EMPTY_BITMAP;

                final var objectBitmap = this.objectBitmaps.get(tripleMatch.getObject());
                if (null == objectBitmap)
                    return EMPTY_BITMAP;

                return RoaringBitmap.and(subjectBitmap, objectBitmap);
            }

            case SUB_PRE_OBJ:
                throw new IllegalArgumentException("Getting bitmap for match pattern SPO ist not supported because it is not efficient");

            case ANY_ANY_ANY:
                throw new IllegalArgumentException("Cannot get bitmap for match pattern ___");

            default:
                throw new IllegalStateException(String.format(UNKNOWN_PATTERN_CLASSIFIER, PatternClassifier.classify(tripleMatch)));
        }
    }

    private boolean hasMatchInBitmaps(final Triple tripleMatch, final MatchPattern matchPattern) {
        switch (matchPattern) {

            case SUB_ANY_ANY:
                return this.subjectBitmaps.containsKey(tripleMatch.getSubject());
            case ANY_PRE_ANY:
                return this.predicateBitmaps.containsKey(tripleMatch.getPredicate());
            case ANY_ANY_OBJ:
                return this.objectBitmaps.containsKey(tripleMatch.getObject());

            case SUB_PRE_ANY: {
                final var subjectBitmap = this.subjectBitmaps.get(tripleMatch.getSubject());
                if (null == subjectBitmap)
                    return false;

                final var predicateBitmap = this.predicateBitmaps.get(tripleMatch.getPredicate());
                if (null == predicateBitmap)
                    return false;

                return RoaringBitmap.intersects(subjectBitmap, predicateBitmap);
            }

            case ANY_PRE_OBJ: {
                final var predicateBitmap = this.predicateBitmaps.get(tripleMatch.getPredicate());
                if (null == predicateBitmap)
                    return false;

                final var objectBitmap = this.objectBitmaps.get(tripleMatch.getObject());
                if (null == objectBitmap)
                    return false;

                return RoaringBitmap.intersects(objectBitmap, predicateBitmap);
            }

            case SUB_ANY_OBJ: {
                final var subjectBitmap = this.subjectBitmaps.get(tripleMatch.getSubject());
                if (null == subjectBitmap)
                    return false;

                final var objectBitmap = this.objectBitmaps.get(tripleMatch.getObject());
                if (null == objectBitmap)
                    return false;

                return RoaringBitmap.intersects(subjectBitmap, objectBitmap);
            }

            case SUB_PRE_OBJ:
                throw new IllegalArgumentException("Getting bitmap for match pattern SPO ist not supported because it is not efficient");

            case ANY_ANY_ANY:
                throw new IllegalArgumentException("Cannot get bitmap for match pattern ___");

            default:
                throw new IllegalStateException(String.format(UNKNOWN_PATTERN_CLASSIFIER, PatternClassifier.classify(tripleMatch)));
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

            case SUB_PRE_OBJ:
                return this.triples.containsKey(tripleMatch) ? Stream.of(tripleMatch) : Stream.empty();

            case SUB_PRE_ANY:
            case SUB_ANY_OBJ:
            case SUB_ANY_ANY:
            case ANY_PRE_OBJ:
            case ANY_PRE_ANY:
            case ANY_ANY_OBJ:
                final var bitmap = this.getBitmapForMatch(tripleMatch, pattern);
                return bitmap.stream().mapToObj(this.triples::getKeyAt);

            case ANY_ANY_ANY:
                return this.stream();

            default:
                throw new IllegalStateException("Unknown pattern classifier: " + PatternClassifier.classify(tripleMatch));
        }
    }

    @Override
    public ExtendedIterator<Triple> find(Triple tripleMatch) {
        var pattern = PatternClassifier.classify(tripleMatch);
        switch (pattern) {

            case SUB_PRE_OBJ:
                return this.triples.containsKey(tripleMatch) ? new SingletonIterator<>(tripleMatch) : NiceIterator.emptyIterator();

            case SUB_PRE_ANY:
            case SUB_ANY_OBJ:
            case SUB_ANY_ANY:
            case ANY_PRE_OBJ:
            case ANY_PRE_ANY:
            case ANY_ANY_OBJ:
                final var bitmap = this.getBitmapForMatch(tripleMatch, pattern);
                return new RoaringBitmapTripleIterator(bitmap.getBatchIterator(), this.triples);

            case ANY_ANY_ANY:
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

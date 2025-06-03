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

import org.apache.jena.atlas.lib.Copyable;
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

/**
 * A triple store that is ideal for handling extremely large graphs.
 * <p>
 * Internal structure:
 * - One indexed hash set (same as GraphMem2Fast uses) that holds all triples
 * - Three hash maps indexed by subjects, predicates, and objects with RoaringBitmaps as values
 * - The bitmaps contain the indices of the triples in the central hash set
 * <p>
 * The bitmaps are used to quickly find triples that match a given pattern.
 * The bitmaps operations like {@link FastAggregation#naive_and(RoaringBitmap...)} and
 * {@link RoaringBitmap#intersects(RoaringBitmap, RoaringBitmap)} are used to find matches for the pattern
 * S_O, SP_, and _PO pretty fast, even in large graphs.
 * <p>
 * Additional optimizations:
 * - because we know that if a triple exists in one of the maps, it also exists in the other two, we can use the
 * {@link org.apache.jena.mem2.collection.JenaMapSetCommon#removeUnchecked(Object)} method to avoid
 * unnecessary checks.
 */
public class RoaringTripleStoreLazyIndexing implements TripleStore {

    private static final String UNKNOWN_PATTERN_CLASSIFIER = "Unknown pattern classifier: %s";
    private static final RoaringBitmap EMPTY_BITMAP = new RoaringBitmap();
    final TripleSet triples; // In this special set, each element has an index

    private SPOBitmaps index = null;
    private LazyIndexStrategy lazyIndexStrategy;

    /**
     * Return the current indexing strategy.
     * @return the current indexing strategy
     */
    public LazyIndexStrategy getLazyIndexStrategy() {
        return lazyIndexStrategy;
    }

    public void clearIndex() {
        if (this.index != null) {
            this.index = null;
        }
    }

    public void rebuildIndex() {
        this.index = new SPOBitmaps(this.triples);
    }

    public enum LazyIndexStrategy {
        /**
         * Store starts without an index. Index is created when needed,
         * i.e. when a triple shall be found by a pattern match.
         */
        AUTOMATIC,
        /**
         * Store starts without an index. Indexing must be triggered manually by calling #rebuildIndex().
         */
        MANUAL
    }

    private class SPOBitmaps {
        final NodesToBitmapsMap subjectBitmaps;
        final NodesToBitmapsMap predicateBitmaps;
        final NodesToBitmapsMap objectBitmaps;
        public SPOBitmaps() {
            this.subjectBitmaps = new NodesToBitmapsMap();
            this.predicateBitmaps = new NodesToBitmapsMap();
            this.objectBitmaps = new NodesToBitmapsMap();
        }
        public SPOBitmaps(final TripleSet triples) {
            this.subjectBitmaps = new NodesToBitmapsMap();
            this.predicateBitmaps = new NodesToBitmapsMap();
            this.objectBitmaps = new NodesToBitmapsMap();
            final var tS = Thread.startVirtualThread(() -> {
                triples.indexIterator().forEachRemaining(indexAndTriple -> {
                    addIndex(this.subjectBitmaps, indexAndTriple.key().getSubject(), indexAndTriple.index());
                });
            });
            final var tP = Thread.startVirtualThread(() -> {
                triples.indexIterator().forEachRemaining(indexAndTriple -> {
                    addIndex(this.predicateBitmaps, indexAndTriple.key().getPredicate(), indexAndTriple.index());
                });
            });
            final var tO = Thread.startVirtualThread(() -> {
                triples.indexIterator().forEachRemaining(indexAndTriple -> {
                    addIndex(this.objectBitmaps, indexAndTriple.key().getObject(), indexAndTriple.index());
                });
            });
            try {
                tS.join();
                tP.join();
                tO.join();
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while building index for triples", e);
            }
        }
        public SPOBitmaps(final SPOBitmaps bitmapsToCopy) {
            this.subjectBitmaps = bitmapsToCopy.subjectBitmaps.copy();
            this.predicateBitmaps = bitmapsToCopy.predicateBitmaps.copy();
            this.objectBitmaps = bitmapsToCopy.objectBitmaps.copy();
        }
        public void addIndex(final Triple triple, final int index) {
            addIndex(this.subjectBitmaps, triple.getSubject(), index);
            addIndex(this.predicateBitmaps, triple.getPredicate(), index);
            addIndex(this.objectBitmaps, triple.getObject(), index);
        }
        public void removeIndex(final Triple triple, final int index) {
            removeIndex(this.subjectBitmaps, triple.getSubject(), index);
            removeIndex(this.predicateBitmaps, triple.getPredicate(), index);
            removeIndex(this.objectBitmaps, triple.getObject(), index);
        }
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
    }

    public RoaringTripleStoreLazyIndexing(final LazyIndexStrategy lazyIndexStrategy) {
        this.lazyIndexStrategy = lazyIndexStrategy;
        triples = new TripleSet();
        rebuildIndex();
    }

    private RoaringTripleStoreLazyIndexing(final RoaringTripleStoreLazyIndexing storeToCopy) {
        this.lazyIndexStrategy = storeToCopy.lazyIndexStrategy;
        if (storeToCopy.index == null) {
            this.index = null;
        } else {
            this.index = new SPOBitmaps(storeToCopy.index);
        }
        triples = storeToCopy.triples.copy();
    }

    @Override
    public void add(final Triple triple) {
        final var index = triples.addAndGetIndex(triple);
        if (index < 0) { /*triple already exists*/
            return;
        }
        if(this.index != null) {
            this.index.addIndex(triple, index);
        }
    }

    @Override
    public void remove(final Triple triple) {
        final var index = triples.removeAndGetIndex(triple);
        if (index < 0) { /*triple does not exist*/
            return;
        }
        if (this.index != null) {
            this.index.removeIndex(triple, index);
        }
    }

    @Override
    public void clear() {
        this.index = null;
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

            case SUB_ANY_ANY,
                 ANY_PRE_ANY,
                 ANY_ANY_OBJ,
                 SUB_PRE_ANY,
                 ANY_PRE_OBJ,
                 SUB_ANY_OBJ:
                checkIndexingStrategy();
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
                return this.index.subjectBitmaps.getOrDefault(tripleMatch.getSubject(), EMPTY_BITMAP);
            case ANY_PRE_ANY:
                return this.index.predicateBitmaps.getOrDefault(tripleMatch.getPredicate(), EMPTY_BITMAP);
            case ANY_ANY_OBJ:
                return this.index.objectBitmaps.getOrDefault(tripleMatch.getObject(), EMPTY_BITMAP);

            case SUB_PRE_ANY: {
                final var subjectBitmap = this.index.subjectBitmaps.get(tripleMatch.getSubject());
                if (null == subjectBitmap)
                    return EMPTY_BITMAP;

                final var predicateBitmap = this.index.predicateBitmaps.get(tripleMatch.getPredicate());
                if (null == predicateBitmap)
                    return EMPTY_BITMAP;

                return FastAggregation.naive_and(subjectBitmap, predicateBitmap);
            }

            case ANY_PRE_OBJ: {
                final var predicateBitmap = this.index.predicateBitmaps.get(tripleMatch.getPredicate());
                if (null == predicateBitmap)
                    return EMPTY_BITMAP;

                final var objectBitmap = this.index.objectBitmaps.get(tripleMatch.getObject());
                if (null == objectBitmap)
                    return EMPTY_BITMAP;

                return FastAggregation.naive_and(predicateBitmap, objectBitmap);
            }

            case SUB_ANY_OBJ: {
                final var subjectBitmap = this.index.subjectBitmaps.get(tripleMatch.getSubject());
                if (null == subjectBitmap)
                    return EMPTY_BITMAP;

                final var objectBitmap = this.index.objectBitmaps.get(tripleMatch.getObject());
                if (null == objectBitmap)
                    return EMPTY_BITMAP;

                return FastAggregation.naive_and(subjectBitmap, objectBitmap);
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
                return this.index.subjectBitmaps.containsKey(tripleMatch.getSubject());
            case ANY_PRE_ANY:
                return this.index.predicateBitmaps.containsKey(tripleMatch.getPredicate());
            case ANY_ANY_OBJ:
                return this.index.objectBitmaps.containsKey(tripleMatch.getObject());

            case SUB_PRE_ANY: {
                final var subjectBitmap = this.index.subjectBitmaps.get(tripleMatch.getSubject());
                if (null == subjectBitmap)
                    return false;

                final var predicateBitmap = this.index.predicateBitmaps.get(tripleMatch.getPredicate());
                if (null == predicateBitmap)
                    return false;

                return RoaringBitmap.intersects(subjectBitmap, predicateBitmap);
            }

            case ANY_PRE_OBJ: {
                final var predicateBitmap = this.index.predicateBitmaps.get(tripleMatch.getPredicate());
                if (null == predicateBitmap)
                    return false;

                final var objectBitmap = this.index.objectBitmaps.get(tripleMatch.getObject());
                if (null == objectBitmap)
                    return false;

                return RoaringBitmap.intersects(objectBitmap, predicateBitmap);
            }

            case SUB_ANY_OBJ: {
                final var subjectBitmap = this.index.subjectBitmaps.get(tripleMatch.getSubject());
                if (null == subjectBitmap)
                    return false;

                final var objectBitmap = this.index.objectBitmaps.get(tripleMatch.getObject());
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

            case SUB_PRE_ANY,
                 SUB_ANY_OBJ,
                 SUB_ANY_ANY,
                 ANY_PRE_OBJ,
                 ANY_PRE_ANY,
                 ANY_ANY_OBJ:
                checkIndexingStrategy();
                return this.getBitmapForMatch(tripleMatch, pattern)
                        .stream().mapToObj(this.triples::getKeyAt);

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

            case SUB_PRE_ANY,
                 SUB_ANY_OBJ,
                 SUB_ANY_ANY,
                 ANY_PRE_OBJ,
                 ANY_PRE_ANY,
                 ANY_ANY_OBJ:
                checkIndexingStrategy();
                return new RoaringBitmapTripleIterator(this.getBitmapForMatch(tripleMatch, pattern), this.triples);

            case ANY_ANY_ANY:
                return this.triples.keyIterator();

            default:
                throw new IllegalStateException("Unknown pattern classifier: " + PatternClassifier.classify(tripleMatch));
        }
    }

    private void checkIndexingStrategy() {
        if(this.index == null) {
            if(this.lazyIndexStrategy == LazyIndexStrategy.AUTOMATIC) {
                this.rebuildIndex();
            } else if (this.lazyIndexStrategy == LazyIndexStrategy.MANUAL) {
                throw new IllegalStateException("Indexing strategy is set to MANUAL and index has not been built yet. Please build the index before querying.");
            }
        }
    }

    @Override
    public RoaringTripleStoreLazyIndexing copy() {
        return new RoaringTripleStoreLazyIndexing(this);
    }

    /**
     * Set of triples that is backed by a {@link TripleSet}.
     */
    private static class TripleSet
            extends FastHashSet<Triple>
            implements Copyable<TripleSet>{

        public TripleSet() {
            super();
        }

        private TripleSet(final FastHashSet<Triple> setToCopy) {
            super(setToCopy);
        }

        @Override
        protected Triple[] newKeysArray(int size) {
            return new Triple[size];
        }

        /**
         * Create a copy of this set.
         *
         * @return TripleSet
         */
        @Override
        public TripleSet copy() {
            return new TripleSet(this);
        }
    }

    /**
     * Map from {@link Node} to {@link RoaringBitmap}.
     */
    private static class NodesToBitmapsMap
            extends FastHashMap<Node, RoaringBitmap>
            implements Copyable<NodesToBitmapsMap> {

        public NodesToBitmapsMap() {
            super();
        }

        public NodesToBitmapsMap(final NodesToBitmapsMap mapToCopy) {
            super(mapToCopy, RoaringBitmap::clone);
        }

        @Override
        protected Node[] newKeysArray(int size) {
            return new Node[size];
        }

        @Override
        protected RoaringBitmap[] newValuesArray(int size) {
            return new RoaringBitmap[size];
        }

        /**
         * Create a copy of this map.
         * The new map will contain all the same nodes as keys of this map, but clones of the bitmaps as values.
         *
         * @return a copy of this map
         */
        @Override
        public NodesToBitmapsMap copy() {
            return new NodesToBitmapsMap(this);
        }
    }
}

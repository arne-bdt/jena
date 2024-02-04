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

package org.apache.jena.mem2.store.kd;

import org.apache.jena.atlas.lib.StreamOps;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.pattern.MatchPattern;
import org.apache.jena.mem2.pattern.PatternClassifier;
import org.apache.jena.mem2.store.TripleStore;
import org.apache.jena.mem2.store.kd.tinspin.Index;
import org.apache.jena.mem2.store.kd.tinspin.KDIterator;
import org.apache.jena.mem2.store.kd.tinspin.KDTree;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.SingletonIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.RoaringBitmap;

import java.util.function.Predicate;
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
public class KDTreeTripleStore implements TripleStore {

    private static final String UNKNOWN_PATTERN_CLASSIFIER = "Unknown pattern classifier: %s";

    private final KDTree<Triple> triples = KDTree.create(3);

    private static int[] getDimensions(Triple triple) {
        return new int[]{
                triple.getSubject().hashCode(),
                triple.getPredicate().hashCode(),
                triple.getObject().hashCode()
        };
    }

    @Override
    public void add(final Triple triple) {
        final var key = getDimensions(triple);
        if(triples.contains(key, triple)) {
            return;
        }
        triples.insert(key, triple);
    }

    @Override
    public void remove(final Triple triple) {
        final var key = getDimensions(triple);
        triples.remove(key, triple);
    }

    @Override
    public void clear() {
        this.triples.clear();
    }

    @Override
    public int countTriples() {
        return this.triples.size();
    }

    @Override
    public boolean isEmpty() {
        return this.triples.size() == 0;
    }

    private KDIterator<Triple> query(MatchPattern matchPattern, Triple tripleMatch) {
        final int[] min;
        final int[] max;
        switch (matchPattern) {
            case SUB_PRE_ANY -> {
                min = new int[]{tripleMatch.getSubject().hashCode(), tripleMatch.getPredicate().hashCode(), Integer.MIN_VALUE};
                max = new int[]{tripleMatch.getSubject().hashCode(), tripleMatch.getPredicate().hashCode(), Integer.MAX_VALUE};
            }
            case SUB_ANY_OBJ -> {
                min = new int[]{tripleMatch.getSubject().hashCode(), Integer.MIN_VALUE, tripleMatch.getObject().hashCode()};
                max = new int[]{tripleMatch.getSubject().hashCode(), Integer.MAX_VALUE, tripleMatch.getObject().hashCode()};
            }
            case SUB_ANY_ANY -> {
                min = new int[]{tripleMatch.getSubject().hashCode(), Integer.MIN_VALUE, Integer.MIN_VALUE};
                max = new int[]{tripleMatch.getSubject().hashCode(), Integer.MAX_VALUE, Integer.MAX_VALUE};
            }
            case ANY_PRE_OBJ -> {
                min = new int[]{Integer.MIN_VALUE, tripleMatch.getPredicate().hashCode(), tripleMatch.getObject().hashCode()};
                max = new int[]{Integer.MAX_VALUE, tripleMatch.getPredicate().hashCode(), tripleMatch.getObject().hashCode()};
            }
            case ANY_PRE_ANY -> {
                min = new int[]{Integer.MIN_VALUE, tripleMatch.getPredicate().hashCode(), Integer.MIN_VALUE};
                max = new int[]{Integer.MAX_VALUE, tripleMatch.getPredicate().hashCode(), Integer.MAX_VALUE};
            }
            case ANY_ANY_OBJ -> {
                min = new int[]{Integer.MIN_VALUE, Integer.MIN_VALUE, tripleMatch.getObject().hashCode()};
                max = new int[]{Integer.MAX_VALUE, Integer.MAX_VALUE, tripleMatch.getObject().hashCode()};
            }
            default -> throw new IllegalStateException(String.format(UNKNOWN_PATTERN_CLASSIFIER, matchPattern));
        }
        return this.triples.query(min, max);
    }

    private boolean hasMatch(KDIterator<Triple> iter, Predicate<Triple> predicate) {
        while (iter.hasNext()) {
            if (predicate.test(iter.next().value())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean contains(Triple tripleMatch) {
        final var matchPattern = PatternClassifier.classify(tripleMatch);
        switch (matchPattern) {

            case SUB_ANY_ANY, ANY_PRE_ANY, ANY_ANY_OBJ, SUB_PRE_ANY, ANY_PRE_OBJ, SUB_ANY_OBJ:
                return hasMatch(query(matchPattern, tripleMatch), tripleMatch::matches);

            case SUB_PRE_OBJ:
                return this.triples.contains(getDimensions(tripleMatch), tripleMatch);

            case ANY_ANY_ANY:
                return !this.isEmpty();

            default:
                throw new IllegalStateException(String.format(UNKNOWN_PATTERN_CLASSIFIER, PatternClassifier.classify(tripleMatch)));
        }
    }

    @Override
    public Stream<Triple> stream() {
        return StreamOps.stream(this.triples.iterator()).map(Index.PointEntry::value);
    }

    @Override
    public Stream<Triple> stream(Triple tripleMatch) {
        var pattern = PatternClassifier.classify(tripleMatch);
        switch (pattern) {

            case SUB_PRE_OBJ:
                return this.triples.contains(getDimensions(tripleMatch), tripleMatch) ? Stream.of(tripleMatch) : Stream.empty();

            case SUB_PRE_ANY, SUB_ANY_OBJ, SUB_ANY_ANY, ANY_PRE_OBJ, ANY_PRE_ANY, ANY_ANY_OBJ:
                return StreamOps.stream(query(pattern, tripleMatch)).map(Index.PointEntry::value).filter(tripleMatch::matches);

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
                return this.triples.contains(getDimensions(tripleMatch), tripleMatch)  ? new SingletonIterator<>(tripleMatch) : NiceIterator.emptyIterator();

            case SUB_PRE_ANY, SUB_ANY_OBJ, SUB_ANY_ANY, ANY_PRE_OBJ, ANY_PRE_ANY, ANY_ANY_OBJ:
                return WrappedIterator.create(query(pattern, tripleMatch)).mapWith(Index.PointEntry::value).filterKeep(tripleMatch::matches);

            case ANY_ANY_ANY:
                return WrappedIterator.create(triples.iterator()).mapWith(Index.PointEntry::value);

            default:
                throw new IllegalStateException("Unknown pattern classifier: " + PatternClassifier.classify(tripleMatch));
        }
    }
}

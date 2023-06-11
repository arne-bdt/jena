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

package org.apache.jena.mem2.store.fullyIndexed;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.atlas.lib.tuple.Tuple2;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.collection.FastHashMap;
import org.apache.jena.mem2.collection.FastHashSet;
import org.apache.jena.mem2.collection.specialized.HashedIndexSet;
import org.apache.jena.mem2.pattern.MatchPattern;
import org.apache.jena.mem2.pattern.PatternClassifier;
import org.apache.jena.mem2.store.TripleStore;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.NullIterator;
import org.apache.jena.util.iterator.SingletonIterator;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.ImmutableBitmapDataProvider;
import org.roaringbitmap.RoaringBitmap;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class FullyIndexedTripleStore implements TripleStore {

    private static final RoaringBitmap EMPTY_BITMAP = new RoaringBitmap();
    NodesToIndexSet subjectIndices = new NodesToIndexSet();
    NodesToIndexSet predicateIndices = new NodesToIndexSet();
    NodesToIndexSet objectIndices = new NodesToIndexSet();

    TripleSet triples = new TripleSet(); // We use a list here to maintain the order of triples
    public FullyIndexedTripleStore() {

    }

    @Override
    public void add(final Triple triple) {
        final var index = triples.addAndGetIndex(triple);
        if (index < 0) { /*triple already exists*/
            return;
        }
        this.subjectIndices.computeIfAbsent(triple.getSubject(), HashedIndexSet::new).addUnchecked(index);
        this.predicateIndices.computeIfAbsent(triple.getPredicate(), HashedIndexSet::new).addUnchecked(index);
        this.objectIndices.computeIfAbsent(triple.getObject(), HashedIndexSet::new).addUnchecked(index);
    }

    @Override
    public void remove(final Triple triple) {
        final var index = triples.removeAndGetIndex(triple);
        if (index < 0) { /*triple does not exist*/
            return;
        }
        this.subjectIndices.get(triple.getSubject()).removeUnchecked(index);
        this.predicateIndices.get(triple.getPredicate()).removeUnchecked(index);
        this.objectIndices.get(triple.getObject()).removeUnchecked(index);
    }

    @Override
    public void clear() {
        this.subjectIndices.clear();
        this.predicateIndices.clear();
        this.objectIndices.clear();
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

            case SPO:
                return this.triples.containsKey(tripleMatch);
            case S__:
                return this.subjectIndices.containsKey(tripleMatch.getSubject());
            case _P_:
                return this.predicateIndices.containsKey(tripleMatch.getPredicate());
            case __O:
                return this.objectIndices.containsKey(tripleMatch.getObject());
            case SP_: {
                final var sIndices = this.subjectIndices.get(tripleMatch.getSubject());
                if (null == sIndices)
                    return false;

                final var pIndices = this.predicateIndices.get(tripleMatch.getPredicate());
                if (null == pIndices)
                    return false;

                return sIndices.intersects(pIndices);
            }
            case S_O: {
                final var sIndices = this.subjectIndices.get(tripleMatch.getSubject());
                if (null == sIndices)
                    return false;

                final var oIndices = this.objectIndices.get(tripleMatch.getObject());
                if (null == oIndices)
                    return false;

                return sIndices.intersects(oIndices);
            }
            case _PO: {
                final var pIndices = this.predicateIndices.get(tripleMatch.getPredicate());
                if (null == pIndices)
                    return false;

                final var oIndices = this.objectIndices.get(tripleMatch.getObject());
                if (null == oIndices)
                    return false;

                return oIndices.intersects(pIndices);
            }


            case ___:
                return !this.isEmpty();

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
            case S__:
                return steamOrEmpty(this.subjectIndices.get(tripleMatch.getSubject()));
            case _P_:
                return steamOrEmpty(this.predicateIndices.get(tripleMatch.getPredicate()));
            case __O:
                return steamOrEmpty(this.objectIndices.get(tripleMatch.getObject()));
            case SP_: {
                final var sIndices = this.subjectIndices.get(tripleMatch.getSubject());
                if (null == sIndices)
                    return Stream.empty();

                final var pIndices = this.predicateIndices.get(tripleMatch.getPredicate());
                if (null == pIndices)
                    return Stream.empty();

                return steamOrEmpty(sIndices.calcIntersection(pIndices));
            }
            case S_O: {
                final var sIndices = this.subjectIndices.get(tripleMatch.getSubject());
                if (null == sIndices)
                    return Stream.empty();

                final var oIndices = this.objectIndices.get(tripleMatch.getObject());
                if (null == oIndices)
                    return Stream.empty();

                return steamOrEmpty(sIndices.calcIntersection(oIndices));
            }
            case _PO: {
                final var pIndices = this.predicateIndices.get(tripleMatch.getPredicate());
                if (null == pIndices)
                    return Stream.empty();

                final var oIndices = this.objectIndices.get(tripleMatch.getObject());
                if (null == oIndices)
                    return Stream.empty();

                return steamOrEmpty(oIndices.calcIntersection(pIndices));
            }
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
                return this.triples.containsKey(tripleMatch) ? new SingletonIterator(tripleMatch) : NullIterator.instance();
            case S__:
                return iteratorOrEmpty(this.subjectIndices.get(tripleMatch.getSubject()));
            case _P_:
                return iteratorOrEmpty(this.predicateIndices.get(tripleMatch.getPredicate()));
            case __O:
                return iteratorOrEmpty(this.objectIndices.get(tripleMatch.getObject()));
            case SP_: {
                final var sIndices = this.subjectIndices.get(tripleMatch.getSubject());
                if (null == sIndices)
                    return NullIterator.instance();

                final var pIndices = this.predicateIndices.get(tripleMatch.getPredicate());
                if (null == pIndices)
                    return NullIterator.instance();

                return iteratorOrEmpty(sIndices.calcIntersection(pIndices));
            }
            case S_O: {
                final var sIndices = this.subjectIndices.get(tripleMatch.getSubject());
                if (null == sIndices)
                    return NullIterator.instance();

                final var oIndices = this.objectIndices.get(tripleMatch.getObject());
                if (null == oIndices)
                    return NullIterator.instance();

                return iteratorOrEmpty(sIndices.calcIntersection(oIndices));
            }
            case _PO: {
                final var pIndices = this.predicateIndices.get(tripleMatch.getPredicate());
                if (null == pIndices)
                    return NullIterator.instance();

                final var oIndices = this.objectIndices.get(tripleMatch.getObject());
                if (null == oIndices)
                    return NullIterator.instance();

                return iteratorOrEmpty(oIndices.calcIntersection(pIndices));
            }
            case ___:
                return this.triples.keyIterator();

            default:
                throw new IllegalStateException("Unknown pattern classifier: " + PatternClassifier.classify(tripleMatch));
        }
    }

    private Stream<Triple> steamOrEmpty(final HashedIndexSet indexSet) {
        return indexSet == null || indexSet.isEmpty()
                ? Stream.empty()
                : StreamSupport.stream(indexSet.spliterator(), false).map(this.triples::getKeyAt);
    }

    private ExtendedIterator<Triple> iteratorOrEmpty(final HashedIndexSet indexSet) {
        return indexSet == null || indexSet.isEmpty()
                ? NiceIterator.emptyIterator()
                : new IndexedTripleIterator(indexSet.iterator(), this.triples);
    }

    private static class TripleSet extends FastHashSet<Triple> {

        @Override
        protected Triple[] newKeysArray(int size) {
            return new Triple[size];
        }
    }

    private static class NodesToIndexSet extends FastHashMap<Node, HashedIndexSet> {

        @Override
        protected Node[] newKeysArray(int size) {
            return new Node[size];
        }

        @Override
        protected HashedIndexSet[] newValuesArray(int size) {
            return new HashedIndexSet[size];
        }
    }

    private static class NodesPairToIndexSet extends FastHashMap<Pair<Node, Node>, HashedIndexSet> {

        @Override
        protected Pair<Node, Node>[] newKeysArray(int size) {
            return new Pair[size];
        }

        @Override
        protected HashedIndexSet[] newValuesArray(int size) {
            return new HashedIndexSet[size];
        }
    }
}

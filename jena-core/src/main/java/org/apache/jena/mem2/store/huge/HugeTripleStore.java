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
import org.apache.jena.mem2.collection.JenaSet;
import org.apache.jena.mem2.iterator.NestedIterator;
import org.apache.jena.mem2.pattern.PatternClassifier;
import org.apache.jena.mem2.store.TripleStore;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NullIterator;
import org.apache.jena.util.iterator.SingletonIterator;

import java.util.stream.Stream;

public class HugeTripleStore implements TripleStore {

    private static final int MAX_ARRAY_BUNCH_SIZE = 16;
    final HugeHashedBunchMap subjects = new HugeHashedBunchMap();
    final HugeHashedBunchMap predicates = new HugeHashedBunchMap();
    final HugeHashedBunchMap objects = new HugeHashedBunchMap();
    private int size = 0;

    public HugeTripleStore() {
    }

    private static HudeTripleBunch computeAddUncheckedToPredicates(HudeTripleBunch bunch, final Triple t, final int hashCodeOfTriple) {
        if (bunch == null) {
            bunch = new ArrayBunchIndexingByObject();
            bunch.addUnchecked(t);
        } else if (bunch.isHashed()) {
            bunch.addUnchecked(t, hashCodeOfTriple);
        } else if (bunch.size() == MAX_ARRAY_BUNCH_SIZE) {
            bunch = new HashedBunchIndexingByObject(bunch);
            bunch.addUnchecked(t, hashCodeOfTriple);
        } else {
            bunch.addUnchecked(t);
        }
        return bunch;
    }

    private static HudeTripleBunch computeAddUncheckedToObjects(HudeTripleBunch bunch, final Triple t, final int hashCodeOfTriple) {
        if (bunch == null) {
            bunch = new ArrayBunchIndexingBySubject();
            bunch.addUnchecked(t);
        } else if (bunch.isHashed()) {
            bunch.addUnchecked(t, hashCodeOfTriple);
        } else if (bunch.size() == MAX_ARRAY_BUNCH_SIZE) {
            bunch = new HashedBunchIndexingBySubject(bunch);
            bunch.addUnchecked(t, hashCodeOfTriple);
        } else {
            bunch.addUnchecked(t);
        }
        return bunch;
    }

    private static HudeTripleBunch computeRemoveUnchecked(HudeTripleBunch bunch, final Triple t, final int hashCodeOfTriple) {
        if (bunch.isHashed()) {
            bunch.removeUnchecked(t, hashCodeOfTriple);
        } else {
            bunch.removeUnchecked(t);
        }
        return bunch.isEmpty() ? null : bunch;
    }

    @Override
    public void add(Triple triple) {
        final int hashCodeOfTriple = triple.hashCode();
        subjects.compute(triple.getSubject(), bunch -> {
            final boolean added;
            if (bunch == null) {
                bunch = new ArrayBunchIndexingByPredicate();
                bunch.addUnchecked(triple);
                added = true;
            } else if (bunch.isHashed()) {
                added = bunch.tryAdd(triple, hashCodeOfTriple);
            } else if (bunch.size() == MAX_ARRAY_BUNCH_SIZE) {
                bunch = new HashedBunchIndexingByPredicate(bunch);
                added = bunch.tryAdd(triple, hashCodeOfTriple);
            } else {
                added = bunch.tryAdd(triple);
            }
            if (added) {
                predicates.compute(triple.getPredicate(),
                        pBunch -> computeAddUncheckedToPredicates(pBunch, triple, hashCodeOfTriple));
                objects.compute(triple.getObject(),
                        oBunch -> computeAddUncheckedToObjects(oBunch, triple, hashCodeOfTriple));
                size++;
            }
            return bunch;
        });
    }

    @Override
    public void remove(Triple triple) {
        final int hashCodeOfTriple = triple.hashCode();
        subjects.compute(triple.getSubject(), bunch -> {
            final boolean removed;
            if (bunch == null) {
                removed = false;
            } else if (bunch.isHashed()) {
                removed = bunch.tryRemove(triple, hashCodeOfTriple);
            } else {
                removed = bunch.tryRemove(triple);
            }
            if (removed) {
                if (bunch.isEmpty()) {
                    bunch = null;
                }
                predicates.compute(triple.getPredicate(), pBunch -> computeRemoveUnchecked(pBunch, triple, hashCodeOfTriple));
                objects.compute(triple.getObject(), oBunch -> computeRemoveUnchecked(oBunch, triple, hashCodeOfTriple));
                size--;
            }
            return bunch;
        });
    }

    @Override
    public void clear() {
        subjects.clear();
        predicates.clear();
        objects.clear();
        size = 0;
    }

    @Override
    public int countTriples() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean contains(Triple tripleMatch) {
        switch (PatternClassifier.classify(tripleMatch)) {

            case SPO: {
                final var triples = subjects.get(tripleMatch.getSubject());
                if (triples == null) {
                    return false;
                }
                return triples.containsKey(tripleMatch);
            }

            case SP_: {
                final var triples = subjects.get(tripleMatch.getSubject());
                if (triples == null) {
                    return false;
                }
                return triples.containsNode(tripleMatch.getPredicate());
            }

            case S_O: {
                final var triples = objects.get(tripleMatch.getObject());
                if (triples == null) {
                    return false;
                }
                return triples.containsNode(tripleMatch.getSubject());
            }

            case S__:
                return subjects.containsKey(tripleMatch.getSubject());

            case _PO: {
                final var triples = predicates.get(tripleMatch.getPredicate());
                if (triples == null) {
                    return false;
                }
                return triples.containsNode(tripleMatch.getObject());
            }

            case _P_:
                return predicates.containsKey(tripleMatch.getPredicate());

            case __O:
                return objects.containsKey(tripleMatch.getObject());

            case ___:
                return !isEmpty();

            default:
                throw new IllegalStateException("Unexpected value: " + PatternClassifier.classify(tripleMatch));
        }
    }

    @Override
    public Stream<Triple> stream() {
        return subjects.valueStream().flatMap(HudeTripleBunch::keyStream);
    }

    @Override
    public Stream<Triple> stream(Triple tripleMatch) {
        switch (PatternClassifier.classify(tripleMatch)) {

            case SPO: {
                final var triples = subjects.get(tripleMatch.getSubject());
                if (triples == null) {
                    return Stream.empty();
                }
                return triples.containsKey(tripleMatch) ? Stream.of(tripleMatch) : Stream.empty();
            }

            case SP_: {
                final var triples = subjects.get(tripleMatch.getSubject());
                if (triples == null) {
                    return Stream.empty();
                }
                return triples.keyStream(tripleMatch.getPredicate());
            }

            case S_O: {
                final var triples = objects.get(tripleMatch.getObject());
                if (triples == null) {
                    return Stream.empty();
                }
                return triples.keyStream(tripleMatch.getSubject());
            }

            case S__: {
                final var triples = subjects.get(tripleMatch.getSubject());
                return triples == null ? Stream.empty() : triples.keyStream();
            }

            case _PO: {
                final var triples = predicates.get(tripleMatch.getPredicate());
                if (triples == null) {
                    return Stream.empty();
                }
                return triples.keyStream(tripleMatch.getObject());
            }

            case _P_: {
                final var triples = predicates.get(tripleMatch.getPredicate());
                return triples == null ? Stream.empty() : triples.keyStream();
            }

            case __O: {
                final var triples = objects.get(tripleMatch.getObject());
                return triples == null ? Stream.empty() : triples.keyStream();
            }

            case ___:
                return stream();

            default:
                throw new IllegalStateException("Unexpected value: " + PatternClassifier.classify(tripleMatch));
        }
    }

    @Override
    public ExtendedIterator<Triple> find(Triple tripleMatch) {
        switch (PatternClassifier.classify(tripleMatch)) {

            case SPO: {
                final var triples = subjects.get(tripleMatch.getSubject());
                if (triples == null) {
                    return NullIterator.emptyIterator();
                }
                return triples.containsKey(tripleMatch) ? new SingletonIterator<>(tripleMatch) : NullIterator.emptyIterator();
            }

            case SP_: {
                final var triples = subjects.get(tripleMatch.getSubject());
                if (triples == null) {
                    return NullIterator.emptyIterator();
                }
                return triples.keyIterator(tripleMatch.getPredicate());
            }

            case S_O: {
                final var triples = objects.get(tripleMatch.getObject());
                if (triples == null) {
                    return NullIterator.emptyIterator();
                }
                return triples.keyIterator(tripleMatch.getSubject());
            }

            case S__: {
                final var triples = subjects.get(tripleMatch.getSubject());
                return triples == null ? NullIterator.emptyIterator() : triples.keyIterator();
            }

            case _PO: {
                final var triples = predicates.get(tripleMatch.getPredicate());
                if (triples == null) {
                    return NullIterator.emptyIterator();
                }
                return triples.keyIterator(tripleMatch.getObject());
            }

            case _P_: {
                final var triples = predicates.get(tripleMatch.getPredicate());
                return triples == null ? NullIterator.emptyIterator() : triples.keyIterator();
            }

            case __O: {
                final var triples = objects.get(tripleMatch.getObject());
                return triples == null ? NullIterator.emptyIterator() : triples.keyIterator();
            }

            case ___:
                return new NestedIterator<>(subjects.valueIterator(), HudeTripleBunch::keyIterator);

            default:
                throw new IllegalStateException("Unexpected value: " + PatternClassifier.classify(tripleMatch));
        }
    }

    private static class ArrayBunchIndexingBySubject extends HugeArrayBunch {

        @Override
        protected Node getIndexingNode(Triple t) {
            return t.getSubject();
        }
    }

    private static class ArrayBunchIndexingByPredicate extends HugeArrayBunch {

        @Override
        protected Node getIndexingNode(Triple t) {
            return t.getPredicate();
        }
    }

    private static class ArrayBunchIndexingByObject extends HugeArrayBunch {

        @Override
        protected Node getIndexingNode(Triple t) {
            return t.getObject();
        }
    }

    private static class HashedBunchIndexingBySubject extends HugeHashedTripleBunch {

        protected HashedBunchIndexingBySubject(JenaSet<Triple> b) {
            super(b);
        }

        @Override
        protected Node getIndexingNode(Triple t) {
            return t.getSubject();
        }
    }

    private static class HashedBunchIndexingByPredicate extends HugeHashedTripleBunch {

        protected HashedBunchIndexingByPredicate(JenaSet<Triple> b) {
            super(b);
        }

        @Override
        protected Node getIndexingNode(Triple t) {
            return t.getPredicate();
        }
    }

    private static class HashedBunchIndexingByObject extends HugeHashedTripleBunch {

        protected HashedBunchIndexingByObject(JenaSet<Triple> b) {
            super(b);
        }

        @Override
        protected Node getIndexingNode(Triple t) {
            return t.getObject();
        }
    }
}

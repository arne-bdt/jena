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
package org.apache.jena.mem2.store.fast;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.iterator.NestedIterator;
import org.apache.jena.mem2.pattern.PatternClassifier;
import org.apache.jena.mem2.store.TripleStore;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NullIterator;
import org.apache.jena.util.iterator.SingletonIterator;

import java.util.stream.Stream;

public class FastTripleStore implements TripleStore {

    private static final int MAX_ARRAY_BUNCH_SIZE_SUBJECT = 16;
    private static final int MAX_ARRAY_BUNCH_SIZE_PREDICATE_OBJECT = 32;
    public static final int THRESHOLD_FOR_SECONDARY_LOOKUP = 400;
    final FastHashedBunchMap subjects = new FastHashedBunchMap();
    final FastHashedBunchMap predicates = new FastHashedBunchMap();
    final FastHashedBunchMap objects = new FastHashedBunchMap();
    private int size = 0;
    public FastTripleStore() {
    }

    @Override
    public void add(Triple triple) {
        final int hashCodeOfTriple = triple.hashCode();
        final boolean added;
        var sBunch = subjects.get(triple.getSubject());
        if (sBunch == null) {
            sBunch = new ArrayBunchWithSameSubject();
            sBunch.addUnchecked(triple, hashCodeOfTriple);
            subjects.put(triple.getSubject(), sBunch);
            added = true;
        } else {
            if (!sBunch.isHashed() && sBunch.size() == MAX_ARRAY_BUNCH_SIZE_SUBJECT) {
                sBunch = new FastHashedTripleBunch(sBunch);
                subjects.put(triple.getSubject(), sBunch);
            }
            added = sBunch.tryAdd(triple, hashCodeOfTriple);
        }
        if (added) {
            size++;
            var pBunch = predicates.computeIfAbsent(triple.getPredicate(), () -> new ArrayBunchWithSamePredicate());
            if (!pBunch.isHashed() && pBunch.size() == MAX_ARRAY_BUNCH_SIZE_PREDICATE_OBJECT) {
                pBunch = new FastHashedTripleBunch(pBunch);
                predicates.put(triple.getPredicate(), pBunch);
            }
            pBunch.addUnchecked(triple, hashCodeOfTriple);
            var oBunch = objects.computeIfAbsent(triple.getObject(), () -> new ArrayBunchWithSameObject());
            if (!oBunch.isHashed() && oBunch.size() == MAX_ARRAY_BUNCH_SIZE_PREDICATE_OBJECT) {
                oBunch = new FastHashedTripleBunch(oBunch);
                objects.put(triple.getObject(), oBunch);
            }
            oBunch.addUnchecked(triple, hashCodeOfTriple);
        }
    }

    @Override
    public void remove(Triple triple) {
        final int hashCodeOfTriple = triple.hashCode();
        final var sBunch = subjects.get(triple.getSubject());
        if (sBunch == null)
            return;

        if (sBunch.tryRemove(triple, hashCodeOfTriple)) {
            if (sBunch.isEmpty()) {
                subjects.removeUnchecked(triple.getSubject());
            }
            final var pBunch = predicates.get(triple.getPredicate());
            pBunch.removeUnchecked(triple, hashCodeOfTriple);
            if (pBunch.isEmpty()) {
                predicates.removeUnchecked(triple.getPredicate());
            }
            final var oBunch = objects.get(triple.getObject());
            oBunch.removeUnchecked(triple, hashCodeOfTriple);
            if (oBunch.isEmpty()) {
                objects.removeUnchecked(triple.getObject());
            }
            size--;
        }
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
                final var triplesBySubject = subjects.get(tripleMatch.getSubject());
                if (triplesBySubject == null) {
                    return false;
                }
                return triplesBySubject.anyMatch(t -> tripleMatch.getPredicate().equals(t.getPredicate()));
            }

            case S_O: {
                final var triplesBySubject = subjects.get(tripleMatch.getSubject());
                if (triplesBySubject == null) {
                    return false;
                }
                return triplesBySubject.anyMatch(t -> tripleMatch.getObject().equals(t.getObject()));
            }

            case S__:
                return subjects.containsKey(tripleMatch.getSubject());

            case _PO: {
                final var triplesByObject = objects.get(tripleMatch.getObject());
                if (triplesByObject == null) {
                    return false;
                }
                // Optimization for typical RDF data, where there may be common values like "0" or "false"/"true".
                // In this case, there may be many matches but due to the ordered nature of FastHashBase,
                // the same predicates are often grouped together. If they are at the beginning of the bunch,
                // we can avoid the linear scan of the bunch. This is a common case for RDF data.
                // #anyMatchRandomOrder is a bit slower if the predicate is not found than #anyMatch, but not by much.
                if (triplesByObject.size() > THRESHOLD_FOR_SECONDARY_LOOKUP) {
                    final var triplesByPredicate = predicates.get(tripleMatch.getPredicate());
                    if (triplesByPredicate == null) {
                        return false;
                    }
                    if (triplesByPredicate.size() < triplesByObject.size()) {
                        return triplesByPredicate.anyMatchRandomOrder(t -> tripleMatch.getObject().equals(t.getObject()));
                    }
                }
                return triplesByObject.anyMatchRandomOrder(t -> tripleMatch.getPredicate().equals(t.getPredicate()));
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
        return subjects.valueStream().flatMap(FastTripleBunch::keyStream);
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
                final var triplesBySubject = subjects.get(tripleMatch.getSubject());
                if (triplesBySubject == null) {
                    return Stream.empty();
                }
                return triplesBySubject.keyStream().filter(t -> tripleMatch.getPredicate().equals(t.getPredicate()));
            }

            case S_O: {
                final var triplesBySubject = subjects.get(tripleMatch.getSubject());
                if (triplesBySubject == null) {
                    return Stream.empty();
                }
                return triplesBySubject.keyStream().filter(t -> tripleMatch.getObject().equals(t.getObject()));
            }

            case S__: {
                final var triples = subjects.get(tripleMatch.getSubject());
                return triples == null ? Stream.empty() : triples.keyStream();
            }

            case _PO: {
                final var triplesByObject = objects.get(tripleMatch.getObject());
                if (triplesByObject == null) {
                    return Stream.empty();
                }
                if (triplesByObject.size() > THRESHOLD_FOR_SECONDARY_LOOKUP) {
                    final var triplesByPredicate = predicates.get(tripleMatch.getPredicate());
                    if (triplesByPredicate == null) {
                        return Stream.empty();
                    }
                    if (triplesByPredicate.size() < triplesByObject.size()) {
                        return triplesByPredicate.keyStream().filter(t -> tripleMatch.getObject().equals(t.getObject()));
                    }
                }
                return triplesByObject.keyStream().filter(t -> tripleMatch.getPredicate().equals(t.getPredicate()));
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
                final var triplesBySubject = subjects.get(tripleMatch.getSubject());
                if (triplesBySubject == null) {
                    return NullIterator.emptyIterator();
                }
                return triplesBySubject.keyIterator().filterKeep(t -> tripleMatch.getPredicate().equals(t.getPredicate()));
            }

            case S_O: {
                final var triplesBySubject = subjects.get(tripleMatch.getSubject());
                if (triplesBySubject == null) {
                    return NullIterator.emptyIterator();
                }
                return triplesBySubject.keyIterator().filterKeep(t -> tripleMatch.getObject().equals(t.getObject()));
            }

            case S__: {
                final var triples = subjects.get(tripleMatch.getSubject());
                return triples == null ? NullIterator.emptyIterator() : triples.keyIterator();
            }

            case _PO: {
                final var triplesByObject = objects.get(tripleMatch.getObject());
                if (triplesByObject == null) {
                    return NullIterator.emptyIterator();
                }
                if (triplesByObject.size() > THRESHOLD_FOR_SECONDARY_LOOKUP) {
                    final var triplesByPredicate = predicates.get(tripleMatch.getPredicate());
                    if (triplesByPredicate == null) {
                        return NullIterator.emptyIterator();
                    }
                    if (triplesByPredicate.size() < triplesByObject.size()) {
                        return triplesByPredicate.keyIterator().filterKeep(t -> tripleMatch.getObject().equals(t.getObject()));
                    }
                }
                return triplesByObject.keyIterator().filterKeep(t -> tripleMatch.getPredicate().equals(t.getPredicate()));
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
                return new NestedIterator<>(subjects.valueIterator(), FastTripleBunch::keyIterator);

            default:
                throw new IllegalStateException("Unexpected value: " + PatternClassifier.classify(tripleMatch));
        }
    }

    private static class ArrayBunchWithSameSubject extends FastArrayBunch {

        @Override
        public boolean areEqual(final Triple a, final Triple b) {
            return a.getPredicate().equals(b.getPredicate())
                    && a.getObject().equals(b.getObject());
        }
    }

    private static class ArrayBunchWithSamePredicate extends FastArrayBunch {
        @Override
        public boolean areEqual(final Triple a, final Triple b) {
            return a.getSubject().equals(b.getSubject())
                    && a.getObject().equals(b.getObject());
        }
    }

    private static class ArrayBunchWithSameObject extends FastArrayBunch {
        @Override
        public boolean areEqual(final Triple a, final Triple b) {
            return a.getSubject().equals(b.getSubject())
                    && a.getPredicate().equals(b.getPredicate());
        }
    }
}
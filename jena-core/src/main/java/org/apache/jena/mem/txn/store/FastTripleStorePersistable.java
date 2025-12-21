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
package org.apache.jena.mem.txn.store;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.iterator.IteratorOfJenaSets;
import org.apache.jena.mem.pattern.PatternClassifier;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.SingletonIterator;

import java.util.ArrayList;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class FastTripleStorePersistable implements TripleStorePersistable {

    protected static final int THRESHOLD_FOR_SECONDARY_LOOKUP = 400;
    protected static final int MAX_ARRAY_BUNCH_SIZE_SUBJECT = 16;
    protected static final int MAX_ARRAY_BUNCH_SIZE_PREDICATE_OBJECT = 32;
    final FastHashedBunchMapPersistable subjects;
    final FastHashedBunchMapPersistable predicates;
    final FastHashedBunchMapPersistable objects;
    protected ArrayList<Mutations> subjectsMutations = new ArrayList<>();
    protected ArrayList<Mutations> predicatesMutations = new ArrayList<>();
    protected ArrayList<Mutations> objectsMutations = new ArrayList<>();
    private int size = 0;

    public FastTripleStorePersistable() {
        subjects = new FastHashedBunchMapPersistable();
        predicates = new FastHashedBunchMapPersistable();
        objects = new FastHashedBunchMapPersistable();
    }

    protected FastTripleStorePersistable(final FastTripleStorePersistable base, boolean createImmutableChild) {
        if(createImmutableChild) {
            subjects = base.subjects.createImmutableChild();
            predicates = base.predicates.createImmutableChild();
            objects = base.objects.createImmutableChild();
        } else {
            subjects = base.subjects.copy();
            predicates = base.predicates.copy();
            objects = base.objects.copy();
        }
        size = base.size;
    }

    protected record Mutations(Node node, int hashCodeOfNode, FastTripleBunchPersistable mutableTripleBunch) {}

    @Override
    public void add(Triple triple) {
        final var sHashCode = triple.getSubject().hashCode();
        final var pHashCode = triple.getPredicate().hashCode();
        final var oHashCode = triple.getObject().hashCode();
        final var hashCodeOfTriple = Triple.hashCode(sHashCode, pHashCode, oHashCode);
        final boolean added;

        var sBunch = subjects.get(triple.getSubject(), sHashCode);
        if (sBunch == null) {
            sBunch = new ArrayBunchWithSameSubject();
            sBunch.addUnchecked(triple);
            subjects.put(triple.getSubject(), sHashCode, sBunch);
            added = true;
        } else {
            if(sBunch.isImmutable()) {
                sBunch = sBunch.getMutableParentBunch();
                subjects.put(triple.getSubject(), sHashCode, sBunch);
            }
            if (sBunch.isArray() && sBunch.size() == MAX_ARRAY_BUNCH_SIZE_SUBJECT) {
                sBunch = new FastHashedTripleBunchPersistable(sBunch);
                subjects.put(triple.getSubject(), sHashCode, sBunch);
            }
            added = sBunch.tryAdd(triple, hashCodeOfTriple);
        }
        if (added) {
            this.subjectsMutations.add(new Mutations(triple.getSubject(), sHashCode, sBunch));
            {
                var pBunch = predicates.get(triple.getPredicate(), pHashCode);
                if (pBunch == null) {
                    pBunch = new ArrayBunchWithSamePredicate();
                    pBunch.addUnchecked(triple);
                    predicates.put(triple.getPredicate(), pHashCode, pBunch);
                } else {
                    if (pBunch.isImmutable()) {
                        pBunch = pBunch.getMutableParentBunch();
                        predicates.put(triple.getPredicate(), pHashCode, pBunch);
                    }
                    if (pBunch.isArray() && pBunch.size() == MAX_ARRAY_BUNCH_SIZE_PREDICATE_OBJECT) {
                        pBunch = new FastHashedTripleBunchPersistable(pBunch);
                        predicates.put(triple.getPredicate(), pHashCode, pBunch);
                    }
                    pBunch.addUnchecked(triple, hashCodeOfTriple);
                }
                this.predicatesMutations.add(new Mutations(triple.getPredicate(), pHashCode, pBunch));
            }
            {
                var oBunch = objects.get(triple.getObject(), oHashCode);
                if (oBunch == null) {
                    oBunch = new ArrayBunchWithSameObject();
                    oBunch.addUnchecked(triple);
                    objects.put(triple.getObject(), oHashCode, oBunch);
                } else {
                    if (oBunch.isImmutable()) {
                        oBunch = oBunch.getMutableParentBunch();
                        objects.put(triple.getObject(), oHashCode, oBunch);
                    }
                    if (oBunch.isArray() && oBunch.size() == MAX_ARRAY_BUNCH_SIZE_PREDICATE_OBJECT) {
                        oBunch = new FastHashedTripleBunchPersistable(oBunch);
                        objects.put(triple.getObject(), oHashCode, oBunch);
                    }
                    oBunch.addUnchecked(triple, hashCodeOfTriple);
                }
                this.objectsMutations.add(new Mutations(triple.getObject(), oHashCode, oBunch));
            }
            size++;
        }
    }

    @Override
    public void remove(Triple triple) {
        final var sHashCode = triple.getSubject().hashCode();

        var sBunch = subjects.get(triple.getSubject(), sHashCode);
        if (sBunch == null)
            return;

        final var pHashCode = triple.getPredicate().hashCode();
        final var oHashCode = triple.getObject().hashCode();
        final var hashCodeOfTriple = Triple.hashCode(sHashCode, pHashCode, oHashCode);

        if (sBunch.isImmutable()) {
            sBunch = sBunch.getMutableParentBunch();
        }

        if (sBunch.tryRemove(triple, hashCodeOfTriple)) {
            if (sBunch.isEmpty()) {
                subjects.removeUnchecked(triple.getSubject(), sHashCode);
            } else {
                subjects.put(triple.getSubject(), sHashCode, sBunch);
                subjectsMutations.add(new Mutations(triple.getSubject(), sHashCode, sBunch));
            }
            var pBunch = predicates.get(triple.getPredicate());
            if(pBunch.isImmutable()) {
                pBunch = pBunch.getMutableParentBunch();
            }
            pBunch.removeUnchecked(triple, hashCodeOfTriple);
            if (pBunch.isEmpty()) {
                predicates.removeUnchecked(triple.getPredicate(), pHashCode);
            } else {
                predicates.put(triple.getPredicate(), pHashCode, pBunch);
                predicatesMutations.add(new Mutations(triple.getPredicate(), pHashCode, pBunch));
            }
            var oBunch = objects.get(triple.getObject());
            if(oBunch.isImmutable()) {
                oBunch = oBunch.getMutableParentBunch();
            }
            oBunch.removeUnchecked(triple, hashCodeOfTriple);
            if (oBunch.isEmpty()) {
                objects.removeUnchecked(triple.getObject(), oHashCode);
            } else {
                objects.put(triple.getObject(), oHashCode, oBunch);
                objectsMutations.add(new Mutations(triple.getObject(), oHashCode, oBunch));
            }
            size--;
        }
    }

    @Override
    public void clear() {
        subjects.clear();
        predicates.clear();
        objects.clear();
        subjectsMutations = new ArrayList<>();
        predicatesMutations = new ArrayList<>();
        objectsMutations = new ArrayList<>();
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

            case SUB_PRE_OBJ: {
                final var triples = subjects.get(tripleMatch.getSubject());
                if (triples == null) {
                    return false;
                }
                return triples.containsKey(tripleMatch);
            }

            case SUB_PRE_ANY: {
                final var triplesBySubject = subjects.get(tripleMatch.getSubject());
                if (triplesBySubject == null) {
                    return false;
                }
                return triplesBySubject.anyMatch(t -> tripleMatch.getPredicate().equals(t.getPredicate()));
            }

            case SUB_ANY_OBJ: {
                final var triplesBySubject = subjects.get(tripleMatch.getSubject());
                if (triplesBySubject == null) {
                    return false;
                }
                return triplesBySubject.anyMatch(t -> tripleMatch.getObject().equals(t.getObject()));
            }

            case SUB_ANY_ANY:
                return subjects.containsKey(tripleMatch.getSubject());

            case ANY_PRE_OBJ: {
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

            case ANY_PRE_ANY:
                return predicates.containsKey(tripleMatch.getPredicate());

            case ANY_ANY_OBJ:
                return objects.containsKey(tripleMatch.getObject());

            case ANY_ANY_ANY:
                return !isEmpty();

            default:
                throw new IllegalStateException(String.format("Unexpected value: %s", PatternClassifier.classify(tripleMatch)));
        }
    }

    @Override
    public Stream<Triple> stream() {
        return StreamSupport.stream(subjects.valueSpliterator(), false)
                .flatMap(bunch -> StreamSupport.stream(bunch.keySpliterator(), false));
    }

    @Override
    public Stream<Triple> stream(Triple tripleMatch) {
        switch (PatternClassifier.classify(tripleMatch)) {

            case SUB_PRE_OBJ: {
                final var triples = subjects.get(tripleMatch.getSubject());
                if (triples == null) {
                    return Stream.empty();
                }
                return triples.containsKey(tripleMatch) ? Stream.of(tripleMatch) : Stream.empty();
            }

            case SUB_PRE_ANY: {
                final var triplesBySubject = subjects.get(tripleMatch.getSubject());
                if (triplesBySubject == null) {
                    return Stream.empty();
                }
                return triplesBySubject.keyStream().filter(t -> tripleMatch.getPredicate().equals(t.getPredicate()));
            }

            case SUB_ANY_OBJ: {
                final var triplesBySubject = subjects.get(tripleMatch.getSubject());
                if (triplesBySubject == null) {
                    return Stream.empty();
                }
                return triplesBySubject.keyStream().filter(t -> tripleMatch.getObject().equals(t.getObject()));
            }

            case SUB_ANY_ANY: {
                final var triples = subjects.get(tripleMatch.getSubject());
                return triples == null ? Stream.empty() : triples.keyStream();
            }

            case ANY_PRE_OBJ: {
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

            case ANY_PRE_ANY: {
                final var triples = predicates.get(tripleMatch.getPredicate());
                return triples == null ? Stream.empty() : triples.keyStream();
            }

            case ANY_ANY_OBJ: {
                final var triples = objects.get(tripleMatch.getObject());
                return triples == null ? Stream.empty() : triples.keyStream();
            }

            case ANY_ANY_ANY:
                return stream();

            default:
                throw new IllegalStateException("Unexpected value: " + PatternClassifier.classify(tripleMatch));
        }
    }

    @Override
    public ExtendedIterator<Triple> find(Triple tripleMatch) {
        switch (PatternClassifier.classify(tripleMatch)) {

            case SUB_PRE_OBJ: {
                final var triples = subjects.get(tripleMatch.getSubject());
                if (triples == null) {
                    return NiceIterator.emptyIterator();
                }
                return triples.containsKey(tripleMatch) ? new SingletonIterator<>(tripleMatch) : NiceIterator.emptyIterator();
            }

            case SUB_PRE_ANY: {
                final var triplesBySubject = subjects.get(tripleMatch.getSubject());
                if (triplesBySubject == null) {
                    return NiceIterator.emptyIterator();
                }
                return triplesBySubject.keyIterator().filterKeep(t -> tripleMatch.getPredicate().equals(t.getPredicate()));
            }

            case SUB_ANY_OBJ: {
                final var triplesBySubject = subjects.get(tripleMatch.getSubject());
                if (triplesBySubject == null) {
                    return NiceIterator.emptyIterator();
                }
                return triplesBySubject.keyIterator().filterKeep(t -> tripleMatch.getObject().equals(t.getObject()));
            }

            case SUB_ANY_ANY: {
                final var triples = subjects.get(tripleMatch.getSubject());
                return triples == null ? NiceIterator.emptyIterator() : triples.keyIterator();
            }

            case ANY_PRE_OBJ: {
                final var triplesByObject = objects.get(tripleMatch.getObject());
                if (triplesByObject == null) {
                    return NiceIterator.emptyIterator();
                }
                if (triplesByObject.size() > THRESHOLD_FOR_SECONDARY_LOOKUP) {
                    final var triplesByPredicate = predicates.get(tripleMatch.getPredicate());
                    if (triplesByPredicate == null) {
                        return NiceIterator.emptyIterator();
                    }
                    if (triplesByPredicate.size() < triplesByObject.size()) {
                        return triplesByPredicate.keyIterator().filterKeep(t -> tripleMatch.getObject().equals(t.getObject()));
                    }
                }
                return triplesByObject.keyIterator().filterKeep(t -> tripleMatch.getPredicate().equals(t.getPredicate()));
            }

            case ANY_PRE_ANY: {
                final var triples = predicates.get(tripleMatch.getPredicate());
                return triples == null ? NiceIterator.emptyIterator() : triples.keyIterator();
            }

            case ANY_ANY_OBJ: {
                final var triples = objects.get(tripleMatch.getObject());
                return triples == null ? NiceIterator.emptyIterator() : triples.keyIterator();
            }

            case ANY_ANY_ANY:
                return new IteratorOfJenaSets<>(subjects.valueIterator());

            default:
                throw new IllegalStateException("Unexpected value: " + PatternClassifier.classify(tripleMatch));
        }
    }

    @Override
    public FastTripleStorePersistable copy() {
        return new FastTripleStorePersistable(this, false);
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public FastTripleStorePersistable getMutableParent() {
        throw new UnsupportedOperationException("This map is already mutable");
    }

    @Override
    public FastTripleStorePersistableImmutable createImmutableChild() {
        for(var mut : subjectsMutations) {
            subjects.put(mut.node, mut.hashCodeOfNode, mut.mutableTripleBunch.createImmutableChildBunch());
        }
        for(var mut : predicatesMutations) {
            predicates.put(mut.node, mut.hashCodeOfNode, mut.mutableTripleBunch.createImmutableChildBunch());
        }
        for(var mut : objectsMutations) {
            objects.put(mut.node, mut.hashCodeOfNode, mut.mutableTripleBunch.createImmutableChildBunch());
        }
        subjectsMutations = new ArrayList<>();
        predicatesMutations = new ArrayList<>();
        objectsMutations = new ArrayList<>();
        return new FastTripleStorePersistableImmutable(this);
    }

    protected static class ArrayBunchWithSameSubject extends FastArrayBunchPersistable {

        public ArrayBunchWithSameSubject() {
            super();
        }

        private ArrayBunchWithSameSubject(ArrayBunchWithSameSubject bunchToCopy) {
            super(bunchToCopy);
        }

        @Override
        public ArrayBunchWithSameSubject copy() {
            return new ArrayBunchWithSameSubject(this);
        }

        @Override
        public boolean areEqual(final Triple a, final Triple b) {
            return a.getPredicate().equals(b.getPredicate())
                    && a.getObject().equals(b.getObject());
        }

        @Override
        public ArrayBunchImmutableWithSameSubject createImmutableChildBunch() {
            return new ArrayBunchImmutableWithSameSubject(this);
        }
    }

    protected static class ArrayBunchImmutableWithSameSubject extends FastArrayBunchPersistableImmutable {

        protected ArrayBunchImmutableWithSameSubject(FastArrayBunchPersistable bunchToCopy) {
            super(bunchToCopy);
        }

        @Override
        public ArrayBunchImmutableWithSameSubject copy() {
            return new ArrayBunchImmutableWithSameSubject(this);
        }

        @Override
        public boolean areEqual(final Triple a, final Triple b) {
            return a.getPredicate().equals(b.getPredicate())
                    && a.getObject().equals(b.getObject());
        }

        @Override
        public ArrayBunchImmutableWithSameSubject createImmutableChildBunch() {
            throw new UnsupportedOperationException("This bunch is already immutable");
        }
    }


    protected static class ArrayBunchWithSamePredicate extends FastArrayBunchPersistable {

        public ArrayBunchWithSamePredicate() {
            super();
        }

        private ArrayBunchWithSamePredicate(ArrayBunchWithSamePredicate bunchToCopy) {
            super(bunchToCopy);
        }

        @Override
        public ArrayBunchWithSamePredicate copy() {
            return new ArrayBunchWithSamePredicate(this);
        }

        @Override
        public boolean areEqual(final Triple a, final Triple b) {
            return a.getSubject().equals(b.getSubject())
                    && a.getObject().equals(b.getObject());
        }

        @Override
        public ArrayBunchImmutableWithSamePredicate createImmutableChildBunch() {
            return new ArrayBunchImmutableWithSamePredicate(this);
        }
    }

    protected static class ArrayBunchImmutableWithSamePredicate extends FastArrayBunchPersistableImmutable {

        protected ArrayBunchImmutableWithSamePredicate(FastArrayBunchPersistable bunchToCopy) {
            super(bunchToCopy);
        }

        @Override
        public ArrayBunchImmutableWithSamePredicate copy() {
            return new ArrayBunchImmutableWithSamePredicate(this);
        }

        @Override
        public boolean areEqual(final Triple a, final Triple b) {
            return a.getSubject().equals(b.getSubject())
                    && a.getObject().equals(b.getObject());
        }

        @Override
        public ArrayBunchImmutableWithSamePredicate createImmutableChildBunch() {
            throw new UnsupportedOperationException("This bunch is already immutable");
        }
    }

    protected static class ArrayBunchWithSameObject extends FastArrayBunchPersistable {

        public ArrayBunchWithSameObject() {
            super();
        }

        private ArrayBunchWithSameObject(ArrayBunchWithSameObject bunchToCopy) {
            super(bunchToCopy);
        }

        @Override
        public ArrayBunchWithSameObject copy() {
            return new ArrayBunchWithSameObject(this);
        }

        @Override
        public boolean areEqual(final Triple a, final Triple b) {
            return a.getSubject().equals(b.getSubject())
                    && a.getPredicate().equals(b.getPredicate());
        }

        @Override
        public ArrayBunchImmutableWithSameObject createImmutableChildBunch() {
            return new ArrayBunchImmutableWithSameObject(this);
        }
    }

    protected static class ArrayBunchImmutableWithSameObject extends FastArrayBunchPersistableImmutable {

        protected ArrayBunchImmutableWithSameObject(FastArrayBunchPersistable bunchToCopy) {
            super(bunchToCopy);
        }

        @Override
        public ArrayBunchImmutableWithSameObject copy() {
            return new ArrayBunchImmutableWithSameObject(this);
        }

        @Override
        public boolean areEqual(final Triple a, final Triple b) {
            return a.getSubject().equals(b.getSubject())
                    && a.getPredicate().equals(b.getPredicate());
        }

        @Override
        public ArrayBunchImmutableWithSameObject createImmutableChildBunch() {
            throw new UnsupportedOperationException("This bunch is already immutable");
        }
    }
}

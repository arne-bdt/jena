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

package org.apache.jena.mem2.store.adaptive.base;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.iterator.NestedIterator;
import org.apache.jena.mem2.specialized.FastHashMapBase;
import org.apache.jena.mem2.store.adaptive.QueryableTripleSet;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.function.Consumer;
import java.util.stream.Stream;

public abstract class MapOfIndexedSetsBase extends FastHashMapBase<QueryableTripleSet> implements QueryableTripleSet {

    public MapOfIndexedSetsBase(int minCapacity) {
        super(minCapacity);
    }

    @Override
    protected QueryableTripleSet[] createEntryArray(int length) {
        return new QueryableTripleSet[length];
    }

    protected abstract QueryableTripleSet createEntry(Consumer<QueryableTripleSet> transitionConsumer);

    protected abstract Node getIndexingNode(Triple tripleMatch);

    protected abstract int getHashCodeOfIndexingValue(Triple triple);

    @Override
    public int countTriples() {
        return super.stream().mapToInt(QueryableTripleSet::countTriples).sum();
    }

    @Override
    public int countIndexSize() {
        return this.size() + super.stream().mapToInt(QueryableTripleSet::countIndexSize).sum();
    }

    private boolean entryTransitioned = false;
    private QueryableTripleSet transitionedEntry = null;

    private Consumer<QueryableTripleSet> entryTransition = (QueryableTripleSet transitionedEntry) -> {
        this.transitionedEntry = transitionedEntry;
        this.entryTransitioned = true;
    };

    @Override
    public boolean addTriple(final Triple triple, final int hashCode) {
        boolean added[] = {true};
        this.compute(getHashCodeOfIndexingValue(triple), (set) -> {
            if (set == null) {
                set = createEntry(this.entryTransition);
                set.addTripleUnchecked(triple, hashCode);
                return set;
            }
            added[0] = set.addTriple(triple, hashCode);
            if(this.entryTransitioned) {
                this.entryTransitioned = false;
                return this.transitionedEntry;
            }
            return set;
        });
        return added[0];
    }

    @Override
    public void addTripleUnchecked(final Triple triple, final int hashCode) {
        super.compute(getHashCodeOfIndexingValue(triple), (set) -> {
            if (set == null) {
                set = createEntry(this.entryTransition);
                set.addTripleUnchecked(triple, hashCode);
                return set;
            }
            set.addTripleUnchecked(triple, hashCode);
            if(this.entryTransitioned) {
                this.entryTransitioned = false;
                return this.transitionedEntry;
            }
            return set;
        });
    }

    @Override
    public boolean removeTriple(final Triple triple, final int hashCode) {
        boolean tripleRemoved[] = {false};
        this.compute(getHashCodeOfIndexingValue(triple), (set) -> {
            if (set == null) {
                return null;
            }
            if(tripleRemoved[0] = set.removeTriple(triple, hashCode) && set.isEmpty()) {
                return null;
            }
            return set;
        });
        return tripleRemoved[0];
    }

    @Override
    public void removeTripleUnchecked(final Triple triple, final int hashCode) {
        this.compute(getHashCodeOfIndexingValue(triple), (set) -> {
            set.removeTripleUnchecked(triple, hashCode);
            if(set.isEmpty()) {
                return null;
            }
            return set;
        });
    }

    @Override
    public boolean containsMatch(final Triple tripleMatch) {
        var set = super.getIfPresent(getHashCodeOfIndexingValue(tripleMatch));
        if(set == null) {
            if(getIndexingNode(tripleMatch).isConcrete()) {
                return false;
            }
            return stream().anyMatch(s -> s.containsMatch(tripleMatch));
        }
        return set.containsMatch(tripleMatch);
    }

    @Override
    public Stream<Triple> streamTriples(final Triple tripleMatch) {
        var set = super.getIfPresent(getHashCodeOfIndexingValue(tripleMatch));
        if(set == null) {
            if(getIndexingNode(tripleMatch).isConcrete()) {
                return Stream.empty();
            }
            return stream().flatMap(s -> s.streamTriples(tripleMatch));
        }
        return set.streamTriples(tripleMatch);
    }

    @Override
    public ExtendedIterator<Triple> findTriples(final Triple tripleMatch, final Graph graphForIteratorRemove) {
        var set = super.getIfPresent(getHashCodeOfIndexingValue(tripleMatch));
        if(set == null) {
            return NiceIterator.emptyIterator();
        }
        return set.findTriples(tripleMatch, graphForIteratorRemove);
    }

    @Override
    public ExtendedIterator<Triple> findAll(Graph graphForIteratorRemove) {
        return new NestedIterator<>(super.iterator(), (set) -> set.findAll(graphForIteratorRemove));
    }

    @Override
    public Stream<Triple> streamTriples() {
        return super.stream().flatMap(QueryableTripleSet::streamTriples);
    }

}

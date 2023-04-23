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

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.specialized.FastHashMapBase;
import org.apache.jena.mem2.store.adaptive.QueryableTripleSet;
import org.apache.jena.mem2.store.adaptive.TripleWithIndexingHashCodes;

import java.util.Collections;
import java.util.Iterator;
import java.util.stream.Stream;

public abstract class MapOfIndexedSetsBase extends FastHashMapBase<QueryableTripleSet> implements QueryableTripleSet {

    public MapOfIndexedSetsBase(QueryableTripleSet tripleSet) {
        super();
        tripleSet.streamTriples().forEach(t -> this.addTripleUnchecked(new TripleWithIndexingHashCodes(t)));
    }

    @Override
    protected QueryableTripleSet[] createEntryArray(int length) {
        return new QueryableTripleSet[length];
    }

    protected abstract QueryableTripleSet createEntry();

    protected abstract Node getIndexingNode(Triple tripleMatch);

    protected abstract int getHashCodeOfIndexingValue(TripleWithIndexingHashCodes triple);

    @Override
    public int countTriples() {
        return super.stream().mapToInt(QueryableTripleSet::countTriples).sum();
    }

    @Override
    public int indexSize() {
        return this.size();
    }

    @Override
    public QueryableTripleSet addTriple(TripleWithIndexingHashCodes triple) {
        boolean tripleExists[] = {true};
        this.compute(getHashCodeOfIndexingValue(triple), (set) -> {
            if (set == null) {
                set = createEntry();
                return set.addTripleUnchecked(triple);
            }
            var newSet = set.addTriple(triple);
            if(newSet == null) {
                tripleExists[0] = false;
                return set;
            }
            return newSet;
        });
        return tripleExists[0] ? this : null;
    }

    @Override
    public QueryableTripleSet addTripleUnchecked(TripleWithIndexingHashCodes triple) {
        super.compute(getHashCodeOfIndexingValue(triple), (set) -> {
            if (set == null) {
                set = createEntry();
                return set.addTripleUnchecked(triple);
            }
            return set.addTripleUnchecked(triple);
        });
        return this;
    }

    @Override
    public boolean removeTriple(TripleWithIndexingHashCodes triple) {
        boolean tripleRemoved[] = {false};
        this.compute(getHashCodeOfIndexingValue(triple), (set) -> {
            if (set == null) {
                return null;
            }
            if(tripleRemoved[0] = set.removeTriple(triple) && 0 == set.indexSize()) {
                return null;
            }
            return set;
        });
        return tripleRemoved[0];
    }

    @Override
    public void removeTripleUnchecked(TripleWithIndexingHashCodes triple) {
        this.compute(getHashCodeOfIndexingValue(triple), (set) -> {
            set.removeTripleUnchecked(triple);
            if(0 == set.indexSize()) {
                return null;
            }
            return set;
        });
    }

    @Override
    public boolean containsTriple(TripleWithIndexingHashCodes concreteTriple) {
        var set = super.getIfPresent(getHashCodeOfIndexingValue(concreteTriple));
        if(set == null) {
            return false;
        }
        return set.containsTriple(concreteTriple);
    }

    @Override
    public boolean containsMatch(TripleWithIndexingHashCodes tripleMatch) {
        var set = super.getIfPresent(getHashCodeOfIndexingValue(tripleMatch));
        if(set == null) {
            if(getIndexingNode(tripleMatch.getTriple()).isConcrete()) {
                return false;
            }
            return stream().anyMatch(s -> s.containsMatch(tripleMatch));
        }
        return set.containsMatch(tripleMatch);
    }

    @Override
    public Stream<Triple> streamTriples(TripleWithIndexingHashCodes tripleMatch) {
        var set = super.getIfPresent(getHashCodeOfIndexingValue(tripleMatch));
        if(set == null) {
            if(getIndexingNode(tripleMatch.getTriple()).isConcrete()) {
                return Stream.empty();
            }
            return stream().flatMap(s -> s.streamTriples(tripleMatch));
        }
        return set.streamTriples(tripleMatch);
    }

    @Override
    public Iterator<Triple> findTriples(TripleWithIndexingHashCodes tripleMatch) {
        var set = super.getIfPresent(getHashCodeOfIndexingValue(tripleMatch));
        if(set == null) {
            if(getIndexingNode(tripleMatch.getTriple()).isConcrete()) {
                return Collections.emptyIterator();
            }
            return stream().flatMap(s -> s.streamTriples(tripleMatch)).iterator();
        }
        return set.findTriples(tripleMatch);
    }

    @Override
    public Stream<Triple> streamTriples() {
        return super.stream().flatMap(QueryableTripleSet::streamTriples);
    }
}

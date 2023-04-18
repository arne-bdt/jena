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

package org.apache.jena.mem2.store;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.TripleWithIndexingHashCodes;
import org.apache.jena.mem2.map.FastHashSetBase;
import org.apache.jena.mem2.map.IntIntSetBase;
import org.apache.jena.mem2.map.TriplesMapWithOneIndex;
import org.apache.jena.mem2.map.TriplesMapWithOneIndexUsingFastHashSets;
import org.apache.jena.mem2.pattern.PatternClassifier;

import java.util.Iterator;
import java.util.stream.Stream;

public class TripleStoreUsingSingleSetAndIndices implements TripleStore {

    private static class TripleSet extends FastHashSetBase<TripleWithIndexingHashCodes> {

        @Override
        protected TripleWithIndexingHashCodes[] createEntryArray(int length) {
            return new TripleWithIndexingHashCodes[length];
        }
    }
    final TripleSet triples = new TripleSet();
    final IntIntSetBase spo = new IntIntSetBase();
    final IntIntSetBase pos = new IntIntSetBase();
    final IntIntSetBase osp = new IntIntSetBase();

    @Override
    public void add(Triple triple) {
        var th = new TripleWithIndexingHashCodes(triple);
        if(triples.add(th, th.hashCode())) {
            this.spo.addWithoutChecking(th.getSubjectIndexingHashCode(), th.hashCode());
            this.pos.addWithoutChecking(th.getPredicateIndexingHashCode(), th.hashCode());
            this.osp.addWithoutChecking(th.getObjectIndexingHashCode(), th.hashCode());
        }
    }

    @Override
    public void remove(Triple triple) {
        var th = new TripleWithIndexingHashCodes(triple);
        if(triples.remove(th, th.hashCode())) {
            this.spo.removeWithoutChecking(th.getSubjectIndexingHashCode(), th.hashCode());
            this.pos.removeWithoutChecking(th.getPredicateIndexingHashCode(), th.hashCode());
            this.osp.removeWithoutChecking(th.getObjectIndexingHashCode(), th.hashCode());
        }
    }

    @Override
    public void clear() {
        this.triples.clear();
        this.spo.clear();
        this.pos.clear();
        this.osp.clear();
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
        return this.stream(tripleMatch.getSubject(), tripleMatch.getPredicate(), tripleMatch.getObject())
                .findAny().isPresent();
    }

    @Override
    public Stream<Triple> stream() {
        return this.triples.stream().map(TripleWithIndexingHashCodes::getTriple);
    }

    @Override
    public Stream<Triple> stream(Node sm, Node pm, Node om) {
        int pIndex, oIndex;
        switch (PatternClassifier.classify(sm, pm, om)) {
            case SPO:
                pIndex = pm.getIndexingValue().hashCode();
                oIndex = om.getIndexingValue().hashCode();
                return this.spo.streamAllWithSameHashCode(
                                sm.getIndexingValue().hashCode()).map(tripleHashCode ->
                                    triples.getIfPresent(tripleHashCode)
                                ).filter(th ->
                                th.getPredicateIndexingHashCode() == pIndex
                                && th.getObjectIndexingHashCode() == oIndex)
                        .map(TripleWithIndexingHashCodes::getTriple)
                        .filter(triple ->
                                pm.matches(triple.getPredicate())
                                && om.matches(triple.getObject())
                                && sm.matches(triple.getSubject()));
            case SP_:
                pIndex = pm.getIndexingValue().hashCode();
                return this.spo.streamAllWithSameHashCode(
                                sm.getIndexingValue().hashCode()).map(tripleHashCode ->
                                triples.getIfPresent(tripleHashCode)
                        ).filter(th ->
                                th.getPredicateIndexingHashCode() == pIndex)
                        .map(TripleWithIndexingHashCodes::getTriple)
                        .filter(triple ->
                                pm.matches(triple.getPredicate())
                                && sm.matches(triple.getSubject()));
            case S_O:
                oIndex = om.getIndexingValue().hashCode();
                return this.spo.streamAllWithSameHashCode(
                                sm.getIndexingValue().hashCode()).map(tripleHashCode ->
                                triples.getIfPresent(tripleHashCode)
                        ).filter(th ->
                                th.getObjectIndexingHashCode() == oIndex)
                        .map(TripleWithIndexingHashCodes::getTriple)
                        .filter(triple ->
                                om.matches(triple.getObject())
                                && sm.matches(triple.getSubject()));
            case S__:
                return this.spo.streamAllWithSameHashCode(
                                sm.getIndexingValue().hashCode()).map(tripleHashCode ->
                                triples.getIfPresent(tripleHashCode)
                        ).map(TripleWithIndexingHashCodes::getTriple)
                        .filter(triple -> sm.matches(triple.getSubject()));
            case _PO:
                oIndex = om.getIndexingValue().hashCode();
                return this.pos.streamAllWithSameHashCode(
                                pm.getIndexingValue().hashCode()).map(tripleHashCode ->
                                triples.getIfPresent(tripleHashCode)
                        ).filter(th ->
                                th.getObjectIndexingHashCode() == oIndex)
                        .map(TripleWithIndexingHashCodes::getTriple)
                        .filter(triple ->
                                pm.matches(triple.getPredicate())
                                        && om.matches(triple.getSubject()));
            case _P_:
                return this.pos.streamAllWithSameHashCode(
                                pm.getIndexingValue().hashCode()).map(tripleHashCode ->
                                triples.getIfPresent(tripleHashCode)
                        ).map(TripleWithIndexingHashCodes::getTriple)
                        .filter(triple -> pm.matches(triple.getPredicate()));
            case __O:
                return this.osp.streamAllWithSameHashCode(
                                om.getIndexingValue().hashCode()).map(tripleHashCode ->
                                triples.getIfPresent(tripleHashCode)
                        ).map(TripleWithIndexingHashCodes::getTriple)
                        .filter(triple -> om.matches(triple.getObject()));
            case ___:
                return this.triples.stream().map(TripleWithIndexingHashCodes::getTriple);
            default:
                throw new IllegalArgumentException("Unknown pattern: " + sm + " " + pm + " " + om);
        }
    }

    @Override
    public Iterator<Triple> find(Triple tripleMatch) {
        return this.stream(tripleMatch.getSubject(), tripleMatch.getPredicate(), tripleMatch.getObject())
                .iterator();
    }


    /**
     * Gets the map with the fewest top level nodes.
     * This map should be used to iterate over the triples or count them.
     * The basic idea is to reduce the number of nodes that need to be iterated over.
     * @return
     */
    private IntIntSetBase getMapWithFewestTopLevelIndices() {
        final var subjectIndicesCount = this.spo.size();
        final var predicateIndicesCount = this.pos.size();
        final var objectIndicesCount = this.osp.size();
        if(subjectIndicesCount < predicateIndicesCount) {
            if(subjectIndicesCount < objectIndicesCount) {
                return this.spo;
            } else {
                return this.osp;
            }
        } else {
            if(predicateIndicesCount < objectIndicesCount) {
                return this.pos;
            } else {
                return this.osp;
            }
        }
    }
}

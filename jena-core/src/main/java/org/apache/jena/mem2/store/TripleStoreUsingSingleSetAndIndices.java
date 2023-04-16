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
import org.apache.jena.mem2.map.FastHashSetBase;
import org.apache.jena.mem2.map.TriplesMapWithOneIndex;
import org.apache.jena.mem2.map.TriplesMapWithOneIndexUsingFastHashSets;
import org.apache.jena.mem2.pattern.PatternClassifier;

import java.util.Iterator;
import java.util.stream.Stream;

public class TripleStoreUsingSingleSetAndIndices implements TripleStore {

    private static class TripleSet extends FastHashSetBase<Triple> {

        @Override
        protected Triple[] createEntryArray(int length) {
            return new Triple[length];
        }
    }

    final TripleSet triples = new TripleSet();

    final TriplesMapWithOneIndex spo = new TriplesMapWithOneIndexUsingFastHashSets();
    final TriplesMapWithOneIndex pos = new TriplesMapWithOneIndexUsingFastHashSets();
    final TriplesMapWithOneIndex osp = new TriplesMapWithOneIndexUsingFastHashSets();

    @Override
    public void add(Triple triple) {
        var hashes = getHashCodes(triple);
        if(this.spo.add(hashes[1], hashes[0], triple)) { // if it was not already present, add it to the other two maps without checking
            this.pos.addWithoutChecking(hashes[2], hashes[0], triple);
            this.osp.addWithoutChecking(hashes[3], hashes[0], triple);
        }
    }

    @Override
    public void remove(Triple triple) {
        var hashes = getHashCodes(triple);
        if(this.spo.remove(hashes[1], hashes[0], triple)) { // if it exists in the fist map, it exists in the other two
            this.pos.removeWithoutChecking(hashes[2], hashes[0], triple);
            this.osp.removeWithoutChecking(hashes[3], hashes[0], triple);
        }
    }

    @Override
    public void clear() {
        this.spo.clear();
        this.pos.clear();
        this.osp.clear();
    }

    @Override
    public int countTriples() {
        return getMapWithFewestTopLevelIndices().countTriples();
    }

    @Override
    public boolean isEmpty() {
        return spo.isEmpty();
    }

    @Override
    public boolean contains(Triple tripleMatch) {
        return this.stream(tripleMatch.getSubject(), tripleMatch.getPredicate(), tripleMatch.getObject())
                .findAny().isPresent();
    }

    @Override
    public Stream<Triple> stream() {
        return getMapWithFewestTopLevelIndices().stream();
    }

    @Override
    public Stream<Triple> stream(Node sm, Node pm, Node om) {
        switch (PatternClassifier.classify(sm, pm, om)) {
            case SPO:
                return this.spo.stream(
                                sm.getIndexingValue().hashCode())
                        .filter(triple ->
                                pm.matches(triple.getPredicate())
                                && om.matches(triple.getObject())
                                && sm.matches(triple.getSubject()));
            case SP_:
                return this.spo.stream(
                                sm.getIndexingValue().hashCode())
                        .filter(triple ->
                                pm.matches(triple.getPredicate())
                                && sm.matches(triple.getSubject()));
            case S_O:
                return this.osp.stream(
                            om.getIndexingValue().hashCode())
                    .filter(triple ->
                            sm.matches(triple.getSubject())
                            && om.matches(triple.getObject()));
            case S__:
                return this.spo.stream(
                                sm.getIndexingValue().hashCode())
                        .filter(triple ->
                                sm.matches(triple.getSubject()));
            case _PO:
                return this.pos.stream(
                                pm.getIndexingValue().hashCode())
                        .filter(triple ->
                                om.matches(triple.getObject())
                                && pm.matches(triple.getPredicate()));
            case _P_:
                return this.pos.stream(
                                pm.getIndexingValue().hashCode())
                        .filter(triple ->
                                pm.matches(triple.getPredicate()));
            case __O:
                return this.osp.stream(
                                om.getIndexingValue().hashCode())
                        .filter(triple ->
                                om.matches(triple.getObject()));
            case ___:
                return this.stream();
            default:
                throw new IllegalArgumentException("Unknown pattern: " + sm + " " + pm + " " + om);
        }
    }

    @Override
    public Iterator<Triple> find(Triple tripleMatch) {
        final Iterator<Triple> it;
        switch (PatternClassifier.classify(tripleMatch)) {
            case SPO:
                it = this.spo.stream(
                                tripleMatch.getSubject().getIndexingValue().hashCode())
                        .filter(triple ->
                                tripleMatch.getPredicate().matches(triple.getPredicate())
                                && tripleMatch.getObject().matches(triple.getObject())
                                && tripleMatch.getSubject().matches(triple.getSubject()))
                        .iterator();
                break;
            case SP_:
                it = this.spo.stream(
                        tripleMatch.getSubject().getIndexingValue().hashCode())
                        .filter(triple ->
                                tripleMatch.getPredicate().matches(triple.getPredicate())
                                && tripleMatch.getSubject().matches(triple.getSubject()))
                        .iterator();
                break;
            case S_O:
                /*TODO: Optimize!*/
                it = this.osp.stream(
                        tripleMatch.getObject().getIndexingValue().hashCode())
                        .filter(triple ->
                                tripleMatch.getSubject().matches(triple.getSubject())
                                        && tripleMatch.getObject().matches(triple.getObject()))
                        .iterator();
                break;
            case S__:
                it = this.spo.stream(
                                tripleMatch.getSubject().getIndexingValue().hashCode())
                        .filter(triple ->
                                tripleMatch.getSubject().matches(triple.getSubject()))
                        .iterator();
                break;
            case _PO:
                it = this.pos.stream(
                                tripleMatch.getPredicate().getIndexingValue().hashCode())
                        .filter(triple ->
                                tripleMatch.getObject().matches(triple.getObject())
                                && tripleMatch.getPredicate().matches(triple.getPredicate()))
                        .iterator();
                break;
            case _P_:
                it = this.pos.stream(
                                tripleMatch.getPredicate().getIndexingValue().hashCode())
                        .filter(triple ->
                                tripleMatch.getPredicate().matches(triple.getPredicate()))
                        .iterator();
                break;
            case __O:
                it = this.osp.stream(
                                tripleMatch.getObject().getIndexingValue().hashCode())
                        .filter(triple ->
                                tripleMatch.getObject().matches(triple.getObject()))
                        .iterator();
                break;
            case ___:
                it = this.getMapWithFewestTopLevelIndices().find();
                break;
            default:
                throw new IllegalArgumentException("Unknown pattern: " + tripleMatch);
        }
        return it;
    }

    /**
     * The hash code of the triple is cached in the first element of this array.
     * The hash codes of the subject, predicate and object are cached in the
     * second, third and fourth element of this array, respectively.
     */
    private int[] getHashCodes(Triple triple) {
        var hashes = new int[] {
                0,
                triple.getSubject().getIndexingValue().hashCode(),
                triple.getPredicate().getIndexingValue().hashCode(),
                triple.getObject().getIndexingValue().hashCode()
        };
        /**
         * same as in {@link Triple#hashCode(Node, Node, Node)}
         **/
        hashes[0] = (hashes[1] >> 1) ^ hashes[2] ^ (hashes[3] << 1);
        return hashes;
    }

    /**
     * Gets the map with the fewest top level nodes.
     * This map should be used to iterate over the triples or count them.
     * The basic idea is to reduce the number of nodes that need to be iterated over.
     * @return
     */
    private TriplesMapWithOneIndex getMapWithFewestTopLevelIndices() {
        final var subjectIndicesCount = this.spo.numberOfFirstIndices();
        final var predicateIndicesCount = this.pos.numberOfFirstIndices();
        final var objectIndicesCount = this.osp.numberOfFirstIndices();
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

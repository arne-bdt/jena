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

package org.apache.jena.mem2.store.adaptive;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.pattern.PatternClassifier;
import org.apache.jena.mem2.store.TripleStore;
import org.apache.jena.mem2.store.adaptive.set.TripleListSetOSP;
import org.apache.jena.mem2.store.adaptive.set.TripleListSetPOS;
import org.apache.jena.mem2.store.adaptive.set.TripleListSetSPO;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.stream.Stream;

public class AdaptiveTripleStore implements TripleStore {

    public static final int INITIAL_SIZE_FOR_ARRAY_LISTS = 2;

    public static int THRESHOLD_FOR_ARRAY_LISTS = 60;//60-350;

    private QueryableTripleSet spo;
    private QueryableTripleSet pos;
    private QueryableTripleSet osp;

    private final Graph graphForIteratorRemove;

    public AdaptiveTripleStore(final Graph graphForIteratorRemove) {
        this.clear();
        this.graphForIteratorRemove = graphForIteratorRemove;
    }


    @Override
    public void add(final Triple triple) {
        final var hashCode = triple.hashCode();
        if (this.spo.addTriple(triple, hashCode)) {
            this.pos.addTripleUnchecked(triple, hashCode);
            this.osp.addTripleUnchecked(triple, hashCode);
        }
    }

    @Override
    public void remove(final Triple triple) {
        final var hashCode = triple.hashCode();
        if(this.spo.removeTriple(triple, hashCode)) {
            this.pos.removeTripleUnchecked(triple, hashCode);
            this.osp.removeTripleUnchecked(triple, hashCode);
        }
    }

    @Override
    public void clear() {
        this.spo = new TripleListSetSPO(newSet -> this.spo = newSet);
        this.pos = new TripleListSetPOS(newSet -> this.pos = newSet);
        this.osp = new TripleListSetOSP(newSet -> this.osp = newSet);
    }

    @Override
    public int countTriples() {
        return getMapWithFewestTopLevelIndices().countTriples();
    }

    @Override
    public boolean isEmpty() {
        return this.spo.isEmpty();
    }

    @Override
    public boolean contains(Triple tripleMatch) {
        switch (PatternClassifier.classify(tripleMatch)) {

            case SPO:
            case SP_:
            case S__:
                return this.spo.containsMatch(tripleMatch);

            case _PO:
            case _P_:
                return this.pos.containsMatch(tripleMatch);

            case S_O:
            case __O:
                return this.osp.containsMatch(tripleMatch);

            case ___:
                return !this.spo.isEmpty();

            default:
                throw new IllegalStateException("Unknown pattern classifier: " + PatternClassifier.classify(tripleMatch));
        }
    }

    @Override
    public Stream<Triple> stream() {
        return getMapWithFewestTopLevelIndices().streamTriples();
    }

    @Override
    public Stream<Triple> stream(Triple tripleMatch) {
        switch (PatternClassifier.classify(tripleMatch)) {

            case SPO:
            case SP_:
            case S__:
                return this.spo.streamTriples(tripleMatch);

            case _PO:
            case _P_:
                return this.pos.streamTriples(tripleMatch);

            case S_O:
            case __O:
                return this.osp.streamTriples(tripleMatch);

            case ___:
                return this.stream();

            default:
                throw new IllegalStateException("Unknown pattern classifier: " + PatternClassifier.classify(tripleMatch));
        }
    }

    @Override
    public ExtendedIterator<Triple> find(Triple tripleMatch) {
        switch (PatternClassifier.classify(tripleMatch)) {

            case SPO:
            case SP_:
            case S__:
                return this.spo.findTriples(tripleMatch, graphForIteratorRemove);

            case _PO:
            case _P_:
                return this.pos.findTriples(tripleMatch, graphForIteratorRemove);

            case S_O:
            case __O:
                return this.osp.findTriples(tripleMatch, graphForIteratorRemove);

            case ___:
                return this.getMapWithFewestTopLevelIndices().findAll(graphForIteratorRemove);

            default:
                throw new IllegalStateException("Unknown pattern classifier: " + PatternClassifier.classify(tripleMatch));
        }
    }

    /**
     * Gets the map with the fewest top level nodes.
     * This map should be used to iterate over the triples or count them.
     * The basic idea is to reduce the number of nodes that need to be iterated over.
     * @return
     */
    private QueryableTripleSet getMapWithFewestTopLevelIndices() {
//        final var subjectIndexSize = this.spo.countIndexSize();
//        final var predicateIndexSize = this.pos.countIndexSize();
//        final var objectIndexSize = this.osp.countIndexSize();
//        if(subjectIndexSize < predicateIndexSize) {
//            if(subjectIndexSize < objectIndexSize) {
//                return this.spo;
//            } else {
//                return this.osp;
//            }
//        } else {
//            if(predicateIndexSize < objectIndexSize) {
//                return this.pos;
//            } else {
//                return this.osp;
//            }
//        }
        return this.spo;
    }
}

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

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.pattern.PatternClassifier;
import org.apache.jena.mem2.store.TripleStore;
import org.apache.jena.mem2.store.adaptive.set.TripleListSetOSP;
import org.apache.jena.mem2.store.adaptive.set.TripleListSetPOS;
import org.apache.jena.mem2.store.adaptive.set.TripleListSetSPO;

import java.util.Iterator;
import java.util.stream.Stream;

public class AdaptiveTripleStore implements TripleStore {

    public static final int INITIAL_SIZE_FOR_ARRAY_LISTS = 2;

    public static int THRESHOLD_FOR_ARRAY_LISTS = 60;//60-350;

    private QueryableTripleSet spo;
    private QueryableTripleSet pos;
    private QueryableTripleSet osp;


    public AdaptiveTripleStore() {
        this.clear();
    }


    @Override
    public void add(Triple triple) {
        var set = this.spo.addTriple(triple);
        if (null == set) {
            return;
        }
        if(set != this.spo) {
            this.spo = set;
        }
        this.pos = this.pos.addTripleUnchecked(triple);
        this.osp = this.osp.addTripleUnchecked(triple);
    }

    @Override
    public void remove(Triple triple) {
        if(this.spo.removeTriple(triple)) {
            this.pos.removeTripleUnchecked(triple);
            this.osp.removeTripleUnchecked(triple);
        }
    }

    @Override
    public void clear() {
        this.spo = new TripleListSetSPO();
        this.pos = new TripleListSetPOS();
        this.osp = new TripleListSetOSP();
    }

    @Override
    public int countTriples() {
        return getMapWithFewestTopLevelIndices().countTriples();
    }

    @Override
    public boolean isEmpty() {
        return this.spo.indexSize() == 0;
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
                return this.spo.indexSize() != 0;

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
    public Iterator<Triple> find(Triple tripleMatch) {
        switch (PatternClassifier.classify(tripleMatch)) {

            case SPO:
            case SP_:
            case S__:
                return this.spo.findTriples(tripleMatch);

            case _PO:
            case _P_:
                return this.pos.findTriples(tripleMatch);

            case S_O:
            case __O:
                return this.osp.findTriples(tripleMatch);

            case ___:
                return this.stream().iterator();

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
        final var subjectIndexSize = this.spo.indexSize();
        final var predicateIndexSize = this.pos.indexSize();
        final var objectIndexSize = this.osp.indexSize();
        if(subjectIndexSize < predicateIndexSize) {
            if(subjectIndexSize < objectIndexSize) {
                return this.spo;
            } else {
                return this.osp;
            }
        } else {
            if(predicateIndexSize < objectIndexSize) {
                return this.pos;
            } else {
                return this.osp;
            }
        }
    }
}

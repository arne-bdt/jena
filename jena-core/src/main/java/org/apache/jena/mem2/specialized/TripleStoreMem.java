/**
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
package org.apache.jena.mem2.specialized;

import org.apache.jena.graph.Triple;

public class TripleStoreMem {

    private TriplesBySubjectAndPredicate sp = new TriplesBySubjectAndPredicate();
    private TriplesByPredicateAndObject po = new TriplesByPredicateAndObject();
    private TriplesByObjectAndSubject os = new TriplesByObjectAndSubject();

    public class TripleWithHashCodes {
        public final int[] hashes;
        public final Triple triple;

        public TripleWithHashCodes(Triple triple) {
            this.triple = triple;
            this.hashes = new int[4];
            this.hashes[1] = triple.getSubject().hashCode();
            this.hashes[2] = triple.getPredicate().hashCode();
            this.hashes[3] = triple.getObject().hashCode();
            /**
             * same as in {@link Triple.hashCode(org.apache.jena.graph.Node, org.apache.jena.graph.Node, org.apache.jena.graph.Node)}
             **/
            this.hashes[0] = (hashes[1] >> 1) ^ hashes[2] ^ (hashes[3] << 1);
        }
    }

    public class TriplesBySubjectAndPredicate extends FastHashMapBase<FastHashMapBase<FastHashSetBase<Triple>>>  {
        @Override
        protected FastHashMapBase<FastHashSetBase<Triple>>[] createEntryArray(int length) {
            return new TriplesByNode[length];
        }

        public boolean addWithHashCodes(TripleWithHashCodes tripleWithHashCodes) {
            return this.getOrCreate(tripleWithHashCodes.hashes[1], () -> new TriplesByNode())
                    .getOrCreate(tripleWithHashCodes.hashes[2], () -> new TripleSet())
                    .add(tripleWithHashCodes.triple, tripleWithHashCodes.hashes[0]);
        }

        public boolean removeWithHashCodes(TripleWithHashCodes tripleWithHashCodes) {
            final boolean[] removed = {false};
            this.removeIf(tripleWithHashCodes.hashes[1],
                            (s) -> {
                                s.removeIf(tripleWithHashCodes.hashes[2],
                                        (p) -> {
                                            if (removed[0] = p.remove(tripleWithHashCodes.triple, tripleWithHashCodes.hashes[0])) {
                                                return p.isEmpty();
                                            }
                                            return false;
                                        });
                                return s.isEmpty();
                            });
            return removed[0];
        }
    }

    public class TriplesByPredicateAndObject extends FastHashMapBase<FastHashMapBase<FastHashSetBase<Triple>>>  {
        @Override
        protected FastHashMapBase<FastHashSetBase<Triple>>[] createEntryArray(int length) {
            return new TriplesByNode[length];
        }

        public void addWithHashCodesUnsafe(TripleWithHashCodes tripleWithHashCodes) {
            this.getOrCreate(tripleWithHashCodes.hashes[2], () -> new TriplesByNode())
                    .getOrCreate(tripleWithHashCodes.hashes[3], () -> new TripleSet())
                    .addUnchecked(tripleWithHashCodes.triple, tripleWithHashCodes.hashes[0]);
        }

        public void removeWithHashCodesUnsafe(TripleWithHashCodes tripleWithHashCodes) {
            this.removeIf(tripleWithHashCodes.hashes[2],
                    (p) -> {
                        p.removeIf(tripleWithHashCodes.hashes[3],
                                (o) -> {
                                    o.removeUnchecked(tripleWithHashCodes.triple, tripleWithHashCodes.hashes[0]);
                                    return o.isEmpty();
                                });
                        return p.isEmpty();
                    });
        }
    }

    public class TriplesByObjectAndSubject extends FastHashMapBase<FastHashMapBase<FastHashSetBase<Triple>>>  {
        @Override
        protected FastHashMapBase<FastHashSetBase<Triple>>[] createEntryArray(int length) {
            return new TriplesByNode[length];
        }

        public void addWithHashCodesUnsafe(TripleWithHashCodes tripleWithHashCodes) {
            this.getOrCreate(tripleWithHashCodes.hashes[3], () -> new TriplesByNode())
                    .getOrCreate(tripleWithHashCodes.hashes[1], () -> new TripleSet())
                    .addUnchecked(tripleWithHashCodes.triple, tripleWithHashCodes.hashes[0]);
        }

        public void removeWithHashCodesUnsafe(TripleWithHashCodes tripleWithHashCodes) {
            this.removeIf(tripleWithHashCodes.hashes[3],
                    (o) -> {
                        o.removeIf(tripleWithHashCodes.hashes[1],
                                (s) -> {
                                    s.removeUnchecked(tripleWithHashCodes.triple, tripleWithHashCodes.hashes[0]);
                                    return s.isEmpty();
                                });
                        return o.isEmpty();
                    });
        }
    }

    public class TriplesByNode extends FastHashMapBase<FastHashSetBase<Triple>>  {
        @Override
        protected FastHashSetBase<Triple>[] createEntryArray(int length) {
            return new TripleSet[length];
        }
    }

    public class TripleSet extends FastHashSetBase<Triple> {

        @Override
        protected Triple[] createEntryArray(int length) {
            return new Triple[length];
        }
    }



    public void add(final Triple triple) {
        var t = new TripleWithHashCodes(triple);

        if(this.sp.addWithHashCodes(t)) {
            this.po.addWithHashCodesUnsafe(t);
            this.os.addWithHashCodesUnsafe(t);
        }
    }

    public void remove(final Triple triple) {
        var t = new TripleWithHashCodes(triple);

        if(this.sp.removeWithHashCodes(t)) {
            this.po.removeWithHashCodesUnsafe(t);
            this.os.removeWithHashCodesUnsafe(t);
        }
    }
}

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

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

/**
 * A triple with a cached hash codes for the triple itself and the indexing values of subject, predicate and object.
 */
public class TripleWithIndexingHashCodes2 {
    /**
     * The hash code of the triple is cached in the first element of this array.
     * The hash codes of the subject, predicate and object are cached in the
     * second, third and fourth element of this array, respectively.
     */
    private final int[] hashes;
    private final Triple triple;

    public Triple getTriple() {
        return triple;
    }

    public int getSubjectIndexingHashCode() {
        return hashes[1];
    }

    public int getPredicateIndexingHashCode() {
        return hashes[2];
    }

    public int getObjectIndexingHashCode() {
        return hashes[3];
    }

    public TripleWithIndexingHashCodes2(Triple triple) {
        this.triple = triple;
        this.hashes = new int[] {
            0,
            triple.getSubject().getIndexingValue().hashCode(),
            triple.getPredicate().getIndexingValue().hashCode(),
            triple.getObject().getIndexingValue().hashCode()
        };
        /**
         * same as in {@link Triple#hashCode(Node, Node, Node)}
         **/
        this.hashes[0] = (hashes[1] >> 1) ^ hashes[2] ^ (hashes[3] << 1);
    }

    public static int calcHashCodeBasedOnIndexingValues(Triple triple) {
        return (triple.getSubject().getIndexingValue().hashCode() >> 1)
                ^ triple.getPredicate().getIndexingValue().hashCode()
                ^ (triple.getObject().getIndexingValue().hashCode() << 1);
    }

    @Override
    public int hashCode() {
        return this.hashes[0];
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof TripleWithIndexingHashCodes2)) {
            return false;
        }
        var other = (TripleWithIndexingHashCodes2) obj;
        /*compare only the triple hash code - which is a combination of the other hashCodes  */
        if (this.hashes[0] != other.hashes[0]) {
            return false;
        }
        /*shortcut, which skips "instance of and cast" within Triple#equals */
        return this.triple.sameAs(other.triple.getSubject(), other.triple.getPredicate(), other.triple.getObject());
    }
}

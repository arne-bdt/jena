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

/**
 * A triple with a cached hash codes for the triple itself and the indexing values of subject, predicate and object.
 */
public class TripleWithNodeHashes {
    /**
     * The hash code of the triple is cached in the first element of this array.
     * The hash codes of the subject, predicate and object are cached in the
     * second, third and fourth element of this array, respectively.
     */
    private final int[] hashes = new int[3];
    private final boolean[] hashesCalculated = new boolean[3];
    private final Triple triple;
    public Triple getTriple() {
        return triple;
    }

    public int getSubjectHashCode() {
        if (!hashesCalculated[0]) {
            hashes[0] = triple.getSubject().hashCode();
            hashesCalculated[0] = true;
        }
        return hashes[0];
    }

    public int getPredicateHashCode() {
        if (!hashesCalculated[1]) {
            hashes[1] = triple.getPredicate().hashCode();
            hashesCalculated[1] = true;
        }
        return hashes[1];
    }

    public int getObjectHashCode() {
        if (!hashesCalculated[2]) {
            hashes[2] = triple.getObject().hashCode();
            hashesCalculated[2] = true;
        }
        return hashes[2];
    }

    public TripleWithNodeHashes(Triple triple) {
        this.triple = triple;
    }
}

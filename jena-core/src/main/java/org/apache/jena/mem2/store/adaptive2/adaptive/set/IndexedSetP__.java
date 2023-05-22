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

package org.apache.jena.mem2.store.adaptive2.adaptive.set;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.store.adaptive2.adaptive.QueryableTripleSet;
import org.apache.jena.mem2.store.adaptive2.adaptive.TripleWithNodeHashes;
import org.apache.jena.mem2.store.adaptive2.adaptive.base.MapOfIndexedSetsBase;

public class IndexedSetP__ extends MapOfIndexedSetsBase {
    @Override
    protected Node extractKeyFromValueToAddAndRemove(TripleWithNodeHashes tripleWithNodeHashes) {
        return tripleWithNodeHashes.getTriple().getPredicate();
    }

    @Override
    protected QueryableTripleSet extractContainedValue(TripleWithNodeHashes tripleWithNodeHashes) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected int extractHashCode(TripleWithNodeHashes tripleWithNodeHashes) {
        return tripleWithNodeHashes.getPredicateHashCode();
    }

    public IndexedSetP__(final int minCapacity) {
        super(minCapacity);
    }

    @Override
    protected QueryableTripleSet createEntry() {
        return new TripleListSet_OS();
    }

    @Override
    protected Node extractKey(final Triple tripleMatch) {
        return tripleMatch.getPredicate();
    }

    @Override
    public Node getIndexingNode() {
        throw new UnsupportedOperationException();
    }
}

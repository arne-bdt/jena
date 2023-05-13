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

package org.apache.jena.mem2.store.adaptive.set;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.store.adaptive.QueryableTripleSet;
import org.apache.jena.mem2.store.adaptive.base.IndexedMapOfIndexedSetsBase;

import java.util.function.Consumer;

public class IndexedSet_O_ extends IndexedMapOfIndexedSetsBase {

    public IndexedSet_O_(final int indexingValueHashCode, final int minCapacity) {
        super(indexingValueHashCode, minCapacity);
    }

    @Override
    protected QueryableTripleSet createEntry(Consumer<QueryableTripleSet> transitionConsumer) {
        return new TripleListSet__S(transitionConsumer);
    }

    @Override
    protected Node getIndexingNode(final Triple tripleMatch) {
        return tripleMatch.getObject();
    }

    @Override
    protected int getHashCodeOfIndexingValue(final Triple triple) {
        return triple.getObject().getIndexingValue().hashCode();
    }
}

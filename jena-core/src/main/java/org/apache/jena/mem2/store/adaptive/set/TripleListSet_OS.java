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

import org.apache.jena.graph.Triple;
import org.apache.jena.mem.FieldFilter;
import org.apache.jena.mem2.store.adaptive.QueryableTripleSet;
import org.apache.jena.mem2.store.adaptive.QueryableTripleSetWithIndexingValue;
import org.apache.jena.mem2.store.adaptive.base.AdaptiveTripleListSetBase;

import java.util.function.Consumer;
import java.util.function.Predicate;

public class TripleListSet_OS extends AdaptiveTripleListSetBase implements QueryableTripleSetWithIndexingValue {

    public TripleListSet_OS(Consumer<QueryableTripleSet> transitionConsumer) {
        super(transitionConsumer);
    }

    @Override
    protected FieldFilter getMatchFilter(Triple tripleMatch) {
        return FieldFilter.filterOn(tripleMatch,
                Triple.Field.fieldSubject, Triple.Field.fieldObject, Triple.Field.fieldPredicate);
    }

    @Override
    protected QueryableTripleSet transition() {
        final var set = new IndexedSet_O_(this.getIndexValueHashCode(), this.size());
        for(var triple : this) {
            set.addTripleUnchecked(triple, triple.hashCode());
        }
        return set;
    }

    @Override
    public int getIndexValueHashCode() {
        return this.get(0).getPredicate().getIndexingValue().hashCode();
    }
}

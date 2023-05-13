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

package org.apache.jena.mem2.store.adaptive.base;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.store.adaptive.AdaptiveTripleStore;
import org.apache.jena.mem2.store.adaptive.QueryableTripleSet;

import java.util.function.Consumer;

public abstract class AdaptiveTripleListSetBase extends TripleListSetBase {


    private final Consumer<QueryableTripleSet> transitionConsumer;

    public AdaptiveTripleListSetBase(Consumer<QueryableTripleSet> transitionConsumer) {
        super(AdaptiveTripleStore.INITIAL_SIZE_FOR_ARRAY_LISTS);
        this.transitionConsumer = transitionConsumer;
    }

    protected abstract QueryableTripleSet transition();

    @Override
    public boolean addTriple(final Triple triple, final int hashCode) {
        if (this.size() == AdaptiveTripleStore.THRESHOLD_FOR_ARRAY_LISTS) {
            var set = transition();
            transitionConsumer.accept(set);
            return set.addTriple(triple, hashCode);
        }
        return super.addTriple(triple, hashCode);
    }

    @Override
    public void addTripleUnchecked(final Triple triple, final int hashCode) {
        if (this.size() == AdaptiveTripleStore.THRESHOLD_FOR_ARRAY_LISTS) {
            var set = transition();
            transitionConsumer.accept(set);
            set.addTripleUnchecked(triple, hashCode);
        } else {
            super.addTripleUnchecked(triple, hashCode);
        }
    }
}

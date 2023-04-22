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
import org.apache.jena.mem2.specialized.FastHashSetBase;
import org.apache.jena.mem2.store.adaptive.QueryableTripleSet;
import org.apache.jena.mem2.store.adaptive.QueryableTripleSetWithIndexingValue;

import java.util.Iterator;
import java.util.stream.Stream;

public abstract class TripleHashSetBase extends FastHashSetBase<Triple> implements QueryableTripleSetWithIndexingValue {

    private final Object indexingValue;

    public TripleHashSetBase(QueryableTripleSetWithIndexingValue set) {
        super(set.streamTriples(), set.countTriples());
        this.indexingValue = set.getIndexingValue();
    }

    protected abstract boolean matches(final Triple tripleMatch, final Triple triple);

    @Override
    protected Triple[] createEntryArray(int length) {
        return new Triple[length];
    }

    @Override
    public int countTriples() {
        return super.size;
    }

    @Override
    public int indexSize() {
        return super.size;
    }

    @Override
    public QueryableTripleSet addTriple(Triple triple) {
        if(super.add(triple)) {
            return this;
        }
        return null;
    }

    @Override
    public QueryableTripleSet addTripleUnchecked(Triple triple) {
        super.addUnchecked(triple);
        return this;
    }

    @Override
    public boolean removeTriple(Triple triple) {
        return super.remove(triple);
    }

    @Override
    public void removeTripleUnchecked(Triple triple) {
        super.removeUnchecked(triple);
    }

    @Override
    public boolean containsMatch(Triple tripleMatch) {
        return this.streamTriples(tripleMatch).findAny().isPresent();
    }

    @Override
    public Stream<Triple> streamTriples(Triple tripleMatch) {
        return super.stream().filter(triple -> this.matches(tripleMatch, triple));
    }

    @Override
    public Stream<Triple> streamTriples() {
        return super.stream();
    }

    @Override
    public Iterator<Triple> findTriples(Triple tripleMatch) {
        return this.streamTriples(tripleMatch).iterator();
    }

    @Override
    public Object getIndexingValue() {
        return this.indexingValue;
    }
}

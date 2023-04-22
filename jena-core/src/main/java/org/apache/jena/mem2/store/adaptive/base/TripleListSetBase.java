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
import org.apache.jena.mem2.store.adaptive.QueryableTripleSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.IntFunction;
import java.util.stream.Stream;

public abstract class TripleListSetBase extends ArrayList<Triple> implements QueryableTripleSet {

    public TripleListSetBase(int initialCapacity) {
        super(initialCapacity);
    }

    protected abstract boolean matches(final Triple tripleMatch, final Triple triple);

    @Override
    public int countTriples() {
        return this.size();
    }

    @Override
    public int indexSize() {
        return this.size();
    }

    @Override
    public QueryableTripleSet addTriple(Triple triple) {
        if (super.contains(triple)) {
            return null;
        }
        super.add(triple);
        return this;
    }

    @Override
    public QueryableTripleSet addTripleUnchecked(Triple triple) {
        super.add(triple);
        return this;
    }

    @Override
    public boolean removeTriple(Triple triple) {
        return super.remove(triple);
    }

    @Override
    public void removeTripleUnchecked(Triple triple) {
        super.remove(triple);
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
    public Iterator<Triple> findTriples(Triple tripleMatch) {
        return this.streamTriples(tripleMatch).iterator();
    }

    @Override
    public <T> T[] toArray(IntFunction<T[]> generator) {
        return super.toArray(generator);
    }

    @Override
    public Stream<Triple> streamTriples() {
        return super.stream();
    }

    @Override
    public Stream<Triple> parallelStream() {
        return super.parallelStream();
    }
}

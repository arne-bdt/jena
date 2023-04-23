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
import org.apache.jena.mem2.store.adaptive.TripleWithIndexingHashCodes;

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
    public QueryableTripleSet addTriple(TripleWithIndexingHashCodes triple) {
        if (super.contains(triple)) {
            return null;
        }
        super.add(triple.getTriple());
        return this;
    }

    @Override
    public QueryableTripleSet addTripleUnchecked(TripleWithIndexingHashCodes triple) {
        super.add(triple.getTriple());
        return this;
    }

    @Override
    public boolean removeTriple(TripleWithIndexingHashCodes triple) {
        return super.remove(triple.getTriple());
    }

    @Override
    public void removeTripleUnchecked(TripleWithIndexingHashCodes triple) {
        super.remove(triple.getTriple());
    }

    @Override
    public boolean containsMatch(TripleWithIndexingHashCodes tripleMatch) {
        return this.containsTriple(tripleMatch); /*no optimization possible here*/
    }

    @Override
    public boolean containsTriple(TripleWithIndexingHashCodes concreteTriple) {
       for (var triple : this) {
           if (this.matches(concreteTriple.getTriple(), triple)) {
               return true;
           }
       }
       return false;
    }

    @Override
    public Stream<Triple> streamTriples(TripleWithIndexingHashCodes tripleMatch) {
        return super.stream().filter(triple -> this.matches(tripleMatch.getTriple(), triple));
    }

    @Override
    public Iterator<Triple> findTriples(TripleWithIndexingHashCodes tripleMatch) {
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

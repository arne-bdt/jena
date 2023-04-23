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
import org.apache.jena.mem2.store.adaptive.TripleWithIndexingHashCodes;

import java.util.Iterator;
import java.util.stream.Stream;

public abstract class TripleHashSetBase extends FastHashSetBase<Triple> implements QueryableTripleSetWithIndexingValue {

    private final int indexingValueHashCode;

    public TripleHashSetBase(QueryableTripleSetWithIndexingValue set) {
        super(set.countTriples());
        set.streamTriples().forEach(t -> super.addUnchecked(t, TripleWithIndexingHashCodes.calcHashCodeBasedOnIndexingValues(t)));
        this.indexingValueHashCode = set.getIndexValueHashCode();
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
    public QueryableTripleSet addTriple(TripleWithIndexingHashCodes triple) {
        if(super.add(triple.getTriple(), triple.hashCode())) {
            return this;
        }
        return null;
    }

    @Override
    public QueryableTripleSet addTripleUnchecked(TripleWithIndexingHashCodes triple) {
        super.addUnchecked(triple.getTriple(), triple.hashCode());
        return this;
    }

    @Override
    public boolean removeTriple(TripleWithIndexingHashCodes triple) {
        return super.remove(triple.getTriple(), triple.hashCode());
    }

    @Override
    public void removeTripleUnchecked(TripleWithIndexingHashCodes triple) {
        super.removeUnchecked(triple.getTriple(), triple.hashCode());
    }

    @Override
    public boolean containsTriple(TripleWithIndexingHashCodes concreteTriple) {
        var hashCode = concreteTriple.hashCode();
        var index = super.calcStartIndexByHashCode(hashCode);
        if(null == entries[index]) {
            return false;
        }
        if(hashCode == hashCodes[index] && concreteTriple.getTriple().matches(entries[index])) {
            return true;
        } else if(--index < 0){
            index += entries.length;
        }
        while(true) {
            if(null == entries[index]) {
                return false;
            } else if(hashCode == hashCodes[index] && concreteTriple.getTriple().matches(entries[index])) {
                return true;
            } else if(--index < 0){
                index += entries.length;
            }
        }
    }


    @Override
    public boolean containsMatch(TripleWithIndexingHashCodes tripleMatch) {
        return super.stream().anyMatch(triple -> this.matches(tripleMatch.getTriple(), triple));
    }

    @Override
    public Stream<Triple> streamTriples(TripleWithIndexingHashCodes tripleMatch) {
        return super.stream().filter(triple -> this.matches(tripleMatch.getTriple(), triple));
    }

    @Override
    public Stream<Triple> streamTriples() {
        return super.stream();
    }

    @Override
    public Iterator<Triple> findTriples(TripleWithIndexingHashCodes tripleMatch) {
        return this.streamTriples(tripleMatch).iterator();
    }

    @Override
    public int getIndexValueHashCode() {
        return this.indexingValueHashCode;
    }
}

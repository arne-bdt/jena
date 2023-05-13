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

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.FieldFilter;
import org.apache.jena.mem2.iterator.IteratorFiltering;
import org.apache.jena.mem2.iterator.IteratorWrapperWithRemove;
import org.apache.jena.mem2.specialized.FastHashSetBase;
import org.apache.jena.mem2.store.adaptive.QueryableTripleSetWithIndexingValue;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.function.Predicate;
import java.util.stream.Stream;

public abstract class TripleHashSetBase extends FastHashSetBase<Triple> implements QueryableTripleSetWithIndexingValue {

    private final int indexingValueHashCode;

    public TripleHashSetBase(final int indexingValueHashCode, final int minCapacity) {
        super(minCapacity);
        this.indexingValueHashCode = indexingValueHashCode;
    }

    protected abstract FieldFilter getMatchFilter(final Triple tripleMatch);

    @Override
    protected Triple[] createEntryArray(int length) {
        return new Triple[length];
    }

    @Override
    public int countTriples() {
        return super.size;
    }

    @Override
    public int countIndexSize() {
        return 0;
    }

    @Override
    public boolean addTriple(final Triple triple, final int hashCode) {
        return super.add(triple, hashCode);
    }

    @Override
    public void addTripleUnchecked(final Triple triple, final int hashCode) {
        super.addUnchecked(triple, hashCode);
    }

    @Override
    public boolean removeTriple(final Triple triple, final int hashCode) {
        return super.remove(triple, hashCode);
    }

    @Override
    public void removeTripleUnchecked(final Triple triple, final int hashCode) {
        super.removeUnchecked(triple, hashCode);
    }

    @Override
    public boolean containsMatch(final Triple tripleMatch) {
        final var hashCode = tripleMatch.hashCode();
        var index = super.calcStartIndexByHashCode(hashCode);
        if(null == entries[index]) {
            return false;
        }
        final var fieldFilter = this.getMatchFilter(tripleMatch);
        if(!fieldFilter.hasFilter()) {
            return true;
        }
        final var matcher = fieldFilter.getFilter();
        if(hashCode == hashCodes[index] && matcher.test(entries[index])) {
            return true;
        } else if(--index < 0){
            index += entries.length;
        }
        while(true) {
            if(null == entries[index]) {
                return false;
            } else if(hashCode == hashCodes[index] && matcher.test(entries[index])) {
                return true;
            } else if(--index < 0){
                index += entries.length;
            }
        }
    }

    @Override
    public Stream<Triple> streamTriples(final Triple tripleMatch) {
        final var fieldFilter = this.getMatchFilter(tripleMatch);
        if(!fieldFilter.hasFilter()) {
            return super.stream();
        }
        return super.stream().filter(fieldFilter.getFilter());
    }

    @Override
    public Stream<Triple> streamTriples() {
        return super.stream();
    }

    @Override
    public ExtendedIterator<Triple> findTriples(final Triple tripleMatch, final Graph graphForIteratorRemove) {
        final var fieldFilter = this.getMatchFilter(tripleMatch);
        if(!fieldFilter.hasFilter()) {
            return new IteratorWrapperWithRemove(super.iterator(), graphForIteratorRemove);
        }
        return new IteratorFiltering(super.iterator(), fieldFilter.getFilter(), graphForIteratorRemove);
    }

    @Override
    public ExtendedIterator<Triple> findAll(Graph graphForIteratorRemove) {
        return new IteratorWrapperWithRemove(super.iterator(), graphForIteratorRemove);
    }

    @Override
    public int getIndexValueHashCode() {
        return this.indexingValueHashCode;
    }
}

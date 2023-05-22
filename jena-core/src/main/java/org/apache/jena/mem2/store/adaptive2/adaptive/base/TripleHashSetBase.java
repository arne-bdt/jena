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

package org.apache.jena.mem2.store.adaptive2.adaptive.base;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.iterator.IteratorFiltering;
import org.apache.jena.mem2.store.adaptive2.adaptive.QueryableTripleSet;
import org.apache.jena.mem2.store.adaptive2.adaptive.TripleFilter;
import org.apache.jena.mem2.store.adaptive2.adaptive.TripleWithNodeHashes;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;

import java.util.function.Consumer;
import java.util.stream.Stream;

public abstract class TripleHashSetBase extends FastHashSetBase<Triple, TripleWithNodeHashes> implements QueryableTripleSet {

    private final Node indexingNode;

    @Override
    public boolean isReadyForTransition() {
        return false;
    }

    @Override
    public QueryableTripleSet createTransition() {
        throw new UnsupportedOperationException();
    }

    public TripleHashSetBase(final Node indexingNode, final int minCapacity) {
        super(minCapacity);
        this.indexingNode = indexingNode;
    }

    protected abstract TripleFilter getMatchFilter(final Triple tripleMatch);

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
    public boolean addTriple(final TripleWithNodeHashes tripleWithHashes) {
        return super.addValue(tripleWithHashes);
    }

    @Override
    public void addTripleUnchecked(final TripleWithNodeHashes tripleWithHashes) {
        super.addUnchecked(tripleWithHashes);
    }

    @Override
    public boolean removeTriple(final TripleWithNodeHashes tripleWithHashes) {
        return super.remove(tripleWithHashes);
    }

    @Override
    public void removeTripleUnchecked(final TripleWithNodeHashes tripleWithHashes) {
        super.removeUnchecked(tripleWithHashes);
    }

    @Override
    public boolean containsMatch(final Triple tripleMatch) {
        if(tripleMatch.isConcrete()) {
            return super.contains(tripleMatch);
        }
        final var fieldFilter = this.getMatchFilter(tripleMatch);
        if(!fieldFilter.hasFilter()) {
            return true;
        }
        final var filter = fieldFilter.getFilter();
        var spliterator = super.spliterator();
        final boolean[] found = {false};
        Consumer<Triple> tester = triple -> found[0] = filter.test(triple);
        while (!found[0] && spliterator.tryAdvance(tester));
        return found[0];
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
    public ExtendedIterator<Triple> findTriples(final Triple tripleMatch) {
        final var fieldFilter = this.getMatchFilter(tripleMatch);
        if(!fieldFilter.hasFilter()) {
            return WrappedIterator.createNoRemove(super.iterator());
        }
        return new IteratorFiltering(super.iterator(), fieldFilter.getFilter());
    }

    @Override
    public ExtendedIterator<Triple> findAll() {
        return WrappedIterator.createNoRemove(super.iterator());
    }

    @Override
    public Node getIndexingNode() {
        return this.indexingNode;
    }
}

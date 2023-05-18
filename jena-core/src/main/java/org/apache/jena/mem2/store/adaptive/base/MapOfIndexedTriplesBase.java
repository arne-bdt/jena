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

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.store.adaptive.QueryableTripleSet;
import org.apache.jena.mem2.store.adaptive.TripleWithNodeHashes;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.WrappedIterator;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

public abstract class MapOfIndexedTriplesBase extends KeyedValueHashSetBase<Node, Triple, TripleWithNodeHashes> implements QueryableTripleSet {

    private final Node indexingNode;

    @Override
    public Node getIndexingNode() {
        return this.indexingNode;
    }

    public MapOfIndexedTriplesBase(Node indexingNode, int minCapacity)
    {
        super(minCapacity);
        this.indexingNode = indexingNode;
    }

    @Override
    protected Triple extractContainedValue(TripleWithNodeHashes tripleWithNodeHashes) {
        return tripleWithNodeHashes.getTriple();
    }

    @Override
    protected Triple[] createEntryArray(int length) {
        return new Triple[length];
    }

    @Override
    public int countTriples() {
        return this.size();
    }

    @Override
    public int countIndexSize() {
        return this.size();
    }

    @Override
    public boolean isReadyForTransition() {
        return false;
    }

    @Override
    public QueryableTripleSet createTransition() {
        throw new UnsupportedOperationException();
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
        return super.removeByKey(tripleWithHashes);
    }

    @Override
    public void removeTripleUnchecked(final TripleWithNodeHashes tripleWithHashes) {
        super.removeUnchecked(tripleWithHashes);
    }

    @Override
    public boolean containsMatch(final Triple tripleMatch) {
        final var indexingNode = extractKeyFromValue(tripleMatch);
        if(indexingNode.isConcrete()) {
            return -1 < super.findIndexOfKey(indexingNode, indexingNode.hashCode());
        }
        return !super.isEmpty();
    }

    @Override
    public Stream<Triple> streamTriples(final Triple tripleMatch) {
        final var keyNode = extractKeyFromValue(tripleMatch);
        if(keyNode.isConcrete()) {
            var triple = super.getIfPresent(keyNode);
            if(triple == null) {
                return Stream.empty();
            }
            return Stream.of(triple);
        }
        return stream();
    }

    @Override
    public ExtendedIterator<Triple> findTriples(final Triple tripleMatch) {
        final var keyNode = extractKeyFromValue(tripleMatch);
        if(keyNode.isConcrete()) {
            var triple = super.getIfPresent(keyNode);
            if(triple == null) {
                return NiceIterator.emptyIterator();
            }
            return WrappedIterator.createNoRemove(new IteratorOne(triple));
        }
        return WrappedIterator.createNoRemove(super.iterator());
    }

    @Override
    public ExtendedIterator<Triple> findAll() {
        return WrappedIterator.createNoRemove(super.iterator());
    }

    @Override
    public Stream<Triple> streamTriples() {
        return super.stream();
    }


    private class IteratorOne implements Iterator<Triple> {
        private final Triple triple;
        private boolean finished = false;

        public IteratorOne(Triple triple) {
            this.triple = triple;
        }

        @Override
        public boolean hasNext() {
            return !finished;
        }

        @Override
        public Triple next() {
            if(finished) {
                throw new NoSuchElementException();
            }
            finished = true;
            return this.triple;
        }
    }
}

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
import org.apache.jena.mem2.iterator.NestedIterator;
import org.apache.jena.mem2.store.adaptive.QueryableTripleSet;
import org.apache.jena.mem2.store.adaptive.TripleWithNodeHashes;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.stream.Stream;

public abstract class MapOfIndexedSetsBase extends KeyedValueHashSetBase<Node, QueryableTripleSet, TripleWithNodeHashes> implements QueryableTripleSet {

    public MapOfIndexedSetsBase(int minCapacity) {
        super(minCapacity);
    }

    @Override
    protected QueryableTripleSet[] createEntryArray(int length) {
        return new QueryableTripleSet[length];
    }

    @Override
    protected Node extractKeyFromValue(QueryableTripleSet queryableTripleSetWithIndexingNode) {
        return queryableTripleSetWithIndexingNode.getIndexingNode();
    }

    protected abstract QueryableTripleSet createEntry();

    protected abstract Node extractKey(final Triple tripleMatch);

    @Override
    public int countTriples() {
        return super.stream().mapToInt(QueryableTripleSet::countTriples).sum();
    }

    @Override
    public int countIndexSize() {
        return this.size() + super.stream().mapToInt(QueryableTripleSet::countIndexSize).sum();
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
        final Node key = extractKeyFromValueToAddAndRemove(tripleWithHashes);
        final int hashCode = extractHashCode(tripleWithHashes);
        final var index = findIndexOfKey(key, hashCode);
        final boolean added;
        if(index < 0) { /*value does not exist yet*/
            final var newSet = createEntry();
            newSet.addTripleUnchecked(tripleWithHashes);
            entries[~index] = newSet;
            hashCodes[~index] = hashCode;
            size++;
            grow();
            added = true;
        } else { /*existing value found*/
            final var existingSet = entries[index];
            added = existingSet.addTriple(tripleWithHashes);
            if(existingSet.isReadyForTransition()){
                entries[index] = existingSet.createTransition();
            }
        }
        return added;
    }

    @Override
    public void addTripleUnchecked(final TripleWithNodeHashes tripleWithHashes) {
        final Node key = extractKeyFromValueToAddAndRemove(tripleWithHashes);
        final int hashCode = extractHashCode(tripleWithHashes);
        final var index = findIndexOfKey(key, hashCode);
        if(index < 0) { /*value does not exist yet*/
            final var newSet = createEntry();
            newSet.addTripleUnchecked(tripleWithHashes);
            entries[~index] = newSet;
            hashCodes[~index] = hashCode;
            size++;
            grow();
        } else { /*existing value found*/
            final var existingSet = entries[index];
            existingSet.addTripleUnchecked(tripleWithHashes);
            if(existingSet.isReadyForTransition()){
                entries[index] = existingSet.createTransition();
            }
        }
    }

    @Override
    public boolean removeTriple(final TripleWithNodeHashes tripleWithHashes) {
        final Node key = extractKeyFromValueToAddAndRemove(tripleWithHashes);
        final int hashCode = extractHashCode(tripleWithHashes);
        var index = findIndexOfKey(key, hashCode);
        final boolean removed;
        if(index < 0) { /*value does not exist yet*/
            removed = false;
        } else { /*existing value found*/
            final var existingSet = entries[index];
            removed = existingSet.removeTriple(tripleWithHashes);
            if(existingSet.isEmpty()) {
                entries[index] = null;
                rearrangeNeighbours(index);
                size--;
            }
        }
        return removed;
    }

    @Override
    public void removeTripleUnchecked(final TripleWithNodeHashes tripleWithHashes) {
        final Node key = extractKeyFromValueToAddAndRemove(tripleWithHashes);
        final int hashCode = extractHashCode(tripleWithHashes);
        var index = findIndexOfKey(key, hashCode);
        if(index >= 0) { /*existing value found*/
            final var existingSet = entries[index];
            existingSet.removeTripleUnchecked(tripleWithHashes);
            if(existingSet.isEmpty()) {
                entries[index] = null;
                rearrangeNeighbours(index);
                size--;
            }
        }
    }

    @Override
    public boolean containsMatch(final Triple tripleMatch) {
        final var indexingNode = extractKey(tripleMatch);
        if(indexingNode.isConcrete()) {
            var set = super.getIfPresent(indexingNode);
            if(set == null) {
                return false;
            }
            return set.containsMatch(tripleMatch);
        }
        return stream().anyMatch(s -> s.containsMatch(tripleMatch));
    }

    @Override
    public Stream<Triple> streamTriples(final Triple tripleMatch) {
        final var keyNode = extractKey(tripleMatch);
        if(keyNode.isConcrete()) {
            var set = super.getIfPresent(keyNode);
            if(set == null) {
                return Stream.empty();
            }
            return set.streamTriples(tripleMatch);
        }
        return stream().flatMap(s -> s.streamTriples(tripleMatch));
    }

    @Override
    public ExtendedIterator<Triple> findTriples(final Triple tripleMatch) {
        final var keyNode = extractKey(tripleMatch);
        if(keyNode.isConcrete()) {
            var set = super.getIfPresent(keyNode);
            if(set == null) {
                return NiceIterator.emptyIterator();
            }
            return set.findTriples(tripleMatch);
        }
        return this.findAll();
    }

    @Override
    public ExtendedIterator<Triple> findAll() {
        return new NestedIterator<>(super.iterator(), QueryableTripleSet::findAll);
    }

    @Override
    public Stream<Triple> streamTriples() {
        return super.stream().flatMap(QueryableTripleSet::streamTriples);
    }

}

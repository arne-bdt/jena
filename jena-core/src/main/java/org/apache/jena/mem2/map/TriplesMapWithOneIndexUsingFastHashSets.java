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

package org.apache.jena.mem2.map;

import org.apache.jena.graph.Triple;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

public class TriplesMapWithOneIndexUsingFastHashSets implements TriplesMapWithOneIndex {

    private static class TripleSet extends FastHashSetBase<Triple> {

        @Override
        protected Triple[] createEntryArray(int length) {
            return new Triple[length];
        }
    }


    private static class Level0Index extends FastHashSetBase<TripleSet>  {

        @Override
        protected TripleSet[] createEntryArray(int length) {
            return new TripleSet[length];
        }
    }

    final transient Level0Index index0 = new Level0Index();

    @Override
    public boolean add(int level0IndexingHashCode, int tripleHashCode, Triple triple) {
        final boolean[] added = {false};
        index0.modifyOrCreateEntry(level0IndexingHashCode, (l1) -> {
            if (l1 == null) {
                l1 = new TripleSet();
                l1.addWithoutChecking(triple, tripleHashCode);
                added[0] = true;
                return l1;
            }
            added[0] = l1.add(triple, tripleHashCode);
            return l1;
        });
        return added[0];
    }

    @Override
    public void addWithoutChecking(int level0IndexingHashCode, int tripleHashCode, Triple triple) {
        index0.modifyOrCreateEntry(level0IndexingHashCode, (l1) -> {
            if (l1 == null) {
                l1 = new TripleSet();
            }
            l1.addWithoutChecking(triple, tripleHashCode);
            return l1;
        });
    }

    @Override
    public boolean remove(int level0IndexingHashCode, int tripleHashCode, Triple triple) {
        final boolean[] removed = {false};
        index0.modifyOrRemoveEntry(level0IndexingHashCode, (l1) -> {
            if (l1 == null) {
                removed[0] = false;
                return null;
            }
            if (removed[0] = l1.remove(triple, tripleHashCode)) {
                if (l1.isEmpty()) {
                    return null;
                }
            }
            return l1;
        });
        return removed[0];
    }

    @Override
    public void removeWithoutChecking(final int level0IndexingHashCode, final int tripleHashCode, final Triple triple) {
        index0.modifyOrRemoveEntry(level0IndexingHashCode, (l1) -> {
            l1.removeWithoutChecking(triple, tripleHashCode);
            if (l1.isEmpty()) {
                return null;
            }
            return l1;
        });
    }

    @Override
    public void clear() {
        index0.clear();
    }

    @Override
    public int numberOfFirstIndices() {
        return index0.size();
    }

    @Override
    public int countTriples() {
        return index0.stream().mapToInt(tripples -> tripples.size()).sum();
    }

    @Override
    public boolean isEmpty() {
        return index0.isEmpty();
    }

    @Override
    public Stream<Triple> stream() {
        return index0.stream().flatMap(FastHashSetBase::stream);
    }

    @Override
    public Stream<Triple> stream(final int level0IndexingHashCode, final int tripleHashCode, final Triple triple) {
        var l1 = this.index0.getIfPresent(level0IndexingHashCode);
        if(l1 == null) {
            return Stream.empty();
        }
        var t = l1.getIfPresent(triple, tripleHashCode);
        if(t == null) {
            return Stream.empty();
        }
        return Stream.of(t);
    }

    @Override
    public Stream<Triple> stream(final int level0IndexingHashCode) {
        var l1 = this.index0.getIfPresent(level0IndexingHashCode);
        if(l1 == null) {
            return Stream.empty();
        }
        return l1.stream();
    }

    @Override
    public Iterator<Triple> find() {
        return stream().iterator();
    }

    @Override
    public Iterator<Triple> find(final int level0IndexingHashCode, final int tripleHashCode, final Triple triple) {
        var l1 = this.index0.getIfPresent(level0IndexingHashCode);
        if(l1 == null) {
            return emptyIterator;
        }
        var t = l1.getIfPresent(triple, tripleHashCode);
        if(t == null) {
            return emptyIterator;
        }
        return new SingleElementIterator<>(t);
    }

    @Override
    public Iterator<Triple> find(int level0IndexingHashCode) {
        var l1 = this.index0.getIfPresent(level0IndexingHashCode);
        if(l1 == null) {
            return emptyIterator;
        }
        return l1.stream().iterator();
    }

    private static Iterator<Triple> emptyIterator = new EmptyIterator<>();

    private static class EmptyIterator<E> implements Iterator<E> {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public E next() {
            throw new NoSuchElementException();
        }
    }

    private static class SingleElementIterator<E> implements Iterator<E> {

        private E element;

        public SingleElementIterator(E element) {
            this.element = element;
        }

        @Override
        public boolean hasNext() {
            return this.element != null;
        }

        @Override
        public E next() {
            try {
                return this.element;
            }
            finally {
                this.element = null;
            }
        }
    }
}

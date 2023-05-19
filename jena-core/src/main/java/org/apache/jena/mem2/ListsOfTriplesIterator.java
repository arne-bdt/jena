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

package org.apache.jena.mem2;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.specialized.TripleSetWithIndexingNode;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.FilterIterator;
import org.apache.jena.util.iterator.Map1Iterator;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

class ListsOfTriplesIterator implements ExtendedIterator<Triple> {
    private static final Iterator<TripleSetWithIndexingNode> EMPTY_SET_ITERATOR = Collections.emptyIterator();
    private static final Iterator<Triple> EMPTY_TRIPLES_ITERATOR = Collections.emptyIterator();

    private Iterator<TripleSetWithIndexingNode> baseIterator;
    private Iterator<Triple> subIterator;
    private Triple current;

    public ListsOfTriplesIterator(Iterator<TripleSetWithIndexingNode> baseIterator) {
        this.baseIterator = baseIterator;
        subIterator = baseIterator.hasNext() ? baseIterator.next().iterator() : EMPTY_TRIPLES_ITERATOR;
    }

    /**
     * Returns {@code true} if the iteration has more elements.
     * (In other words, returns {@code true} if {@link #next} would
     * return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     */
    @Override
    public boolean hasNext() {
        if (subIterator.hasNext()) {
            return true;
        }
        while (baseIterator.hasNext()) {
            if ((subIterator = baseIterator.next().iterator()).hasNext()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    @Override
    public Triple next() {
        return current = subIterator.next();
    }

    @Override
    public void forEach(Consumer<Triple> action) {
        while (subIterator.hasNext()) {
            action.accept(subIterator.next());
        }
        while (baseIterator.hasNext()) {
            subIterator = baseIterator.next().iterator();
            while (subIterator.hasNext()) {
                action.accept(subIterator.next());
            }
        }
    }

    @Override
    public void close() {

    }

    @Override
    public Triple removeNext() {
        var result = next();
        remove();
        return result;
    }

    @Override
    public <X extends Triple> ExtendedIterator<Triple> andThen(Iterator<X> other) {
        return NiceIterator.andThen(this, other);
    }

    @Override
    public ExtendedIterator<Triple> filterKeep(Predicate<Triple> f) {
        return new FilterIterator<>(f, this);
    }

    @Override
    public ExtendedIterator<Triple> filterDrop(Predicate<Triple> f) {
        return new FilterIterator<>(f.negate(), this);
    }

    @Override
    public <U> ExtendedIterator<U> mapWith(Function<Triple, U> map1) {
        return new Map1Iterator<>(map1, this);
    }

    @Override
    public List<Triple> toList() {
        return NiceIterator.asList(this);
    }

    @Override
    public Set<Triple> toSet() {
        return NiceIterator.asSet(this);
    }
}

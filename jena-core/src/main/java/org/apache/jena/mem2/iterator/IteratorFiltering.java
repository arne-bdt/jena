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

package org.apache.jena.mem2.iterator;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.FilterIterator;
import org.apache.jena.util.iterator.Map1Iterator;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Basically the same as FilterIterator<> but with clear and simple implementation without inheriting possibly
 * strange behaviour from any of the base classes.
 * This Iterator also directly supports wrapWithRemoveSupport
 */
public class IteratorFiltering implements ExtendedIterator<Triple> {

    private final static Predicate<Triple> FILTER_ANY = t -> true;
    private final Graph graphToRemoveFrom;
    private Predicate<Triple> filter;
    private Iterator<Triple> iterator;
    private boolean hasCurrent = false;
    /**
     * The remembered current triple.
     */
    private Triple current;

    /**
     * Initialise this wrapping with the given base iterator and remove-control.
     *
     * @param iterator the base iterator
     * @param filter   the filter predicate for this iteration
     */
    public IteratorFiltering(Iterator<Triple> iterator, Predicate<Triple> filter, Graph graphToRemoveFrom) {
        this.graphToRemoveFrom = graphToRemoveFrom;
        this.iterator = iterator;
        this.filter = filter;
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
        if (hasCurrent) {
            return true;
        }
        while (iterator.hasNext()) {
            if (filter.test(current = this.iterator.next())) {
                return hasCurrent = true;
            }
        }
        return this.hasCurrent;
    }

    /**
     * Answer the next object, remembering it in <code>current</code>.
     *
     * @see Iterator#next()
     */
    @Override
    public Triple next() {
        if (hasCurrent || hasNext()) {
            hasCurrent = false;
            return current;
        }
        throw new NoSuchElementException();
    }

    @Override
    public void forEachRemaining(Consumer<? super Triple> action) {
        if (hasCurrent) {
            action.accept(current);
        }
        while (iterator.hasNext()) {
            if (filter.test(current = this.iterator.next())) {
                action.accept(current);
            }
        }
        hasCurrent = false;
    }

    @Override
    public void remove() {
        if (current == null) {
            throw new IllegalStateException();
        }
        if (this.filter == FILTER_ANY) {
            graphToRemoveFrom.delete(current);
        } else {
            var currentBeforeToList = current;
            this.iterator = this.toList().iterator();
            this.filter = FILTER_ANY;
            graphToRemoveFrom.delete(currentBeforeToList);
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

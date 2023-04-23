package org.apache.jena.mem2;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.specialized.TripleSetWithIndexingValue;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.FilterIterator;
import org.apache.jena.util.iterator.Map1Iterator;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

class ListsOfTriplesIterator implements ExtendedIterator<Triple> {
    private static final Iterator<TripleSetWithIndexingValue> EMPTY_SET_ITERATOR = Collections.emptyIterator();
    private static final Iterator<Triple> EMPTY_TRIPLES_ITERATOR = Collections.emptyIterator();
    private final Graph graph;

    private Iterator<TripleSetWithIndexingValue> baseIterator;
    private Iterator<Triple> subIterator;
    private Triple current;

    public ListsOfTriplesIterator(Iterator<TripleSetWithIndexingValue> baseIterator, Graph graph) {
        this.graph = graph;
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
    public void remove() {
        if (current == null) {
            throw new IllegalStateException();
        }
        if (this.baseIterator == EMPTY_SET_ITERATOR) {
            graph.delete(current);
        } else {
            var currentBeforeToList = current;
            this.subIterator = this.toList().iterator();
            this.baseIterator = EMPTY_SET_ITERATOR;
            graph.delete(currentBeforeToList);
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

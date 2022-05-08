package org.apache.jena.mem.simple;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class HashTripleMap implements TripleMapWithOneKey {

    protected final static float HASH_MAP_LOAD_FACTOR = 0.75f;
    private HashSet<Triple> map;

    private final Function<Triple, Node> keyNodeResolver;
    private final BiPredicate<Triple, Triple> containsPredicate;

    public HashTripleMap(TripleMapWithOneKey tripleMap, final Function<Triple, Node> keyNodeResolver,
                         final BiPredicate<Triple, Triple> containsPredicate) {
        this.keyNodeResolver = keyNodeResolver;
        this.containsPredicate = containsPredicate;
        this.map = new HashSet<>(tripleMap.size(), HASH_MAP_LOAD_FACTOR);
        tripleMap.stream().forEach(this.map::add);
    }

    private int getKey(final Triple t) {
        return keyNodeResolver.apply(t).getIndexingValue().hashCode();
    }

    @Override
    public boolean addIfNotExists(Triple t) {
        return map.add(t);
    }

    @Override
    public void addDefinitetly(Triple t) {
        map.add(t);
    }

    @Override
    public boolean removeIfExits(Triple t) {
        return map.remove(t);
    }

    @Override
    public void removeExisting(Triple t) {
        map.remove(t);
    }

    protected static boolean equalsObjectOk( Triple t )
    {
        Node o = t.getObject();
        return o.isLiteral() ? o.getLiteralDatatype() == null : true;
    }

    @Override
    public boolean contains(Triple t) {
        if(equalsObjectOk(t)) {
            return map.contains(t);
        }
        return map.stream()
                .anyMatch(triple -> containsPredicate.test(triple, t));
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public int numberOfKeys() {
        return 1;
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public Stream<Triple> stream() {
        return map.stream();
    }

    @Override
    public Stream<Triple> stream(Node firstKeyNode) {
        return this.stream()
                .filter(t -> t != null && getKey(t) == firstKeyNode.getIndexingValue().hashCode());
    }

    @Override
    public Iterator<Triple> iterator() {
        if(map.isEmpty()) {
            return null;
        }
        return map.iterator();
    }

    @Override
    public Iterator<Triple> iterator(Node firstKeyNode) {
        if(map.isEmpty()) {
            return null;
        }
        return new IteratorFiltering(map.iterator(), t -> getKey(t) == firstKeyNode.getIndexingValue().hashCode());
    }

    /**
     * Basically the same as FilterIterator<> but with clear and simple implementation without inheriting possibly
     * strange behaviour from any of the base classes.
     * This Iterator also directly supports wrapWithRemoveSupport
     */
    private static class IteratorFiltering implements Iterator<Triple> {

        private final Predicate<Triple> filter;
        private boolean hasCurrent = false;

        private final Iterator<Triple> iterator;

        /**
         The remembered current triple.
         */
        private Triple current;

        /**
         * Initialise this wrapping with the given base iterator and remove-control.
         *
         * @param filter        the filter predicate for this iteration
         */
        protected IteratorFiltering(Iterator<Triple> iterator, Predicate<Triple> filter) {
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
            while(!this.hasCurrent && iterator.hasNext()) {
                var candidate = iterator.next();
                this.hasCurrent = filter.test(candidate);
                if(this.hasCurrent) {
                    this.current = candidate;
                }
            }
            return this.hasCurrent;
        }

        /**
         Answer the next object, remembering it in <code>current</code>.
         @see Iterator#next()
         */
        @Override
        public Triple next()
        {
            if (hasCurrent || hasNext())
            {
                hasCurrent = false;
                return current;
            }
            throw new NoSuchElementException();
        }
    }
}

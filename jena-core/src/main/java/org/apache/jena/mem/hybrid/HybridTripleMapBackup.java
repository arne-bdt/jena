package org.apache.jena.mem.hybrid;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class HybridTripleMapBackup implements TripleMapWithOneKey {

    protected final static int SWITCH_TO_MAP_THRESHOLD = 50;
    private static final int INITIAL_SIZE_FOR_NESTED_ARRAY_LISTS = 1;
    private final Function<Triple, Node> keyNodeResolver;
    private final BiPredicate<Triple, Triple> containsPredicate;
    private ArrayList<Triple> list = new ArrayList<>(1);
    private HashMap<Integer, List<Triple>> map;
    private boolean isList = true;

    public HybridTripleMapBackup(final Function<Triple, Node> keyNodeResolver,
                                 final BiPredicate<Triple, Triple> containsPredicate) {
        this.keyNodeResolver = keyNodeResolver;
        this.containsPredicate = containsPredicate;
    }

    private int getKey(final Triple t) {
        return keyNodeResolver.apply(t).getIndexingValue().hashCode();
    }

    @Override
    public boolean addIfNotExists(Triple t) {
        var key = getKey(t);
        if(isList) {
            for (Triple triple : list) {
                if(key != getKey(triple)) {
                    continue;
                }
                if(containsPredicate.test(triple, t)) {
                    return false;
                }
            }
            if(list.size()+1 == SWITCH_TO_MAP_THRESHOLD) {
                this.map = new HashMap<>(SWITCH_TO_MAP_THRESHOLD);
                for (Triple triple : list) {
                    var l = this.map.computeIfAbsent(getKey(triple),
                            k -> new ArrayList<>(INITIAL_SIZE_FOR_NESTED_ARRAY_LISTS));
                    l.add(triple);
                }
                var l = this.map.computeIfAbsent(key,
                        k -> new ArrayList<>(INITIAL_SIZE_FOR_NESTED_ARRAY_LISTS));
                l.add(t);
                this.isList = false;
                this.list = null;
            } else {
                list.add(t);
            }
            return true;
        } else {
            final boolean[] added = {false};
            map.compute(key, (k, v) -> {
                if(v == null) {
                    v = new ArrayList<Triple>(INITIAL_SIZE_FOR_NESTED_ARRAY_LISTS);
                } else {
                    for (Triple triple : v) {
                        if(containsPredicate.test(triple, t)) {
                            return v;
                        }
                    }
                }
                v.add(t);
                added[0] = true;
                return v;
            });
            return added[0];
        }
    }

    @Override
    public void addDefinitetly(Triple t) {
        if(isList) {
            if(list.size()+1 == SWITCH_TO_MAP_THRESHOLD) {
                this.map = new HashMap<>(SWITCH_TO_MAP_THRESHOLD);
                for (Triple triple : list) {
                    var l = this.map.computeIfAbsent(getKey(triple),
                            k -> new ArrayList<>(INITIAL_SIZE_FOR_NESTED_ARRAY_LISTS));
                    l.add(triple);
                }
                var l = this.map.computeIfAbsent(getKey(t),
                        k -> new ArrayList<>(INITIAL_SIZE_FOR_NESTED_ARRAY_LISTS));
                l.add(t);
                this.isList = false;
                this.list = null;
            } else {
                list.add(t);
            }
        } else {
            var l = map.computeIfAbsent(getKey(t),
                    k -> new ArrayList<>(INITIAL_SIZE_FOR_NESTED_ARRAY_LISTS));
            l.add(t);
        }
    }

    @Override
    public boolean removeIfExits(Triple t) {
        var key = getKey(t);
        if(isList) {
            for (int i=0; i<list.size(); i++) {
                var triple = list.get(i);
                if(key != getKey(triple)) {
                    continue;
                }
                if(containsPredicate.test(triple, t)) {
                    list.remove(i);
                    return true;
                }
            }
            return false;
        }
        final boolean[] removed = {false};
        map.computeIfPresent(key, (k, v) -> {
            for (int i=0; i<v.size(); i++) {
                var triple = v.get(i);
                if(containsPredicate.test(triple, t)) {
                    v.remove(i);
                    removed[0] = true;
                    return v.isEmpty() ? null : v;
                }
            }
            return v;
        });
        return removed[0];
    }

    @Override
    public void removeExisting(Triple t) {
        var key = getKey(t);
        if(isList) {
            for (int i=0; i<list.size(); i++) {
                var triple = list.get(i);
                if(key != getKey(triple)) {
                    continue;
                }
                if(containsPredicate.test(triple, t)) {
                    list.remove(i);
                    return;
                }
            }
        } else {
            var l = map.computeIfPresent(key, (k, v) -> {
                for (int i=0; i<v.size(); i++) {
                    var triple = v.get(i);
                    if(containsPredicate.test(triple, t)) {
                        v.remove(i);
                        return v.isEmpty() ? null : v;
                    }
                }
                return v;
            });
        }
    }

    @Override
    public boolean contains(Triple t) {
        var key = getKey(t);
        if(isList) {
            for (int i=0; i<list.size(); i++) {
                var triple = list.get(i);
                if(key != getKey(triple)) {
                    continue;
                }
                if(containsPredicate.test(triple, t)) {
                    return true;
                }
            }
        } else {
            var l = map.get(key);
            if(l == null) {
                return false;
            }
            for (int i=0; i<l.size(); i++) {
                var triple = l.get(i);
                if(containsPredicate.test(triple, t)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean isEmpty() {
        return isList ? list.isEmpty() : map.isEmpty();
    }

    @Override
    public void clear() {
        if(isList) {
            list.clear();
        } else {
            list = new ArrayList<>(INITIAL_SIZE_FOR_NESTED_ARRAY_LISTS);
            isList = true;
            map = null;
        }
    }

    @Override
    public int numberOfKeys() {
        return isList ? 0 : map.size();
    }

    @Override
    public int size() {
        return isList ? list.size() : map.values().stream().mapToInt(List::size).sum();
    }

    @Override
    public Stream<Triple> stream() {
        return isList ? list.stream() : map.values().stream().flatMap(Collection::stream);
    }

    @Override
    public Stream<Triple> stream(Node firstKeyNode) {
        if(isList) {
            return list.stream().filter(t -> getKey(t) == firstKeyNode.getIndexingValue().hashCode());
        }
        var l = map.get(firstKeyNode.getIndexingValue().hashCode());
        if(l == null) {
            return Stream.empty();
        }
        return l.stream();
    }

    @Override
    public Iterator<Triple> iterator() {
        if(isList) {
            return list.iterator();
        }
        return new NestedTriplesIterator(map.values().iterator());
    }

    @Override
    public Iterator<Triple> iterator(Node firstKeyNode) {
        if(isList) {
            return new IteratorFiltering(list.iterator(), t -> getKey(t) == firstKeyNode.getIndexingValue().hashCode());
        }
        var l = map.get(firstKeyNode.getIndexingValue().hashCode());
        if(l == null) {
            return null;
        }
        return l.iterator();
    }

    private static class NestedTriplesIterator<T extends Iterable<Triple>> implements Iterator<Triple> {

        private final Iterator<T> baseIterator;
        private Iterator<Triple> subIterator;
        private boolean hasSubIterator = false;

        public NestedTriplesIterator(Iterator<T> baseIterator) {

            this.baseIterator = baseIterator;
            if (baseIterator.hasNext()) {
                subIterator = baseIterator.next().iterator();
                hasSubIterator = true;
            }
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
            if (hasSubIterator) {
                if (subIterator.hasNext()) {
                    return true;
                }
                while (baseIterator.hasNext()) {
                    subIterator = baseIterator.next().iterator();
                    if (subIterator.hasNext()) {
                        return true;
                    }
                }
                hasSubIterator = false;
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
            if (!hasSubIterator || !this.hasNext()) {
                throw new NoSuchElementException();
            }
            return subIterator.next();
        }
    }

    /**
     * Basically the same as FilterIterator<> but with clear and simple implementation without inheriting possibly
     * strange behaviour from any of the base classes.
     * This Iterator also directly supports wrapWithRemoveSupport
     */
    private static class IteratorFiltering implements Iterator<Triple> {

        private final Predicate<Triple> filter;
        private final Iterator<Triple> iterator;
        private boolean hasCurrent = false;
        /**
         The remembered current triple.
         */
        private Triple current;

        /**
         * Initialise this wrapping with the given base iterator and remove-control.
         *
         * @param iterator      the base iterator
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
            while(!this.hasCurrent && this.iterator.hasNext()) {
                var candidate = this.iterator.next();
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

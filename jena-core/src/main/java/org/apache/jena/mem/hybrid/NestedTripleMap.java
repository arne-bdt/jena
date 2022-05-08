package org.apache.jena.mem.hybrid;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class NestedTripleMap implements TripleMapWithTwoKeys {

    private final Function<Triple, Node> keyNodeResolver;
    private final Supplier<TripleMapWithOneKey> nestedMapSupplier;
    private final HashMap<Integer, TripleMapWithOneKey> map = new HashMap<>();

    public NestedTripleMap(final Function<Triple, Node> keyNodeResolver,
                           final Supplier<TripleMapWithOneKey> nestedMapSupplier) {
        this.keyNodeResolver = keyNodeResolver;
        this.nestedMapSupplier = nestedMapSupplier;
    }

    private int getKey(final Triple t) {
        return keyNodeResolver.apply(t).getIndexingValue().hashCode();
    }

    @Override
    public boolean addIfNotExists(Triple t) {
        var key = getKey(t);
        final boolean[] added = {false};
        map.compute(key, (k, v) -> {
            if (v == null) {
                v = nestedMapSupplier.get();
                v.addDefinitetly(t);
                added[0] = true;
            } else {
                if(v.addIfNotExists(t)) {
                    added[0] = true;
                }
            }
            return v;
        });
        return added[0];
    }

    @Override
    public void addDefinitetly(Triple t) {
        var nestedMap = map
                .computeIfAbsent(getKey(t),
                        k -> nestedMapSupplier.get());
        nestedMap.addDefinitetly(t);
    }

    @Override
    public boolean removeIfExits(Triple t) {
        var key = getKey(t);
        final boolean[] removed = {false};
        map.computeIfPresent(key, (k, v) -> {
            if(v.removeIfExits(t)) {
                removed[0] = true;
                return v.isEmpty() ? null : v;
            }
            return null;
        });
        return removed[0];
    }

    @Override
    public void removeExisting(Triple t) {
        var key = getKey(t);
        map.computeIfPresent(key, (k, v) -> {
            v.removeExisting(t);
            if(v.isEmpty()) {
                return null;
            }
            return v;
        });
    }

    @Override
    public boolean contains(Triple t) {
        var key = getKey(t);
        var nestedMap = map.get(key);
        if(nestedMap == null) {
            return false;
        }
        return nestedMap.contains(t);
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public void clear() {
        this.map.clear();
    }

    @Override
    public int numberOfKeys() {
        return map.size() * map.values().stream().mapToInt(TripleMapWithOneKey::size).sum();
    }

    @Override
    public int size() {
        return map.values().stream().mapToInt(TripleMapWithOneKey::size).sum();
    }

    @Override
    public Stream<Triple> stream() {
        return map.values().stream().flatMap(TripleMapWithOneKey::stream);
    }

    @Override
    public Stream<Triple> stream(Node firstKeyNode) {
        var nestedMap = map.get(firstKeyNode.getIndexingValue().hashCode());
        if(nestedMap == null) {
            return Stream.empty();
        }
        return nestedMap.stream();
    }

    @Override
    public Iterator<Triple> iterator() {
        return new NestedTriplesIterator(map.values().iterator());
    }

    @Override
    public Iterator<Triple> iterator(Node firstKeyNode) {
        var nestedMap = map.get(firstKeyNode.getIndexingValue().hashCode());
        if(nestedMap == null) {
            return null;
        }
        return nestedMap.iterator();
    }

    @Override
    public Stream<Triple> stream(Node firstKeyNode, Node secondKeyNode) {
        var nestedMap = map.get(firstKeyNode.getIndexingValue().hashCode());
        if(nestedMap == null) {
            return Stream.empty();
        }
        return nestedMap.stream(secondKeyNode);
    }

    @Override
    public Iterator<Triple> iterator(Node firstKeyNode, Node secondKeyNode) {
        var nestedMap = map.get(firstKeyNode.getIndexingValue().hashCode());
        if(nestedMap == null) {
            return null;
        }
        return nestedMap.iterator(secondKeyNode);
    }

    private static class NestedTriplesIterator implements Iterator<Triple> {

        private final Iterator<TripleMapWithOneKey> baseIterator;
        private Iterator<Triple> subIterator;
        private boolean hasSubIterator = false;

        public NestedTriplesIterator(Iterator<TripleMapWithOneKey> baseIterator) {

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
}

package org.apache.jena.mem2.specialized;

import org.apache.jena.graph.Triple;

import java.util.Set;

public interface TripleSetWithIndexingValue extends Set<Triple> {

    Object getIndexingValue();

    void addUnsafe(Triple t);

    boolean areOperationsWithHashCodesSupported();

    default boolean add(Triple t, int hashCode) {
        throw new UnsupportedOperationException("This default implementation only exists to avoid casts and keep the compiler calm.");
    }

    default void addUnsafe(Triple t, int hashCode) {
        throw new UnsupportedOperationException("This default implementation only exists to avoid casts and keep the compiler calm.");
    }

    default boolean remove(Triple t, int hashCode) {
        throw new UnsupportedOperationException("This default implementation only exists to avoid casts and keep the compiler calm.");
    }

    void removeUnsafe(Triple t);

    default void removeUnsafe(Triple t, int hashCode) {
        throw new UnsupportedOperationException("This default implementation only exists to avoid casts and keep the compiler calm.");
    }
}

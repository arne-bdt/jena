package org.apache.jena.mem2.collection;


/**
 * Extension of {@link JenaSet} that allows to add and remove elements
 * with a given hash code.
 * This is useful if the hash code is already known.
 */
public interface JenaSetHashOptimized<E> extends JenaSet<E> {
    boolean tryAdd(E key, int hashCode);

    void addUnchecked(E key, int hashCode);

    boolean tryRemove(E key, int hashCode);

    void removeUnchecked(E key, int hashCode);
}

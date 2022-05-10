package org.apache.jena.mem.hash_no_entry;

import org.apache.jena.graph.Triple;

public class MapEntry<T> {
    public final T value;
    public final int hashes[];
    public MapEntry nextEntries[];

    public MapEntry(T value, int[] hashes) {
        this.value = value;
        this.hashes = hashes;
        this.nextEntries = new MapEntry[hashes.length];
    }

    public static <E extends Triple> MapEntry<E> fromTriple(E t) {
        var hashes = new int[] {
                t.getSubject().getIndexingValue().hashCode(),
                t.getPredicate().getIndexingValue().hashCode(),
                t.getObject().getIndexingValue().hashCode()};
        return new MapEntry<E>(t, hashes);
    }
}

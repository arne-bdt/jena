package org.apache.jena.mem.sorted.experiment.caching_hashes;

import org.apache.jena.graph.Triple;

import java.util.Iterator;
import java.util.stream.Stream;

public interface TripleCollection {
    boolean isEmpty();

    void clear();

    int numberOfKeys();

    int size();

    Stream<Triple> stream();

    Iterator<Triple> iterator();
}

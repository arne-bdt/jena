package org.apache.jena.mem2.collection;

import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface JenaSet<E> extends JenaMapSetCommon<E> {

    boolean tryAdd(E key);

    void addUnchecked(E key);

    ExtendedIterator<E> keyIterator();

    Spliterator<E> keySpliterator();

    default Stream<E> keyStream() {
        return StreamSupport.stream(keySpliterator(), false);
    }

    default Stream<E> keyStreamParallel() {
        return StreamSupport.stream(keySpliterator(), true);
    }
}

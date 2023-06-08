package org.apache.jena.mem2.collection;

import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.Spliterator;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface JenaMapSetCommon<Key> {

    void clear();

    int size();

    boolean isEmpty();

    boolean containsKey(Key key);

    boolean anyMatch(Predicate<Key> predicate);

    boolean tryRemove(Key key);

    void removeUnchecked(Key key);

    ExtendedIterator<Key> keyIterator();

    Spliterator<Key> keySpliterator();

    default Stream<Key> keyStream() {
        return StreamSupport.stream(keySpliterator(), false);
    }

}

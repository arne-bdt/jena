package org.apache.jena.mem2.collection;

import org.apache.jena.mem2.iterator.SparseArrayIterator;
import org.apache.jena.mem2.spliterator.SparseArraySpliterator;
import org.apache.jena.shared.JenaException;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.ConcurrentModificationException;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class HashCommonBase<Key> {
    /**
     * Jeremy suggests, from his experiments, that load factors more than
     * 0.6 leave the table too dense, and little advantage is gained below 0.4.
     * Although that was with a quadratic probe, I'm borrowing the same
     * plausible range, and use 0.5 by default.
     */
    protected static final double loadFactor = 0.5;
    // Hash tables are 0.25 to 0.5 full so these numbers
    // are for storing about 1/3 of that number of items.
    // The larger sizes are added so that the system has "soft failure"
    // rather implying guaranteed performance.
    // https://primes.utm.edu/lists/small/millions/
    static final int[] primes = {7, 19, 37, 79, 149, 307, 617, 1237, 2477, 4957, 9923, 19_853, 39_709, 79_423, 158_849,
            317_701, 635_413, 1_270_849, 2_541_701, 5_083_423, 10_166_857, 20_333_759, 40_667_527, 81_335_047,
            162_670_111, 325_340_233, 650_680_469, 982_451_653 // 50 millionth prime - Largest at primes.utm.edu.
    };
    /**
     * The keys of whatever table it is we're implementing. Since we share code
     * for triple sets and for node->bunch maps, it has to be an Object array; we
     * take the casting hit.
     */
    protected Key[] keys;
    /**
     * The threshold number of elements above which we resize the table;
     * equal to the capacity times the load factor.
     */
    protected int threshold;
    /**
     * The number of active elements in the table, maintained incrementally.
     */
    protected int size = 0;

    public HashCommonBase(int initialCapacity) {
        keys = newKeysArray(initialCapacity);
        threshold = (int) (keys.length * loadFactor);
    }

    protected static int nextSize(int atLeast) {
        for (int prime : primes) {
            if (prime > atLeast) return prime;
        }
        //return atLeast ;        // Input is 2*current capacity.
        // There are some very large numbers in the primes table.
        throw new JenaException("Failed to find a 'next size': atleast = " + atLeast);
    }

    protected void clear(int initialCapacity) {
        size = 0;
        keys = newKeysArray(initialCapacity);
        threshold = (int) (keys.length * loadFactor);
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public abstract boolean containsKey(Key key);

    public ExtendedIterator<Key> keyIterator() {
        final var initialSize = size;
        final Runnable checkForConcurrentModification = () -> {
            if (size != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArrayIterator<>(keys, checkForConcurrentModification);
    }

    public Spliterator<Key> keySpliterator() {
        final var initialSize = size;
        final Runnable checkForConcurrentModification = () -> {
            if (size != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArraySpliterator<>(keys, checkForConcurrentModification);
    }

    public Stream<Key> keyStream() {
        return StreamSupport.stream(keySpliterator(), false);
    }

    /**
     * Subclasses must implement to answer a new Key[size] array.
     */
    protected abstract Key[] newKeysArray(int size);

    /**
     * Answer the initial index for the object <code>key</code> in the table.
     * With luck, this will be the final position for that object. The initial index
     * will always be non-negative and less than <code>capacity</code>.
     * <p>
     * Implementation note: do <i>not</i> use <code>Math.abs</code> to turn a
     * hashcode into a positive value; there is a single specific integer on which
     * it does not work. (Hence, here, the use of bitmasks.)
     */
    protected final int initialIndexFor(int hashOfKey) {
        return (improveHashCode(hashOfKey) & 0x7fffffff) % keys.length;
    }

    /**
     * Answer the transformed hash code, intended to be an improvement
     * on the objects own hashcode. The magic number 127 is performance
     * voodoo to (try to) eliminate problems experienced by Wolfgang.
     */
    protected int improveHashCode(int hashCode) {
        return hashCode * 127;
    }

    protected abstract void grow();

    /**
     * Work out the capacity and threshold sizes for a new improved bigger
     * table (bigger by a factor of two, at present).
     */
    protected int calcGrownCapacityAndSetThreshold() {
        final var capacity = HashCommonBase.nextSize(keys.length * 2);
        threshold = (int) (capacity * loadFactor);
        return capacity;
    }

    protected abstract void removeFrom(int here);
}

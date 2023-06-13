package org.apache.jena.mem2.collection.specialized;

import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.Predicate;

public class HashedIndexSet {
    protected static final int DEFAULT_INITIAL_INDICES_SIZE = 16;

    protected int count = 0;
    /**
     * The negative indices.
     * The positions in the array are derived from the hashCodes.
     * Any position 0 indicates an empty element.
     */
    protected int[] inverseIndices;

    public HashedIndexSet(int initialSize) {
        var positionsSize = Integer.highestOneBit(initialSize << 1);
        if (positionsSize < initialSize << 1) {
            positionsSize <<= 1;
        }
        this.inverseIndices = new int[positionsSize];
    }

    public HashedIndexSet() {
        this.inverseIndices = new int[DEFAULT_INITIAL_INDICES_SIZE];
    }

    private HashedIndexSet(final int[] inverseIndices, final int count) {
        this.inverseIndices = inverseIndices;
        this.count = count;
    }

    public static HashedIndexSet createSmall() {
        return new HashedIndexSet(2);
    }

    public static HashedIndexSet calcIntersection(HashedIndexSet smaller, HashedIndexSet larger) {
        var result = smaller.clone();
        var slot = result.size() - 1;
        while (-1 < slot) {
            if (0 != smaller.inverseIndices[slot] && !larger.contains(~result.inverseIndices[slot])) {
                result.inverseIndices[slot] = 0;
                result.count--;
            }
            slot--;
        }
        return result;
    }

    private static boolean intersects(HashedIndexSet smaller, HashedIndexSet larger) {
        var slot = smaller.inverseIndices.length - 1;
        var otherModBase = larger.inverseIndices.length - 1;
        while (-1 < slot) {
            if (0 != smaller.inverseIndices[slot]) {
                final var index = ~smaller.inverseIndices[slot];
                var otherSlot = index & otherModBase;
                while (0 != larger.inverseIndices[otherSlot]) {
                    if (index == ~larger.inverseIndices[otherSlot]) {
                        return true;
                    } else if (--otherSlot < 0) {
                        otherSlot += larger.inverseIndices.length;
                    }
                }
            }
            slot--;
        }
        return false;
    }

    public int any() {
        for (int i : inverseIndices) {
            if (i != 0) {
                return ~i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * Using the same hash code optimization as {@link java.util.HashMap#hash(Object)}
     *
     * @param index
     * @return
     */
    protected final int calcStartSlotForIndex(final int index) {
        return index & (inverseIndices.length - 1);
    }

    private int calcNewPositionsSize() {
        if (count << 1 > inverseIndices.length) { /*grow*/
            final var newLength = inverseIndices.length << 1;
            return newLength < 0 ? Integer.MAX_VALUE : newLength;
        }
        return -1;
    }

    protected final void growPositionsArrayIfNeeded() {
        final var newSize = calcNewPositionsSize();
        if (newSize < 0) {
            return;
        }
        final var oldPositions = this.inverseIndices;
        this.inverseIndices = new int[newSize];
        for (int oldPosition : oldPositions) {
            if (0 != oldPosition) {
                this.inverseIndices[findEmptySlotWithoutEqualityCheck(~oldPosition)] = oldPosition;
            }
        }
    }

    /**
     * Returns the number of elements in this collection.  If this collection
     * contains more than {@code Integer.MAX_VALUE} elements, returns
     * {@code Integer.MAX_VALUE}.
     *
     * @return the number of elements in this collection
     */
    public int size() {
        return count;
    }

    public boolean tryAdd(final int index) {
        growPositionsArrayIfNeeded();
        var slot = findSlot(index);
        if (slot < 0) {
            inverseIndices[~slot] = ~index;
            count++;
            return true;
        }
        return false;
    }

    public void addUnchecked(final int index) {
        growPositionsArrayIfNeeded();
        inverseIndices[findEmptySlotWithoutEqualityCheck(index)] = ~index;
        count++;
    }

    /**
     * Removes a single instance of the specified element from this
     * collection, if it is present (optional operation).  More formally,
     * removes an element {@code e} such that
     * {@code Objects.equals(o, e)}, if
     * this collection contains one or more such elements.  Returns
     * {@code true} if this collection contained the specified element (or
     * equivalently, if this collection changed as a result of the call).
     *
     * @param index element to be removed from this collection, if present
     * @return {@code true} if an element was removed as a result of this call
     * @throws ClassCastException            if the type of the specified element
     *                                       is incompatible with this collection
     *                                       (<a href="{@docRoot}/java.base/java/util/Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException          if the specified element is null and this
     *                                       collection does not permit null elements
     *                                       (<a href="{@docRoot}/java.base/java/util/Collection.html#optional-restrictions">optional</a>)
     * @throws UnsupportedOperationException if the {@code remove} operation
     *                                       is not supported by this collection
     */
    public final boolean tryRemove(final int index) {
        final var slot = findSlot(index);
        if (slot < 0) {
            return false;
        }
        removeFrom(slot);
        return true;
    }

    public final void removeUnchecked(final int i) {
        removeFrom(findSlot(i));
    }

    protected void removeFrom(int here) {
        count--;
        while (true) {
            inverseIndices[here] = 0;
            int scan = here;
            while (true) {
                if (--scan < 0) scan += inverseIndices.length;
                if (inverseIndices[scan] == 0) return;
                int r = calcStartSlotForIndex(~inverseIndices[scan]);
                if ((scan > r || r >= here) && (r >= here || here >= scan) && (here >= scan || scan > r)) {
                    inverseIndices[here] = inverseIndices[scan];
                    here = scan;
                    break;
                }
            }
        }
    }

    /**
     * Returns {@code true} if this collection contains no elements.
     *
     * @return {@code true} if this collection contains no elements
     */
    public final boolean isEmpty() {
        return count == 0;
    }

    public final boolean contains(int index) {
        var slot = calcStartSlotForIndex(index);
        while (true) {
            if (0 == inverseIndices[slot]) {
                return false;
            } else {
                if (index == ~inverseIndices[slot]) {
                    return true;
                } else if (--slot < 0) {
                    slot += inverseIndices.length;
                }
            }
        }
    }

    public final boolean anyMatch(Predicate<Integer> predicate) {
        for (int pos : inverseIndices) {
            if (0 != pos) {
                if (predicate.test(~pos)) {
                    return true;
                }
            }
        }
        return false;
    }

    public final ExtendedIterator<Integer> iterator() {
        final var initialSize = count;
        final Runnable checkForConcurrentModification = () ->
        {
            if (count != initialSize) throw new ConcurrentModificationException();
        };
        return new IndicesIterator(inverseIndices, checkForConcurrentModification);
    }

    protected final int findSlot(final int index) {
        var slot = calcStartSlotForIndex(index);
        while (true) {
            if (0 == inverseIndices[slot]) {
                return ~slot;
            } else {
                if (index == ~inverseIndices[slot]) {
                    return slot;
                } else if (--slot < 0) {
                    slot += inverseIndices.length;
                }
            }
        }
    }

    protected final int findEmptySlotWithoutEqualityCheck(final int hashCode) {
        var pIndex = calcStartSlotForIndex(hashCode);
        while (true) {
            if (0 == inverseIndices[pIndex]) {
                return pIndex;
            } else if (--pIndex < 0) {
                pIndex += inverseIndices.length;
            }
        }
    }

    /**
     * Removes all the elements from this collection (optional operation).
     * The collection will be empty after this method returns.
     *
     * @throws UnsupportedOperationException if the {@code clear} operation
     *                                       is not supported by this collection
     */
    public void clear() {
        inverseIndices = new int[DEFAULT_INITIAL_INDICES_SIZE];
        count = 0;
    }

    public final Spliterator<Integer> spliterator() {
        final var initialSize = count;
        final Runnable checkForConcurrentModification = () ->
        {
            if (count != initialSize) throw new ConcurrentModificationException();
        };
        return new IndicesSpliterator(inverseIndices, checkForConcurrentModification);
    }

    public HashedIndexSet clone() {
        return new HashedIndexSet(this.inverseIndices.clone(), this.count);
    }

    public HashedIndexSet calcIntersection(HashedIndexSet other) {
        if (this.count < other.count) {
            return calcIntersection(this, other);
        } else {
            return calcIntersection(other, this);
        }
    }

    public boolean intersects(HashedIndexSet other) {
        if (this.count < other.count) {
            return intersects(this, other);
        } else {
            return intersects(other, this);
        }
    }
}

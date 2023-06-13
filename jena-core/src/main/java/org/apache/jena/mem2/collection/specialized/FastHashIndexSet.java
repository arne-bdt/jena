package org.apache.jena.mem2.collection.specialized;

import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Spliterator;
import java.util.function.Predicate;

public class FastHashIndexSet {
    protected static final int MINIMUM_POSITION_SIZE = 16;
    protected static final int MINIMUM_INDICES_SIZE = 10;
    protected int indicesPos = 0;
    protected int[] indices;
    protected int lastDeletedIndicesPosInverse = 0;
    protected int removedIndicesCount = 0;
    /**
     * The negative indices to the entries and hashCode arrays.
     * The indices of the positions array are derived from the hashCodes.
     * Any position 0 indicates an empty element.
     */
    protected int[] positions;

    public FastHashIndexSet(int initialSize) {
        var positionsSize = Integer.highestOneBit(initialSize << 1);
        if (positionsSize < initialSize << 1) {
            positionsSize <<= 1;
        }
        this.positions = new int[positionsSize];
        this.indices = new int[initialSize];
    }

    public FastHashIndexSet() {
        this.positions = new int[MINIMUM_POSITION_SIZE];
        this.indices = new int[MINIMUM_INDICES_SIZE];
    }

    /**
     * Using the same hash code optimization as {@link java.util.HashMap#hash(Object)}
     *
     * @param index
     * @return
     */
    protected final int calcStartSlotForIndex(final int index) {
        return index & (positions.length - 1);
    }

    private int calcNewPositionsSize() {
        if (indicesPos << 1 > positions.length) { /*grow*/
            final var newLength = positions.length << 1;
            return newLength < 0 ? Integer.MAX_VALUE : newLength;
        }
        return -1;
    }

    protected final void growPositionsArrayIfNeeded() {
        final var newSize = calcNewPositionsSize();
        if (newSize < 0) {
            return;
        }
        final var oldPositions = this.positions;
        this.positions = new int[newSize];
        for (int oldPosition : oldPositions) {
            if (0 != oldPosition) {
                this.positions[findEmptySlotWithoutEqualityCheck(indices[~oldPosition])] = oldPosition;
            }
        }
    }

    protected final boolean tryGrowPositionsArrayIfNeeded() {
        final var newSize = calcNewPositionsSize();
        if (newSize < 0) {
            return false;
        }
        final var oldPositions = this.positions;
        this.positions = new int[newSize];
        for (int oldPosition : oldPositions) {
            if (0 != oldPosition) {
                this.positions[findEmptySlotWithoutEqualityCheck(indices[~oldPosition])] = oldPosition;
            }
        }
        return true;
    }

    /**
     * Returns the number of elements in this collection.  If this collection
     * contains more than {@code Integer.MAX_VALUE} elements, returns
     * {@code Integer.MAX_VALUE}.
     *
     * @return the number of elements in this collection
     */
    public int size() {
        return indicesPos - removedIndicesCount;
    }

    protected final int getFreeIndexSlot() {
        final int index;
        if (lastDeletedIndicesPosInverse == 0) {
            index = indicesPos++;
            if (index == indices.length) {
                growIndexArray();
            }
        } else {
            index = ~lastDeletedIndicesPosInverse;
            lastDeletedIndicesPosInverse = index == indices[index] ? 0 : indices[index];
            removedIndicesCount--;
        }
        return index;
    }

    protected void growIndexArray() {
        var newSize = (indices.length >> 1) + indices.length;
        if (newSize < 0) {
            newSize = Integer.MAX_VALUE;
        }
        this.indices = Arrays.copyOf(this.indices, newSize);
    }

    public boolean tryAdd(final int index) {
        var pSlot = findPositionsSlot(index);
        if (pSlot < 0) {
            final var iSlot = getFreeIndexSlot();
            indices[iSlot] = index;
            positions[~pSlot] = ~iSlot;
            growPositionsArrayIfNeeded();
            return true;
        }
        return false;
    }

    public void addUnchecked(final int index) {
        final var iSlot = getFreeIndexSlot();
        indices[iSlot] = index;
        positions[findEmptySlotWithoutEqualityCheck(index)] = ~iSlot;
        growPositionsArrayIfNeeded();
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
        final var pSlot = findPositionsSlot(index);
        if (pSlot < 0) {
            return false;
        }
        removeFrom(pSlot);
        return true;
    }

    public final void removeUnchecked(final int i) {
        removeFrom(findPositionsSlot(i));
    }

    protected void removeFrom(int here) {
        final var pIndex = ~positions[here];
        indices[pIndex] = lastDeletedIndicesPosInverse == 0 ? ~pIndex : lastDeletedIndicesPosInverse;
        lastDeletedIndicesPosInverse = ~pIndex;
        removedIndicesCount++;
        while (true) {
            positions[here] = 0;
            int scan = here;
            while (true) {
                if (--scan < 0) scan += positions.length;
                if (positions[scan] == 0) return;
                int r = calcStartSlotForIndex(indices[~positions[scan]]);
                if ((scan > r || r >= here) && (r >= here || here >= scan) && (here >= scan || scan > r)) {
                    positions[here] = positions[scan];
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
        return this.size() == 0;
    }

    public final boolean contains(int index) {
        var pIndex = calcStartSlotForIndex(index);
        while (true) {
            if (0 == positions[pIndex]) {
                return false;
            } else {
                if (index == indices[~positions[pIndex]]) {
                    return true;
                } else if (--pIndex < 0) {
                    pIndex += positions.length;
                }
            }
        }
    }

    public final boolean anyMatch(Predicate<Integer> predicate) {
        for (int pos : positions) {
            if (0 != pos) {
                if (predicate.test(indices[~pos])) {
                    return true;
                }
            }
        }
        return false;
    }

    public final ExtendedIterator<Integer> interator() {
        final var initialSize = size();
        final Runnable checkForConcurrentModification = () ->
        {
            if (size() != initialSize) throw new ConcurrentModificationException();
        };
        return new IndicesIterator(indices, indicesPos, checkForConcurrentModification);
    }

    protected final int findPositionsSlot(final int i) {
        var pIndex = calcStartSlotForIndex(i);
        while (true) {
            if (0 == positions[pIndex]) {
                return ~pIndex;
            } else {
                final var pos = ~positions[pIndex];
                if (i == indices[pos]) {
                    return pIndex;
                } else if (--pIndex < 0) {
                    pIndex += positions.length;
                }
            }
        }
    }

    protected final int findEmptySlotWithoutEqualityCheck(final int hashCode) {
        var pIndex = calcStartSlotForIndex(hashCode);
        while (true) {
            if (0 == positions[pIndex]) {
                return pIndex;
            } else if (--pIndex < 0) {
                pIndex += positions.length;
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
        positions = new int[MINIMUM_POSITION_SIZE];
        indices = new int[MINIMUM_INDICES_SIZE];
        indicesPos = 0;
        lastDeletedIndicesPosInverse = 0;
        removedIndicesCount = 0;
    }

    public final Spliterator<Integer> spliterator() {
        final var initialSize = this.size();
        final Runnable checkForConcurrentModification = () ->
        {
            if (this.size() != initialSize) throw new ConcurrentModificationException();
        };
        return new IndicesSpliterator(indices, 0, indicesPos, checkForConcurrentModification);
    }
}

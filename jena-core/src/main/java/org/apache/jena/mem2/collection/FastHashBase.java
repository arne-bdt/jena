package org.apache.jena.mem2.collection;

import org.apache.jena.mem2.iterator.SparseArrayIterator;
import org.apache.jena.mem2.spliterator.SparseArraySubSpliterator;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.ConcurrentModificationException;
import java.util.Spliterator;
import java.util.function.Predicate;

public abstract class FastHashBase<K> implements JenaMapSetCommon<K> {
    protected static final int MINIMUM_HASHES_SIZE = 16;
    protected static final int MINIMUM_ELEMENTS_SIZE = 10;
    protected int keysPos = 0;
    protected K[] keys;
    protected int[] hashCodesOrDeletedIndices;
    protected int lastDeletedIndex = -1;
    protected int removedKeysCount = 0;

    /**
     * The negative indices to the entries and hashCode arrays.
     * The indices of the positions array are derived from the hashCodes.
     * Any position 0 indicates an empty element.
     */
    protected int[] positions;

    protected FastHashBase(int initialSize) {
        var positionsSize = Integer.highestOneBit(initialSize << 1);
        if(positionsSize < initialSize << 1) {
            positionsSize <<= 1;
        }
        this.positions = new int[positionsSize];
        this.keys = newKeysArray(initialSize);
        this.hashCodesOrDeletedIndices = new int[initialSize];
    }

    protected FastHashBase() {
        this.positions = new int[MINIMUM_HASHES_SIZE];
        this.keys = newKeysArray(MINIMUM_ELEMENTS_SIZE);
        this.hashCodesOrDeletedIndices = new int[MINIMUM_ELEMENTS_SIZE];

    }

    protected abstract K[] newKeysArray(int size);

    protected final int calcStartIndexByHashCode(final int hashCode) {
        return hashCode & (positions.length - 1);
    }

    private int calcNewPositionsSize() {
        if (keysPos << 1 > positions.length) { /*grow*/
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
                this.positions[findEmptySlotWithoutEqualityCheck(hashCodesOrDeletedIndices[~oldPosition])] = oldPosition;
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
                this.positions[findEmptySlotWithoutEqualityCheck(hashCodesOrDeletedIndices[~oldPosition])] = oldPosition;
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
    @Override
    public int size() {
        return keysPos - removedKeysCount;
    }

    protected final int getFreeKeyIndex() {
        final int index;
        if (lastDeletedIndex == -1) {
            index = keysPos++;
            if (index == keys.length) {
                growKeysAndHashCodeArrays();
            }
        } else {
            index = lastDeletedIndex;
            lastDeletedIndex = hashCodesOrDeletedIndices[lastDeletedIndex];
            removedKeysCount--;
        }
        return index;
    }

    protected void growKeysAndHashCodeArrays() {
        var newSize = (keys.length >> 1) + keys.length;
        if (newSize < 0) {
            newSize = Integer.MAX_VALUE;
        }
        final var oldKeys = this.keys;
        this.keys = newKeysArray(newSize);
        System.arraycopy(oldKeys, 0, keys, 0, oldKeys.length);
        final var oldHashCodes = this.hashCodesOrDeletedIndices;
        this.hashCodesOrDeletedIndices = new int[newSize];
        System.arraycopy(oldHashCodes, 0, hashCodesOrDeletedIndices, 0, oldHashCodes.length);
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
     * @param o element to be removed from this collection, if present
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
    @Override
    public final boolean tryRemove(K o) {
        return tryRemove(o, o.hashCode());
    }

    public final boolean tryRemove(K e, int hashCode) {
        final var index = findPosition(e, hashCode);
        if (index < 0) {
            return false;
        }
        removeFrom(index);
        return true;
    }

    /**
     * Removes the given element and returns its index.
     * If the element is not found, returns -1.
     */
    public final int removeAndGetIndex(final K e) {
        return removeAndGetIndex(e, e.hashCode());
    }

    public final int removeAndGetIndex(final K e, final int hashCode) {
        final var pIndex = findPosition(e, hashCode);
        if (pIndex < 0) {
            return -1;
        }
        final var eIndex = ~positions[pIndex];
        removeFrom(pIndex);
        return eIndex;
    }

    @Override
    public final void removeUnchecked(K e) {
        removeUnchecked(e, e.hashCode());
    }

    public final void removeUnchecked(K e, int hashCode) {
        removeFrom(findPosition(e, hashCode));
    }

    protected void removeFrom(int here) {
        final var pIndex = ~positions[here];
        hashCodesOrDeletedIndices[pIndex] = lastDeletedIndex;
        lastDeletedIndex = pIndex;
        removedKeysCount++;
        keys[pIndex] = null;
        while (true) {
            positions[here] = 0;
            int scan = here;
            while (true) {
                if (--scan < 0) scan += positions.length;
                if (positions[scan] == 0) return;
                int r = calcStartIndexByHashCode(hashCodesOrDeletedIndices[~positions[scan]]);
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
    @Override
    public final boolean isEmpty() {
        return this.size() == 0;
    }

    @Override
    public final boolean containsKey(K o) {
        final int hashCode = o.hashCode();
        var pIndex = calcStartIndexByHashCode(hashCode);
        while (true) {
            if (0 == positions[pIndex]) {
                return false;
            } else {
                final var eIndex = ~positions[pIndex];
                if (hashCode == hashCodesOrDeletedIndices[eIndex] && o.equals(keys[eIndex])) {
                    return true;
                } else if (--pIndex < 0) {
                    pIndex += positions.length;
                }
            }
        }
    }

    @Override
    public final boolean anyMatch(Predicate<K> predicate) {
        var pos = keysPos - 1;
        while (-1 < pos) {
            if (null != keys[pos] && predicate.test(keys[pos])) {
                return true;
            }
            pos--;
        }
        return false;
    }

    @Override
    public final ExtendedIterator<K> keyIterator() {
        final var initialSize = size();
        final Runnable checkForConcurrentModification = () ->
        {
            if (size() != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArrayIterator<>(keys, keysPos, checkForConcurrentModification);
    }

    protected final int findPosition(final K e, final int hashCode) {
        var pIndex = calcStartIndexByHashCode(hashCode);
        while (true) {
            if (0 == positions[pIndex]) {
                return ~pIndex;
            } else {
                final var pos = ~positions[pIndex];
                if (hashCode == hashCodesOrDeletedIndices[pos] && e.equals(keys[pos])) {
                    return pIndex;
                } else if (--pIndex < 0) {
                    pIndex += positions.length;
                }
            }
        }
    }

    protected final int findEmptySlotWithoutEqualityCheck(final int hashCode) {
        var pIndex = calcStartIndexByHashCode(hashCode);
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
    @Override
    public void clear() {
        positions = new int[MINIMUM_HASHES_SIZE];
        keys = newKeysArray(MINIMUM_ELEMENTS_SIZE);
        hashCodesOrDeletedIndices = new int[MINIMUM_ELEMENTS_SIZE];
        keysPos = 0;
        lastDeletedIndex = -1;
        removedKeysCount = 0;
    }

    @Override
    public final Spliterator<K> keySpliterator() {
        final var initialSize = this.size();
        final Runnable checkForConcurrentModification = () ->
        {
            if (this.size() != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArraySubSpliterator<>(keys, 0, keysPos, checkForConcurrentModification);
    }
}

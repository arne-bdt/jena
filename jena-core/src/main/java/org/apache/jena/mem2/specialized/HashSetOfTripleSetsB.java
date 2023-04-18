/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.mem2.specialized;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class HashSetOfTripleSetsB {

    private int calcStartIndexByHashCode(final int hashCode) {
        return (hashCode ^ (hashCode >>> 16)) & (entries.length-1);
    }

    private static int MINIMUM_SIZE = 16;
    private static float loadFactor = 0.5f;
    private transient int size = 0;
    private transient TripleSetWithIndexingValue[] entries;
    private transient int[] hashCodes;
    private transient boolean[] used;

    public HashSetOfTripleSetsB() {
        this.entries = new TripleSetWithIndexingValue[MINIMUM_SIZE];
        this.hashCodes = new int[MINIMUM_SIZE];
        this.used = new boolean[MINIMUM_SIZE];
    }

    public HashSetOfTripleSetsB(int initialCapacity) {
        this.entries = new TripleSetWithIndexingValue[Integer.highestOneBit(((int)(initialCapacity/loadFactor)+1)) << 1];
        this.hashCodes = new int[entries.length];
        this.used = new boolean[entries.length];
    }

    public HashSetOfTripleSetsB(Set<TripleSetWithIndexingValue> set) {
        this(set.size(), set);
    }

    public HashSetOfTripleSetsB(int initialCapacity, Set<TripleSetWithIndexingValue> set) {
        this.entries = new TripleSetWithIndexingValue[Integer.highestOneBit(((int)(Math.max(set.size(), initialCapacity)/loadFactor)+1)) << 1];
        this.hashCodes = new int[entries.length];
        this.used = new boolean[entries.length];
        int index, hashCode;
        Object indexingValue;
        for (TripleSetWithIndexingValue e : set) {
            if((index = findIndex(indexingValue = e.getIndexingValue(), hashCode = indexingValue.hashCode())) < 0) {
                entries[index = ~index] = e;
                hashCodes[index] = hashCode;
                used[index] = true;
                size++;
            }
        }
    }

    private int calcNewSize() {
        if(size >= entries.length*loadFactor && entries.length <= 1 << 30) { /*grow*/
            return entries.length << 1;
        }
        return -1;
    }

    private boolean grow() {
        final var newSize = calcNewSize();
        if(newSize < 0) {
            return false;
        }
        final var oldEntries = this.entries;
        final var oldHashCodes = this.hashCodes;
        final var oldUsed = this.used;
        this.entries = new TripleSetWithIndexingValue[newSize];
        this.hashCodes = new int[newSize];
        this.used = new boolean[newSize];
        for(int i=0; i<oldEntries.length; i++) {
            if(oldUsed[i]) {
                var newSlot = findEmptySlotWithoutEqualityCheck(oldHashCodes[i]);
                this.entries[newSlot] = oldEntries[i];
                this.hashCodes[newSlot] = oldHashCodes[i];
                this.used[newSlot] = true;
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
        return size;
    }

    /**
     * Returns {@code true} if this collection contains no elements.
     *
     * @return {@code true} if this collection contains no elements
     */
    public boolean isEmpty() {
        return size == 0;
    }

    public boolean contains(Object o) {
        var e = (TripleSetWithIndexingValue)o;
        final var key = e.getIndexingValue();
        final var hashCode = key.hashCode();
        var index = calcStartIndexByHashCode(hashCode);
        while(used[index]) {
            if(hashCode == hashCodes[index] && key.equals(entries[index].getIndexingValue())) {
                return true;
            } else if (--index < 0){
                index += entries.length;
            }
        }
        return false;
    }

    public Iterator<TripleSetWithIndexingValue> iterator() {
        return new ArrayWithNullsIterator(entries, used, size);
    }

    public TripleSetWithIndexingValue getIfPresent(final Object indexingValue) {
        var hashCode = indexingValue.hashCode();
        var index = calcStartIndexByHashCode(hashCode);
        while(used[index]) {
            if(hashCode == hashCodes[index] && indexingValue.equals(entries[index].getIndexingValue())) {
                return entries[index];
            } else if(--index < 0){
                index += entries.length;
            }
        }
        return null;
    }

    public TripleSetWithIndexingValue compute(final Object key, final int hashCodeOfKey, Function<TripleSetWithIndexingValue, TripleSetWithIndexingValue> remappingFunction) {
        var index = findIndex(key, hashCodeOfKey);
        if(index < 0) { /*value does not exist yet*/
            var newValue = remappingFunction.apply(null);
            if(newValue == null) {
                return null;
            }
            if(grow()) {
                index = findEmptySlotWithoutEqualityCheck(hashCodeOfKey);
            } else {
                index = ~index;
            }
            entries[index] = newValue;
            hashCodes[index] = hashCodeOfKey;
            used[index] = true;
            size++;
            return newValue;
        } else { /*existing value found*/
            var newValue = remappingFunction.apply(entries[index]);
            if(newValue == null) {
                entries[index] = null;
                used[index] = false;
                size--;
                rearrangeNeighbours(index);
                return null;
            } else {
                entries[index] = newValue;
                return newValue;
            }
        }
    }

    public TripleSetWithIndexingValue compute(final Object key, Function<TripleSetWithIndexingValue, TripleSetWithIndexingValue> remappingFunction) {
        var hashCodeOfKey = key.hashCode();
        var index = findIndex(key, hashCodeOfKey);
        if(index < 0) { /*value does not exist yet*/
            var newValue = remappingFunction.apply(null);
            if(newValue == null) {
                return null;
            }
            if(grow()) {
                index = findEmptySlotWithoutEqualityCheck(hashCodeOfKey);
            } else {
                index = ~index;
            }
            entries[index] = newValue;
            hashCodes[index] = hashCodeOfKey;
            used[index] = true;
            size++;
            return newValue;
        } else { /*existing value found*/
            var newValue = remappingFunction.apply(entries[index]);
            if(newValue == null) {
                entries[index] = null;
                used[index] = false;
                size--;
                rearrangeNeighbours(index);
                return null;
            } else {
                entries[index] = newValue;
                return newValue;
            }
        }
    }

    private int findIndex(final Object key, final int hashCode) {
        var index = calcStartIndexByHashCode(hashCode);
        while(used[index]) {
            if(hashCode == hashCodes[index] && key.equals(entries[index].getIndexingValue())) {
                return index;
            } else if(--index < 0){
                index += entries.length;
            }
        }
        return ~index;
    }

    private int findEmptySlotWithoutEqualityCheck(final int hashCode) {
        var index = calcStartIndexByHashCode(hashCode);
        while(used[index]) {
            if(--index < 0){
                index += entries.length;
            }
        }
        return index;
    }

    private void rearrangeNeighbours(int index) {
        /*rearrange neighbours*/
        var neighbours = getNeighbours(index);
        if(neighbours == null) {
            return;
        }
        Arrays.sort(neighbours, ObjectsWithStartIndexIndexAndDistance.distanceComparator);
        boolean elementsHaveBeenSwitched;
        do {
            elementsHaveBeenSwitched = false;
            for (ObjectsWithStartIndexIndexAndDistance neighbour : neighbours) {
                if(neighbour.distance == 0) {
                    break;
                }
                if (neighbour.isTargetIndexNearerToStartIndex(index)){
                    var oldIndexOfNeighbour = neighbour.currentIndex;
                    entries[index] = entries[oldIndexOfNeighbour];
                    hashCodes[index] = hashCodes[oldIndexOfNeighbour];
                    used[index] = true;
                    entries[oldIndexOfNeighbour] = null;
                    used[oldIndexOfNeighbour] = false;
                    neighbour.setCurrentIndex(index);
                    index = oldIndexOfNeighbour;
                    Arrays.sort(neighbours, ObjectsWithStartIndexIndexAndDistance.distanceComparator);
                    elementsHaveBeenSwitched = true;
                    break;
                }
            }
        } while(elementsHaveBeenSwitched);
    }

    private ObjectsWithStartIndexIndexAndDistance[] getNeighbours(final int index) {
        var neighbours = new ArrayList<ObjectsWithStartIndexIndexAndDistance>();
        ObjectsWithStartIndexIndexAndDistance neighbour;
        /*find left*/
        var i = index;
        while(true) {
            if(--i < 0){
                i += entries.length;
            }
            if(!used[i]) {
                break;
            } else {
                neighbour = new ObjectsWithStartIndexIndexAndDistance(entries.length, calcStartIndexByHashCode(hashCodes[i]), i);
                if(neighbour.distance > 0) {
                    neighbours.add(neighbour);
                }
            }
        }
        i = index;
        /*find right*/
        while(true) {
            if(++i == entries.length){
                i = 0;
            }
            if(!used[i]) {
                break;
            } else {
                neighbour = new ObjectsWithStartIndexIndexAndDistance(entries.length, calcStartIndexByHashCode(hashCodes[i]), i);
                if(neighbour.distance > 0) {
                    neighbours.add(neighbour);
                }
            }
        }
        return neighbours.isEmpty()
                ? null : neighbours.toArray(new ObjectsWithStartIndexIndexAndDistance[neighbours.size()]);
    }

    private static class ObjectsWithStartIndexIndexAndDistance {
        public final static Comparator<ObjectsWithStartIndexIndexAndDistance>  distanceComparator
                = Comparator.comparingInt((ObjectsWithStartIndexIndexAndDistance n) -> n.distance).reversed();
        final int startIndex;
        final int length;
        int currentIndex;
        int distance;

        public ObjectsWithStartIndexIndexAndDistance(final int length, final int startIndex, final int currentIndex) {
            this.length = length;
            this.startIndex = startIndex;
            this.setCurrentIndex(currentIndex);
        }

        void setCurrentIndex(final int currentIndex) {
            this.currentIndex = currentIndex;
            this.distance = calcDistance(currentIndex);
        }

        private int calcDistance(final int index) {
            return index <= startIndex
                    ? startIndex - index
                    : startIndex + length - index;
        }

        boolean isTargetIndexNearerToStartIndex(final int targetIndex) {
            return calcDistance(targetIndex) < distance;
        }
    }

    /**
     * Removes all of the elements from this collection (optional operation).
     * The collection will be empty after this method returns.
     *
     * @throws UnsupportedOperationException if the {@code clear} operation
     *                                       is not supported by this collection
     */
    public void clear() {
        entries = new TripleSetWithIndexingValue[MINIMUM_SIZE];
        hashCodes = new int[MINIMUM_SIZE];
        used = new boolean[MINIMUM_SIZE];
        size = 0;
    }


    public Stream<TripleSetWithIndexingValue> stream() {
        return StreamSupport.stream(new ArrayWithNullsSpliteratorSized(entries, used, size), false);
    }


    public Stream<TripleSetWithIndexingValue> parallelStream() {
        return StreamSupport.stream(new ArrayWithNullsSpliteratorSized(entries, used, size), true);
    }

    private static class ArrayWithNullsSpliteratorSized implements Spliterator<TripleSetWithIndexingValue> {

        private final TripleSetWithIndexingValue[] entries;
        private final boolean[] used;
        private final int maxPos;
        private int pos = -1;
        private int maxRemaining;
        private boolean hasBeenSplit = false;

        public ArrayWithNullsSpliteratorSized(final TripleSetWithIndexingValue[] entries, boolean[] used, final int size) {
            this.entries = entries;
            this.used = used;
            this.maxPos = entries.length - 1;
            this.maxRemaining = size;
        }

        /**
         * If a remaining element exists, performs the given action on it,
         * returning {@code true}; else returns {@code false}.  If this
         * Spliterator is {@link #ORDERED} the action is performed on the
         * next element in encounter order.  Exceptions thrown by the
         * action are relayed to the caller.
         *
         * @param action The action
         * @return {@code false} if no remaining elements existed
         * upon entry to this method, else {@code true}.
         * @throws NullPointerException if the specified action is null
         */
        @Override
        public boolean tryAdvance(Consumer<? super TripleSetWithIndexingValue> action) {
            if(0 < maxRemaining) {
                while (pos < maxPos) {
                    if (used[++pos]) {
                        maxRemaining--;
                        action.accept(entries[pos]);
                        return true;
                    }
                }
            }
            return false;
        }

        /**
         * Performs the given action for each remaining element, sequentially in
         * the current thread, until all elements have been processed or the action
         * throws an exception.  If this Spliterator is {@link #ORDERED}, actions
         * are performed in encounter order.  Exceptions thrown by the action
         * are relayed to the caller.
         *
         * @param action The action
         * @throws NullPointerException if the specified action is null
         * @implSpec The default implementation repeatedly invokes {@link #tryAdvance} until
         * it returns {@code false}.  It should be overridden whenever possible.
         */
        @Override
        public void forEachRemaining(Consumer<? super TripleSetWithIndexingValue> action) {
            while(0 < maxRemaining && pos < maxPos) {
                if(used[++pos]) {
                    maxRemaining--;
                    action.accept(entries[pos]);
                }
            }
        }

        /**
         * If this spliterator can be partitioned, returns a Spliterator
         * covering elements, that will, upon return from this method, not
         * be covered by this Spliterator.
         *
         * <p>If this Spliterator is {@link #ORDERED}, the returned Spliterator
         * must cover a strict prefix of the elements.
         *
         * <p>Unless this Spliterator covers an infinite number of elements,
         * repeated calls to {@code trySplit()} must eventually return {@code null}.
         * Upon non-null return:
         * <ul>
         * <li>the value reported for {@code estimateSize()} before splitting,
         * must, after splitting, be greater than or equal to {@code estimateSize()}
         * for this and the returned Spliterator; and</li>
         * <li>if this Spliterator is {@code SUBSIZED}, then {@code estimateSize()}
         * for this spliterator before splitting must be equal to the sum of
         * {@code estimateSize()} for this and the returned Spliterator after
         * splitting.</li>
         * </ul>
         *
         * <p>This method may return {@code null} for any reason,
         * including emptiness, inability to split after traversal has
         * commenced, data structure constraints, and efficiency
         * considerations.
         *
         * @return a {@code Spliterator} covering some portion of the
         * elements, or {@code null} if this spliterator cannot be split
         * @apiNote An ideal {@code trySplit} method efficiently (without
         * traversal) divides its elements exactly in half, allowing
         * balanced parallel computation.  Many departures from this ideal
         * remain highly effective; for example, only approximately
         * splitting an approximately balanced tree, or for a tree in
         * which leaf nodes may contain either one or two elements,
         * failing to further split these nodes.  However, large
         * deviations in balance and/or overly inefficient {@code
         * trySplit} mechanics typically result in poor parallel
         * performance.
         */
        @Override
        public Spliterator<TripleSetWithIndexingValue> trySplit() {
            if(entries.length - pos < 10) {
                return null;
            }
            var mid = (pos + 1 + entries.length) >>> 1;
            var remainingInSubSpliterator = this.maxRemaining;
            var fromIndexForSubSpliterator = pos + 1;
            this.pos = mid - 1;
            this.maxRemaining = Math.min(this.maxRemaining, entries.length- pos);
            remainingInSubSpliterator = Math.min(remainingInSubSpliterator, mid-fromIndexForSubSpliterator);
            this.hasBeenSplit = true;
            return new ArrayWithNullsSubSpliteratorUnSized(entries, used, fromIndexForSubSpliterator, mid, remainingInSubSpliterator);
        }

        /**
         * Returns an estimate of the number of elements that would be
         * encountered by a {@link #forEachRemaining} traversal, or returns {@link
         * Long#MAX_VALUE} if infinite, unknown, or too expensive to compute.
         *
         * <p>If this Spliterator is {@link #SIZED} and has not yet been partially
         * traversed or split, or this Spliterator is {@link #SUBSIZED} and has
         * not yet been partially traversed, this estimate must be an accurate
         * count of elements that would be encountered by a complete traversal.
         * Otherwise, this estimate may be arbitrarily inaccurate, but must decrease
         * as specified across invocations of {@link #trySplit}.
         *
         * @return the estimated size, or {@code Long.MAX_VALUE} if infinite,
         * unknown, or too expensive to compute.
         * @apiNote Even an inexact estimate is often useful and inexpensive to compute.
         * For example, a sub-spliterator of an approximately balanced binary tree
         * may return a value that estimates the number of elements to be half of
         * that of its parent; if the root Spliterator does not maintain an
         * accurate count, it could estimate size to be the power of two
         * corresponding to its maximum depth.
         */
        @Override
        public long estimateSize() {
            return maxRemaining;
        }

        /**
         * Returns a set of characteristics of this Spliterator and its
         * elements. The result is represented as ORed values from {@link
         * #ORDERED}, {@link #DISTINCT}, {@link #SORTED}, {@link #SIZED},
         * {@link #NONNULL}, {@link #IMMUTABLE}, {@link #CONCURRENT},
         * {@link #SUBSIZED}.  Repeated calls to {@code characteristics()} on
         * a given spliterator, prior to or in-between calls to {@code trySplit},
         * should always return the same result.
         *
         * <p>If a Spliterator reports an inconsistent set of
         * characteristics (either those returned from a single invocation
         * or across multiple invocations), no guarantees can be made
         * about any computation using this Spliterator.
         *
         * @return a representation of characteristics
         * @apiNote The characteristics of a given spliterator before splitting
         * may differ from the characteristics after splitting.  For specific
         * examples see the characteristic values {@link #SIZED}, {@link #SUBSIZED}
         * and {@link #CONCURRENT}.
         */
        @Override
        public int characteristics() {
            if(this.hasBeenSplit) {
                return DISTINCT |NONNULL | IMMUTABLE;
            } else {
                return DISTINCT | SIZED | NONNULL | IMMUTABLE;
            }
        }
    }

    private static class ArrayWithNullsSubSpliteratorUnSized implements Spliterator<TripleSetWithIndexingValue> {

        private final TripleSetWithIndexingValue[] entries;
        private final boolean[] used;
        private int pos;
        private final int maxPos;
        private int maxRemaining;

        public ArrayWithNullsSubSpliteratorUnSized(final TripleSetWithIndexingValue[] entries, boolean[] used, final int fromIndex, final int toIndex, final int maxSize) {
            this.entries = entries;
            this.used = used;
            this.maxRemaining = maxSize;
            this.pos = fromIndex - 1;
            this.maxPos = toIndex - 1;
            this.updateMaxRemaining();
        }

        private void updateMaxRemaining() {
            this.maxRemaining = Math.min(this.maxRemaining, entries.length-(maxPos - pos));
        }

        /**
         * If a remaining element exists, performs the given action on it,
         * returning {@code true}; else returns {@code false}.  If this
         * Spliterator is {@link #ORDERED} the action is performed on the
         * next element in encounter order.  Exceptions thrown by the
         * action are relayed to the caller.
         *
         * @param action The action
         * @return {@code false} if no remaining elements existed
         * upon entry to this method, else {@code true}.
         * @throws NullPointerException if the specified action is null
         */
        @Override
        public boolean tryAdvance(Consumer<? super TripleSetWithIndexingValue> action) {
            if(0 < maxRemaining) {
                while (pos < maxPos) {
                    if (used[++pos]) {
                        maxRemaining--;
                        action.accept(entries[pos]);
                        return true;
                    }
                }
            }
            return false;
        }

        /**
         * Performs the given action for each remaining element, sequentially in
         * the current thread, until all elements have been processed or the action
         * throws an exception.  If this Spliterator is {@link #ORDERED}, actions
         * are performed in encounter order.  Exceptions thrown by the action
         * are relayed to the caller.
         *
         * @param action The action
         * @throws NullPointerException if the specified action is null
         * @implSpec The default implementation repeatedly invokes {@link #tryAdvance} until
         * it returns {@code false}.  It should be overridden whenever possible.
         */
        @Override
        public void forEachRemaining(Consumer<? super TripleSetWithIndexingValue> action) {
            while(0 < maxRemaining && pos < maxPos) {
                if(used[++pos]) {
                    maxRemaining--;
                    action.accept(entries[pos]);
                }
            }
        }

        /**
         * If this spliterator can be partitioned, returns a Spliterator
         * covering elements, that will, upon return from this method, not
         * be covered by this Spliterator.
         *
         * <p>If this Spliterator is {@link #ORDERED}, the returned Spliterator
         * must cover a strict prefix of the elements.
         *
         * <p>Unless this Spliterator covers an infinite number of elements,
         * repeated calls to {@code trySplit()} must eventually return {@code null}.
         * Upon non-null return:
         * <ul>
         * <li>the value reported for {@code estimateSize()} before splitting,
         * must, after splitting, be greater than or equal to {@code estimateSize()}
         * for this and the returned Spliterator; and</li>
         * <li>if this Spliterator is {@code SUBSIZED}, then {@code estimateSize()}
         * for this spliterator before splitting must be equal to the sum of
         * {@code estimateSize()} for this and the returned Spliterator after
         * splitting.</li>
         * </ul>
         *
         * <p>This method may return {@code null} for any reason,
         * including emptiness, inability to split after traversal has
         * commenced, data structure constraints, and efficiency
         * considerations.
         *
         * @return a {@code Spliterator} covering some portion of the
         * elements, or {@code null} if this spliterator cannot be split
         * @apiNote An ideal {@code trySplit} method efficiently (without
         * traversal) divides its elements exactly in half, allowing
         * balanced parallel computation.  Many departures from this ideal
         * remain highly effective; for example, only approximately
         * splitting an approximately balanced tree, or for a tree in
         * which leaf nodes may contain either one or two elements,
         * failing to further split these nodes.  However, large
         * deviations in balance and/or overly inefficient {@code
         * trySplit} mechanics typically result in poor parallel
         * performance.
         */
        @Override
        public Spliterator<TripleSetWithIndexingValue> trySplit() {
            if(maxPos - pos < 10) {
                return null;
            }
            var mid = (pos + maxPos + 2) >>> 1;
            var remainingInSubSpliterator = this.maxRemaining;
            var fromIndexForSubSpliterator = pos +1;
            this.pos = mid - 1;
            this.updateMaxRemaining();
            remainingInSubSpliterator = Math.min(remainingInSubSpliterator, mid-fromIndexForSubSpliterator);
            return new ArrayWithNullsSubSpliteratorUnSized(entries, used, fromIndexForSubSpliterator, mid, remainingInSubSpliterator);
        }

        /**
         * Returns an estimate of the number of elements that would be
         * encountered by a {@link #forEachRemaining} traversal, or returns {@link
         * Long#MAX_VALUE} if infinite, unknown, or too expensive to compute.
         *
         * <p>If this Spliterator is {@link #SIZED} and has not yet been partially
         * traversed or split, or this Spliterator is {@link #SUBSIZED} and has
         * not yet been partially traversed, this estimate must be an accurate
         * count of elements that would be encountered by a complete traversal.
         * Otherwise, this estimate may be arbitrarily inaccurate, but must decrease
         * as specified across invocations of {@link #trySplit}.
         *
         * @return the estimated size, or {@code Long.MAX_VALUE} if infinite,
         * unknown, or too expensive to compute.
         * @apiNote Even an inexact estimate is often useful and inexpensive to compute.
         * For example, a sub-spliterator of an approximately balanced binary tree
         * may return a value that estimates the number of elements to be half of
         * that of its parent; if the root Spliterator does not maintain an
         * accurate count, it could estimate size to be the power of two
         * corresponding to its maximum depth.
         */
        @Override
        public long estimateSize() {
            return maxRemaining;
        }

        /**
         * Returns a set of characteristics of this Spliterator and its
         * elements. The result is represented as ORed values from {@link
         * #ORDERED}, {@link #DISTINCT}, {@link #SORTED}, {@link #SIZED},
         * {@link #NONNULL}, {@link #IMMUTABLE}, {@link #CONCURRENT},
         * {@link #SUBSIZED}.  Repeated calls to {@code characteristics()} on
         * a given spliterator, prior to or in-between calls to {@code trySplit},
         * should always return the same result.
         *
         * <p>If a Spliterator reports an inconsistent set of
         * characteristics (either those returned from a single invocation
         * or across multiple invocations), no guarantees can be made
         * about any computation using this Spliterator.
         *
         * @return a representation of characteristics
         * @apiNote The characteristics of a given spliterator before splitting
         * may differ from the characteristics after splitting.  For specific
         * examples see the characteristic values {@link #SIZED}, {@link #SUBSIZED}
         * and {@link #CONCURRENT}.
         */
        @Override
        public int characteristics() {
            return DISTINCT | NONNULL | IMMUTABLE;
        }
    }

    private static class ArrayWithNullsIterator implements Iterator<TripleSetWithIndexingValue> {

        private final TripleSetWithIndexingValue[] entries;
        private final boolean[] used;
        private int remaining;
        private int pos = -1;

        private ArrayWithNullsIterator(final TripleSetWithIndexingValue[] entries, boolean[] used, final int size) {
            this.entries = entries;
            this.used = used;
            this.remaining = size;
        }

        /**
         * Returns {@code true} if the iteration has more elements.
         * (In other words, returns {@code true} if {@link #next} would
         * return an element rather than throwing an exception.)
         *
         * @return {@code true} if the iteration has more elements
         */
        @Override
        public boolean hasNext() {
            return 0 < remaining;
        }

        /**
         * Returns the next element in the iteration.
         *
         * @return the next element in the iteration
         * @throws NoSuchElementException if the iteration has no more elements
         */
        @Override
        public TripleSetWithIndexingValue next() {
            if(0 < remaining--) {
                while(!used[++pos]);
                return entries[pos];
            }
            throw new NoSuchElementException();
        }
    }
}

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

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.SparseArrayIterator;
import org.apache.jena.mem.SparseArraySpliterator;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Queue which grows, if needed but never shrinks.
 * This queue does not guarantee any order.
 * It´s purpose is to support fast remove operations.
 */
public abstract class FastTripleHashSetWithIndexingValueSG implements TripleSetWithIndexingNodeSG {
    @Override
    public final boolean areOperationsWithHashCodesSupported() {
        return true;
    }

    /*Idea from hashmap: improve hash code by (h = key.hashCode()) ^ (h >>> 16)*/
    private int calcStartIndexByHashCode(final int hashCode) {
        //return (hashCode ^ (hashCode >>> 16)) & (entries.length-1);
        return hashCode & (entries.length-1);
    }

    protected abstract Predicate<Triple> getContainsPredicate(final Triple tripleMatch);


    private static int MINIMUM_SIZE = 16;
    private static float loadFactor = 0.5f;
    protected int size = 0;
    protected Triple[] entries;
    protected int[] hashCodes;

    private final Node indexingValue;

    @Override
    public Node getIndexingNode() {
        return this.indexingValue;
    }

    public FastTripleHashSetWithIndexingValueSG(TripleSetWithIndexingNodeSG set) {
        this.indexingValue = set.getIndexingNode();
        this.entries = new Triple[Integer.highestOneBit(((int)(set.size()/loadFactor)+1)) << 1];
        this.hashCodes = new int[entries.length];
        set.iterator().forEachRemaining(t -> {
            final int hashCode = t.hashCode();
            final int index = findEmptySlotWithoutEqualityCheck(hashCode);
            entries[index] = t;
            hashCodes[index] = hashCode;
            size++;
        });
    }

    private int calcNewSize() {
        if(size >= entries.length*loadFactor && entries.length <= 1 << 30) { /*grow*/
            return entries.length << 1;
        }
        return -1;
    }

    private void grow(final int minCapacity) {
        final var oldEntries = this.entries;
        final var oldHashCodes = this.hashCodes;
        this.entries = new Triple[Integer.highestOneBit(((int)(minCapacity/loadFactor)+1)) << 1];
        this.hashCodes = new int[entries.length];
        for(int i=0; i<oldEntries.length; i++) {
            if(null != oldEntries[i]) {
                var newSlot = findEmptySlotWithoutEqualityCheck(oldHashCodes[i]);
                this.entries[newSlot] = oldEntries[i];
                this.hashCodes[newSlot] = oldHashCodes[i];
            }
        }
    }

    private boolean grow() {
        final var newSize = calcNewSize();
        if(newSize < 0) {
            return false;
        }
        final var oldEntries = this.entries;
        final var oldHashCodes = this.hashCodes;
        this.entries = new Triple[newSize];
        this.hashCodes = new int[newSize];
        for(int i=0; i<oldEntries.length; i++) {
            if(null != oldEntries[i]) {
                var newSlot = findEmptySlotWithoutEqualityCheck(oldHashCodes[i]);
                this.entries[newSlot] = oldEntries[i];
                this.hashCodes[newSlot] = oldHashCodes[i];
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
        return size;
    }

    /**
     * Returns {@code true} if this collection contains no elements.
     *
     * @return {@code true} if this collection contains no elements
     */
    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean contains(Triple e) {
        final int hashCode;
        var index = calcStartIndexByHashCode(hashCode = e.hashCode());
        if(null == entries[index]) {
            return false;
        }
        final var predicate = getContainsPredicate(e);
        if(hashCode == hashCodes[index] && predicate.test(entries[index])) {
            return true;
        } else if(--index < 0){
            index += entries.length;
        }
        while(true) {
            if(null == entries[index]) {
                return false;
            } else if(hashCode == hashCodes[index] && predicate.test(entries[index])) {
                return true;
            } else if(--index < 0){
                index += entries.length;
            }
        }
    }


    @Override
    public ExtendedIterator<Triple> iterator() {
        final var initialSize = size;
        final Runnable checkForConcurrentModification = () ->
        {
            if (size != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArrayIterator<>(entries, checkForConcurrentModification);
    }


    public Triple findAny() {
        var index = -1;
        while(entries[++index] == null);
        return entries[index];
    }

    @Override
    public boolean add(Triple value) {
        return add(value, value.hashCode());
    }

    @Override
    public boolean add(Triple value, int hashCode) {
        grow();
        var index = findIndex(value, hashCode);
        if(index < 0) {
            entries[~index] = value;
            hashCodes[~index] = hashCode;
            size++;
            return true;
        }
        return false;
    }

    @Override
    public void addUnchecked(Triple value) {
        addUnchecked(value, value.hashCode());
    }

    @Override
    public void addUnchecked(Triple value, int hashCode) {
        grow();
        var index = findEmptySlotWithoutEqualityCheck(hashCode);
        entries[index] = value;
        hashCodes[index] = hashCode;
        size++;
    }

    public Triple addIfAbsent(Triple value) {
        grow();
        final int hashCode;
        final var index = findIndex(value, hashCode = value.hashCode());
        if(index < 0) {
            entries[~index] = value;
            hashCodes[~index] = hashCode;
            size++;
            return value;
        }
        return entries[index];
    }

    public Triple getIfPresent(Triple value) {
        final int hashCode;
        var index = calcStartIndexByHashCode(hashCode = value.hashCode());
        while(true) {
            if(null == entries[index]) {
                return null;
            } else if(hashCode == hashCodes[index] && value.equals(entries[index])) {
                return entries[index];
            } else if(--index < 0){
                index += entries.length;
            }
        }
    }

    public Triple compute(Triple value, Function<Triple, Triple> remappingFunction) {
        final int hashCode;
        var index = findIndex(value, hashCode = value.hashCode());
        if(index < 0) { /*value does not exist yet*/
            var newValue = remappingFunction.apply(null);
            if(newValue == null) {
                return null;
            }
            if(!value.equals(newValue)) {
                throw new IllegalArgumentException("remapped value is not equal to value");
            }
            if(grow()) {
                index = findEmptySlotWithoutEqualityCheck(hashCode);
            } else {
                index = ~index;
            }
            entries[index] = newValue;
            hashCodes[index] = hashCode;
            size++;
            return newValue;
        } else { /*existing value found*/
            var newValue = remappingFunction.apply(entries[index]);
            if(newValue == null) {
                entries[index] = null;
                size--;
                rearrangeNeighbours(index);
                return null;
            } else {
                entries[index] = newValue;
                return newValue;
            }
        }
    }

    private int findIndex(final Triple e, final int hashCode) {
        var index = calcStartIndexByHashCode(hashCode);
        while(true) {
            if(null == entries[index]) {
                return ~index;
            } else if(hashCode == hashCodes[index] && e.equals(entries[index])) {
                return index;
            } else if(--index < 0){
                index += entries.length;
            }
        }
    }

    private int findEmptySlotWithoutEqualityCheck(final int hashCode) {
        var index = calcStartIndexByHashCode(hashCode);
        while(true) {
            if(null == entries[index]) {
                return index;
            } else if(--index < 0){
                index += entries.length;
            }
        }
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
     * @param e element to be removed from this collection, if present
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
    public boolean remove(Triple e) {
        return remove(e, e.hashCode());
    }

    public boolean remove(Triple e, int hashCode) {
        var index = findIndex(e, hashCode);
        if (index < 0) {
            return false;
        }
        entries[index] = null;
        size--;
        rearrangeNeighbours(index);
        return true;
    }

    public void removeUnchecked(Triple e) {
        removeUnchecked(e, e.hashCode());
    }

    public void removeUnchecked(Triple e, int hashCode) {
        var index = findIndex(e, hashCode);
        entries[index] = null;
        size--;
        rearrangeNeighbours(index);
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
                    entries[oldIndexOfNeighbour] = null;
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
            if(null == entries[i]) {
                break;
            } else {
                neighbour = new ObjectsWithStartIndexIndexAndDistance(
                        entries.length, calcStartIndexByHashCode(hashCodes[i]), i);
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
            if(null == entries[i]) {
                break;
            } else {
                neighbour = new ObjectsWithStartIndexIndexAndDistance(
                        entries.length, calcStartIndexByHashCode(hashCodes[i]), i);
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
     * Returns a sequential {@code Stream} with this collection as its source.
     *
     * @return a sequential {@code Stream} over the elements in this collection
     * @implSpec The default implementation creates a sequential {@code Stream} from the
     * collection's {@code Spliterator}.
     * @since 1.8
     */
    @Override
    public Stream<Triple> stream() {
        final var initialSize = size;
        final Runnable checkForConcurrentModification = () ->
        {
            if (size != initialSize) throw new ConcurrentModificationException();
        };
        return StreamSupport.stream(new SparseArraySpliterator<>(entries, size, checkForConcurrentModification), false);
    }
}

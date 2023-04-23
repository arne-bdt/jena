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
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class HashSetOfTripleSets {

    private int calcStartIndexByHashCode(final int hashCode) {
        return (hashCode ^ (hashCode >>> 16)) & (entries.length-1);
    }

    private static int MINIMUM_SIZE = 16;
    private static float loadFactor = 0.5f;
    protected int size = 0;
    protected TripleSetWithIndexingValue[] entries;
    protected int[] hashCodes;

    public HashSetOfTripleSets() {
        this.entries = new TripleSetWithIndexingValue[MINIMUM_SIZE];
        this.hashCodes = new int[MINIMUM_SIZE];

    }

    public HashSetOfTripleSets(int initialCapacity) {
        this.entries = new TripleSetWithIndexingValue[Integer.highestOneBit(((int)(initialCapacity/loadFactor)+1)) << 1];
        this.hashCodes = new int[entries.length];
    }

    public HashSetOfTripleSets(Set<TripleSetWithIndexingValue> set) {
        this(set.size(), set);
    }

    public HashSetOfTripleSets(int initialCapacity, Set<TripleSetWithIndexingValue> set) {
        this.entries = new TripleSetWithIndexingValue[Integer.highestOneBit(((int)(Math.max(set.size(), initialCapacity)/loadFactor)+1)) << 1];
        this.hashCodes = new int[entries.length];
        int index, hashCode;
        Object indexingValue;
        for (TripleSetWithIndexingValue e : set) {
            if((index = findIndex(indexingValue = e.getIndexingValue(), hashCode = indexingValue.hashCode())) < 0) {
                entries[~index] = e;
                hashCodes[~index] = hashCode;
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
        this.entries = new TripleSetWithIndexingValue[newSize];
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
        if(null == entries[index]) {
            return false;
        }
        if(hashCode == hashCodes[index] && key.equals(entries[index].getIndexingValue())) {
            return true;
        } else if(--index < 0){
            index += entries.length;
        }
        while(true) {
            if(null == entries[index]) {
                return false;
            } else {
                if(hashCode == hashCodes[index] && key.equals(entries[index].getIndexingValue())) {
                    return true;
                } else if (--index < 0){
                    index += entries.length;
                }
            }
        }
    }

    public Iterator<TripleSetWithIndexingValue> iterator() {
        return new ArrayWithNullsIterator(entries, size);
    }

    public TripleSetWithIndexingValue getIfPresent(final Object indexingValue) {
        var hashCode = indexingValue.hashCode();
        var index = calcStartIndexByHashCode(hashCode);
        while(true) {
            if(null == entries[index]) {
                return null;
            } else if(hashCode == hashCodes[index] && indexingValue.equals(entries[index].getIndexingValue())) {
                return entries[index];
            } else if(--index < 0){
                index += entries.length;
            }
        }
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

    private int findIndex(final Object key, final int hashCode) {
        var index = calcStartIndexByHashCode(hashCode);
        while(true) {
            if(null == entries[index]) {
                return ~index;
            } else if(hashCode == hashCodes[index] && key.equals(entries[index].getIndexingValue())) {
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
            if(null == entries[i]) {
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
        size = 0;
    }


    public Stream<TripleSetWithIndexingValue> stream() {
        return StreamSupport.stream(new ArrayWithNullsSpliteratorSized(entries, size), false);
    }


    public Stream<TripleSetWithIndexingValue> parallelStream() {
        return StreamSupport.stream(new ArrayWithNullsSpliteratorSized(entries, size), true);
    }

}

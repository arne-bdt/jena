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
import org.apache.jena.mem.SparseArrayIterator;
import org.apache.jena.mem.SparseArraySpliterator;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class HashSetOfTripleSetsSG {

    private int calcStartIndexByHashCode(final int hashCode) {
        //return (hashCode ^ (hashCode >>> 16)) & (entries.length-1);
        return hashCode & (entries.length-1);
    }

    private static int MINIMUM_SIZE = 16;
    private static float loadFactor = 0.5f;
    protected int size = 0;
    protected TripleSetWithIndexingNodeSG[] entries;
    protected int[] hashCodes;

    public HashSetOfTripleSetsSG() {
        this.entries = new TripleSetWithIndexingNodeSG[MINIMUM_SIZE];
        this.hashCodes = new int[MINIMUM_SIZE];

    }

    public HashSetOfTripleSetsSG(int initialCapacity) {
        this.entries = new TripleSetWithIndexingNodeSG[Integer.highestOneBit(((int)(initialCapacity/loadFactor)+1)) << 1];
        this.hashCodes = new int[entries.length];
    }

    public HashSetOfTripleSetsSG(Set<TripleSetWithIndexingNodeSG> set) {
        this(set.size(), set);
    }

    public HashSetOfTripleSetsSG(int initialCapacity, Set<TripleSetWithIndexingNodeSG> set) {
        this.entries = new TripleSetWithIndexingNodeSG[Integer.highestOneBit(((int)(Math.max(set.size(), initialCapacity)/loadFactor)+1)) << 1];
        this.hashCodes = new int[entries.length];
        int index, hashCode;
        for (TripleSetWithIndexingNodeSG e : set) {
            if((index = findIndex(e.getIndexingNode(), hashCode = e.getIndexingNode().hashCode())) < 0) {
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

    private void grow() {
        final var newSize = calcNewSize();
        if(newSize < 0) {
            return;
        }
        final var oldEntries = this.entries;
        final var oldHashCodes = this.hashCodes;
        this.entries = new TripleSetWithIndexingNodeSG[newSize];
        this.hashCodes = new int[newSize];
        for(int i=0; i<oldEntries.length; i++) {
            if(null != oldEntries[i]) {
                var newSlot = findEmptySlotWithoutEqualityCheck(oldHashCodes[i]);
                this.entries[newSlot] = oldEntries[i];
                this.hashCodes[newSlot] = oldHashCodes[i];
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
        var e = (TripleSetWithIndexingNode)o;
        final var key = e.getIndexingNode();
        final var hashCode = key.hashCode();
        var index = calcStartIndexByHashCode(hashCode);
        if(null == entries[index]) {
            return false;
        }
        if(hashCode == hashCodes[index] && key.equals(entries[index].getIndexingNode())) {
            return true;
        } else if(--index < 0){
            index += entries.length;
        }
        while(true) {
            if(null == entries[index]) {
                return false;
            } else {
                if(hashCode == hashCodes[index] && key.equals(entries[index].getIndexingNode())) {
                    return true;
                } else if (--index < 0){
                    index += entries.length;
                }
            }
        }
    }

    public Iterator<TripleSetWithIndexingNodeSG> iterator() {
        final var initialSize = size;
        final Runnable checkForConcurrentModification = () ->
        {
            if (size != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArrayIterator<>(entries, checkForConcurrentModification);
    }

    public TripleSetWithIndexingNodeSG getIfPresent(final Node indexingNode) {
        var hashCode = indexingNode.hashCode();
        var index = calcStartIndexByHashCode(hashCode);
        while(true) {
            if(null == entries[index]) {
                return null;
            } else if(hashCode == hashCodes[index] && indexingNode.equals(entries[index].getIndexingNode())) {
                return entries[index];
            } else if(--index < 0){
                index += entries.length;
            }
        }
    }

    public TripleSetWithIndexingNodeSG compute(final Node indexingNode, final int hashCodeOfKey, Function<TripleSetWithIndexingNodeSG, TripleSetWithIndexingNodeSG> remappingFunction) {
        final var index = findIndex(indexingNode, hashCodeOfKey);
        if(index < 0) { /*value does not exist yet*/
            var newValue = remappingFunction.apply(null);
            if(newValue == null) {
                return null;
            }
            entries[~index] = newValue;
            hashCodes[~index] = hashCodeOfKey;
            size++;
            grow();
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

    public TripleSetWithIndexingNodeSG compute(final Node indexingNode, Function<TripleSetWithIndexingNodeSG, TripleSetWithIndexingNodeSG> remappingFunction) {
        var hashCodeOfKey = indexingNode.hashCode();
        final var index = findIndex(indexingNode, hashCodeOfKey);
        if(index < 0) { /*value does not exist yet*/
            var newValue = remappingFunction.apply(null);
            if(newValue == null) {
                return null;
            }
            entries[~index] = newValue;
            hashCodes[~index] = hashCodeOfKey;
            size++;
            grow();
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

    private int findIndex(final Node indexingNode, final int hashCode) {
        var index = calcStartIndexByHashCode(hashCode);
        while(true) {
            if(null == entries[index]) {
                return ~index;
            } else if(hashCode == hashCodes[index] && indexingNode.equals(entries[index].getIndexingNode())) {
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
     * Removes all the elements from this collection (optional operation).
     * The collection will be empty after this method returns.
     *
     * @throws UnsupportedOperationException if the {@code clear} operation
     *                                       is not supported by this collection
     */
    public void clear() {
        entries = new TripleSetWithIndexingNodeSG[MINIMUM_SIZE];
        hashCodes = new int[MINIMUM_SIZE];
        size = 0;
    }


    public Stream<TripleSetWithIndexingNodeSG> stream() {
        final var initialSize = size;
        final Runnable checkForConcurrentModification = () ->
        {
            if (size != initialSize) throw new ConcurrentModificationException();
        };
        return StreamSupport.stream(new SparseArraySpliterator<>(entries, size, checkForConcurrentModification), false);
    }


    public Stream<TripleSetWithIndexingNodeSG> parallelStream() {
        final var initialSize = size;
        final Runnable checkForConcurrentModification = () ->
        {
            if (size != initialSize) throw new ConcurrentModificationException();
        };
        return StreamSupport.stream(new SparseArraySpliterator<>(entries, size, checkForConcurrentModification), true);
    }

}

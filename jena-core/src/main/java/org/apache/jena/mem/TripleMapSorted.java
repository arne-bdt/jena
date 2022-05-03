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
package org.apache.jena.mem;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

import java.util.*;
import java.util.function.Function;

class TripleMapSorted extends TripleMap {

    protected final int switchToSortedThreshold;
    protected final Comparator<Triple> listComparator;

    public TripleMapSorted(final Function<Triple, Node> keyNodeResolver,
                           final Function<Triple, Node> sortNodeResolver,
                           int switchToSortedThreshold) {
        super(keyNodeResolver);
        this.listComparator = Comparator.comparingInt(t -> sortNodeResolver.apply(t).getIndexingValue().hashCode());
        this.switchToSortedThreshold = switchToSortedThreshold;
    }

    /**
     * Adds only if not already exists.
     *
     * @param t
     * @return
     */
    @Override
    public boolean addIfNotExists(final Triple t) {
        var key = getKey(t);
        var list = map.get(key);
        if(list != null) {
            if(list.size() < switchToSortedThreshold) {
                if (list.contains(t)) {
                    return false; /*triple already exists*/
                }
                list.add(t);
                if(list.size() == switchToSortedThreshold) {
                    list.sort(listComparator);
                }
            } else {
                var index = Collections.binarySearch(list, t, listComparator);
                // < 0 if element is not in the list, see Collections.binarySearch
                if (index < 0) {
                    index = -(index + 1);
                    list.add(index, t);
                }
                else {
                    /*search forward*/
                    for (var i = index; i < list.size(); i++) {
                        var t1 = list.get(i);
                        if (t.equals(t1)) {
                            return false;
                        }
                        if (0 != listComparator.compare(t, t1)) {
                            break;
                        }
                    }
                    if(index > 0) {
                        /*search backward*/
                        index--;
                        for (var i = index; i >= 0; i--) {
                            var t1 = list.get(i);
                            if (t.equals(t1)) {
                                return false;
                            }
                            if (0 != listComparator.compare(t, t1)) {
                                break;
                            }
                        }
                        index++;
                    }
                    // Insertion index is index of existing element, to add new element
                    // behind it increase index
                    index++;
                    list.add(index, t);
                }
            }
        } else {
            list = new ArrayList<>(INITIAL_SIZE_FOR_ARRAY_LISTS);
            list.add(t);
            map.put(key, list);
        }
        return true;
    }

    /**
     * Add with no checks
     * @param t
     */
    @Override
    public void addDefinitetly(final Triple t) {
        var list = map
                .computeIfAbsent(getKey(t),
                        k -> new ArrayList<>(INITIAL_SIZE_FOR_ARRAY_LISTS));
        if(list.size() < switchToSortedThreshold) {
            list.add(t);
            if(list.size() == switchToSortedThreshold) {
                list.sort(listComparator);
            }
        } else {
            var index = Collections.binarySearch(list, t, listComparator);
            // < 0 if element is not in the list, see Collections.binarySearch
            if (index < 0) {
                index = -(index + 1);
                list.add(index, t);
            } else {
                // Insertion index is index of existing element, to add new element
                // behind it increase index
                index++;
                list.add(index, t);
            }
        }
    }

    /**
     * Source: https://www.geeksforgeeks.org/find-first-and-last-positions-of-an-element-in-a-sorted-array/
     * by DANISH_RAZA
     * @param list
     * @param key
     * @param c
     * @return
     */
    private static int getFirstOccurence(final List<Triple> list, final Triple key, final Comparator<Triple> c){
        int low = 0, high = list.size() - 1,
                res = -1;
        while (low <= high)
        {
            // Normal Binary Search Logic
            int mid = (low + high) / 2;
            var comp = c.compare(key, list.get(mid));
            if (comp < 0)
                high = mid - 1;
            else if (comp > 0)
                low = mid + 1;
            else
            {
                // If arr[mid] is same as
                // x, we update res and
                // move to the left half.
                res = mid;
                high = mid - 1;
            }
        }
        return res;
    }

    /**
     * Source: https://www.geeksforgeeks.org/find-first-and-last-positions-of-an-element-in-a-sorted-array/
     * by DANISH_RAZA
     * @param list
     * @param key
     * @param c
     * @return
     */
    private static int getLastOccurence(final List<Triple> list, final Triple key, final Comparator<Triple> c){
        int low = 0, high = list.size()-1,
                res = -1;
        while (low <= high)
        {
            // Normal Binary Search Logic
            int mid = (low + high) / 2;
            var comp = c.compare(key, list.get(mid));
            if (comp < 0)
                high = mid - 1;
            else if (comp > 0)
                low = mid + 1;
            else
            {
                // If arr[mid] is same as x,
                // we update res and move to
                // the right half.
                res = mid;
                low = mid + 1;
            }
        }
        return res;
    }
    /**
     *
     * @param t triple with concrete key and concrete sort node
     * @return
     */
    public List<Triple> searchCandidates(final Triple t) {
        var list = map.get(getKey(t));
        if(list == null) {
            return null;
        }
        if(list.size() < switchToSortedThreshold) {
            return list;
        }
        var firstIndex = getFirstOccurence(list, t, this.listComparator);
        if (firstIndex < 0) {
            return null;
        }
        return list.subList(firstIndex,
                getLastOccurence(list.subList(firstIndex, list.size()), t, this.listComparator)+1+firstIndex);
    }
}

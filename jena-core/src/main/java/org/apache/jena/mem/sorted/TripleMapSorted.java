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
package org.apache.jena.mem.sorted;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Function;

public class TripleMapSorted extends TripleMap {

    protected final static int SWITCH_TO_SORTED_THRESHOLD = 40;
    protected final Comparator<Triple> listComparator;
    protected final BiPredicate<Triple, Triple> containsPredicate;

    public TripleMapSorted(final Function<Triple, Node> keyNodeResolver,
                           final Function<Triple, Node> firstSortNodeResolver,
                           final Function<Triple, Node> secondSortNodeResolver,
                           final BiPredicate<Triple, Triple> containsPredicate) {
        super(keyNodeResolver);
        this.listComparator = Comparator.comparingInt((Triple t) -> firstSortNodeResolver.apply(t).getIndexingValue().hashCode())
                .thenComparing(t -> secondSortNodeResolver.apply(t).getIndexingValue().hashCode());
        this.containsPredicate = containsPredicate;
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
            if(list.size() < SWITCH_TO_SORTED_THRESHOLD) {
                if (list.contains(t)) {
                    return false; /*triple already exists*/
                }
                list.add(t);
                if(list.size() == SWITCH_TO_SORTED_THRESHOLD) {
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
                        if (0 != listComparator.compare(t1, t)) {
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
                            if (0 != listComparator.compare(t1, t)) {
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
        if(list.size() < SWITCH_TO_SORTED_THRESHOLD) {
            list.add(t);
            if(list.size() == SWITCH_TO_SORTED_THRESHOLD) {
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
     * Removes only if exists.
     *
     * @param t
     * @return
     */
    @Override
    public boolean removeIfExits(final Triple t) {
        var key = getKey(t);
        var list = map.get(key);
        if (list != null) {
            if (list.size() < SWITCH_TO_SORTED_THRESHOLD) {
                if (list.remove(t)) {
                    if (list.isEmpty()) {
                        map.remove(key);
                    }
                    return true;
                }
            } else {
                var index = Collections.binarySearch(list, t, listComparator);
                // < 0 if element is not in the list, see Collections.binarySearch
                if (index < 0) {
                    return false;
                } else {
                    /*search forward*/
                    for (var i = index; i < list.size(); i++) {
                        var t1 = list.get(i);
                        if (containsPredicate.test(t1, t)) {
                            list.remove(i);
                            if (list.isEmpty()) {
                                map.remove(key);
                            }
                            return true;
                        }
                        if (0 != listComparator.compare(t1, t)) {
                            break;
                        }
                    }
                    if (index > 0) {
                        /*search backward*/
                        index--;
                        for (var i = index; i >= 0; i--) {
                            var t1 = list.get(i);
                            if (containsPredicate.test(t1, t)) {
                                list.remove(i);
                                if (list.isEmpty()) {
                                    map.remove(key);
                                }
                                return true;
                            }
                            if (0 != listComparator.compare(t1, t)) {
                                break;
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    /**
     * Remove existing triple, without any checks.
     *
     * @param t
     */
    @Override
    public void removeExisting(final Triple t) {
        var key = getKey(t);
        var list = map
                .computeIfAbsent(key,
                        k -> new ArrayList<>(INITIAL_SIZE_FOR_ARRAY_LISTS));
        if(list.size() < SWITCH_TO_SORTED_THRESHOLD) {
            if (list.remove(t)) {
                if (list.isEmpty()) {
                    map.remove(key);
                }
            }
        } else {
            var index = Collections.binarySearch(list, t, listComparator);
            /*search forward*/
            for (var i = index; i < list.size(); i++) {
                var t1 = list.get(i);
                if (containsPredicate.test(t1, t)) {
                    list.remove(i);
                    if (list.isEmpty()) {
                        map.remove(key);
                    }
                    return;
                }
                if (0 != listComparator.compare(t1, t)) {
                    break;
                }
            }
            if (index > 0) {
                /*search backward*/
                index--;
                for (var i = index; i >= 0; i--) {
                    var t1 = list.get(i);
                    if (containsPredicate.test(t1, t)) {
                        list.remove(i);
                        if (list.isEmpty()) {
                            map.remove(key);
                        }
                        return;
                    }
                    if (0 != listComparator.compare(t1, t)) {
                        break;
                    }
                }
            }
        }
    }

    /**
     *
     * @param t triple with concrete key and concrete sort node
     * @return
     */
    public boolean contains(final Triple t) {
        var key = getKey(t);
        var list = map.get(key);
        if(list == null) {
            return false;
        }
        if(list.size() < SWITCH_TO_SORTED_THRESHOLD) {
            for (Triple triple : list) {
                if(containsPredicate.test(triple, t)) {
                    return true;
                }
            }
        } else  {
            var index = Collections.binarySearch(list, t, listComparator);
            if (index < 0) {
                return false;
            }
            /*search forward*/
            for (var i = index; i < list.size(); i++) {
                var t1 = list.get(i);
                if (containsPredicate.test(t1, t)) {
                    return true;
                }
                if (0 != listComparator.compare(t1, t)) {
                    break;
                }
            }
            if (index > 0) {
                /*search backward*/
                index--;
                for (var i = index; i > -1; i--) {
                    var t1 = list.get(i);
                    if (containsPredicate.test(t1, t)) {
                        return true;
                    }
                    if (0 != listComparator.compare(t1, t)) {
                        break;
                    }
                }
            }
        }
        return false;
    }
}

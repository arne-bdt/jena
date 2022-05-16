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

package org.apache.jena.mem2.generic;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class LeanHashMap<K, V> implements Map<K,V> {

    private class Entry<K, V> implements Map.Entry<K, V> {
        private final K key;
        private V value;
        private Entry<K, V> next;

        public Entry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }

        public Entry<K, V> getNext() {
            return next;
        }

        public V setValue(V value) {
            var oldValue = this.value;
            this.value = value;
            return oldValue;
        }

        public void setNext(Entry<K, V> next) {
            this.next = next;
        }

        /**
         * Returns a hash code value for the object. This method is
         * supported for the benefit of hash tables such as those provided by
         * {@link HashMap}.
         * <p>
         * The general contract of {@code hashCode} is:
         * <ul>
         * <li>Whenever it is invoked on the same object more than once during
         *     an execution of a Java application, the {@code hashCode} method
         *     must consistently return the same integer, provided no information
         *     used in {@code equals} comparisons on the object is modified.
         *     This integer need not remain consistent from one execution of an
         *     application to another execution of the same application.
         * <li>If two objects are equal according to the {@code equals(Object)}
         *     method, then calling the {@code hashCode} method on each of
         *     the two objects must produce the same integer result.
         * <li>It is <em>not</em> required that if two objects are unequal
         *     according to the {@link Object#equals(Object)}
         *     method, then calling the {@code hashCode} method on each of the
         *     two objects must produce distinct integer results.  However, the
         *     programmer should be aware that producing distinct integer results
         *     for unequal objects may improve the performance of hash tables.
         * </ul>
         * <p>
         * As much as is reasonably practical, the hashCode method defined
         * by class {@code Object} does return distinct integers for
         * distinct objects. (The hashCode may or may not be implemented
         * as some function of an object's memory address at some point
         * in time.)
         *
         * @return a hash code value for this object.
         * @see Object#equals(Object)
         * @see System#identityHashCode
         */
        @Override
        public int hashCode() {
            return this.getKey().hashCode() ^ this.getValue().hashCode();
        }

        /**
         * Indicates whether some other object is "equal to" this one.
         * <p>
         * The {@code equals} method implements an equivalence relation
         * on non-null object references:
         * <ul>
         * <li>It is <i>reflexive</i>: for any non-null reference value
         *     {@code x}, {@code x.equals(x)} should return
         *     {@code true}.
         * <li>It is <i>symmetric</i>: for any non-null reference values
         *     {@code x} and {@code y}, {@code x.equals(y)}
         *     should return {@code true} if and only if
         *     {@code y.equals(x)} returns {@code true}.
         * <li>It is <i>transitive</i>: for any non-null reference values
         *     {@code x}, {@code y}, and {@code z}, if
         *     {@code x.equals(y)} returns {@code true} and
         *     {@code y.equals(z)} returns {@code true}, then
         *     {@code x.equals(z)} should return {@code true}.
         * <li>It is <i>consistent</i>: for any non-null reference values
         *     {@code x} and {@code y}, multiple invocations of
         *     {@code x.equals(y)} consistently return {@code true}
         *     or consistently return {@code false}, provided no
         *     information used in {@code equals} comparisons on the
         *     objects is modified.
         * <li>For any non-null reference value {@code x},
         *     {@code x.equals(null)} should return {@code false}.
         * </ul>
         * <p>
         * The {@code equals} method for class {@code Object} implements
         * the most discriminating possible equivalence relation on objects;
         * that is, for any non-null reference values {@code x} and
         * {@code y}, this method returns {@code true} if and only
         * if {@code x} and {@code y} refer to the same object
         * ({@code x == y} has the value {@code true}).
         * <p>
         * Note that it is generally necessary to override the {@code hashCode}
         * method whenever this method is overridden, so as to maintain the
         * general contract for the {@code hashCode} method, which states
         * that equal objects must have equal hash codes.
         *
         * @param obj the reference object with which to compare.
         * @return {@code true} if this object is the same as the obj
         * argument; {@code false} otherwise.
         * @see #hashCode()
         * @see HashMap
         */
        @Override
        public boolean equals(Object obj) {
            if(obj == null) {
                return false;
            }
            if(obj == this) {
                return true;
            }
            if(!(obj instanceof Entry)) {
                return false;
            }
            var other = (Entry<K, V>)obj;
            return this.getKey().equals(other.getKey()) && this.getValue().equals(other.getValue());
        }
    }

    protected int getHashCode(final K key) {
        return key.hashCode();
    }

    /*Idea from hashmap: improve hash code by (h = key.hashCode()) ^ (h >>> 16)*/
    private int calcIndex(final int hashCode) {
        return (hashCode ^ (hashCode >>> 16)) & (entries.length-1);
    }

    private int calcIndex(final K key) {
        return calcIndex(getHashCode(key));
    }

    private static float DEFAULT_LOAD_FACTOR = 0.75f;
    private final float loadFactor;
    private static int DEFAULT_INITIAL_CAPACITY = 16;
    private int mapSize = 0;
    private Object[] entries;

    public LeanHashMap() {
        this.loadFactor = DEFAULT_LOAD_FACTOR;
        this.entries = new Object[DEFAULT_INITIAL_CAPACITY]; /*default capacity*/
    }

    public LeanHashMap(int initialCapacity, float loadFactor) {
        this.loadFactor = loadFactor;
        this.entries = new Object[Integer.highestOneBit(((int)(initialCapacity/loadFactor)+1)) << 1];
    }

    public LeanHashMap(int initialCapacity) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR);
    }

    private int calcNewSize() {
        if(mapSize >= entries.length*loadFactor && entries.length <= 1 << 30) { /*grow*/
            return entries.length << 1;
        }
        return -1;
    }

    private void grow() {
        final var newSize = calcNewSize();
        if(newSize < 0) {
            return;
        }
        final var oldEntries = entries;
        this.entries = new Object[newSize];
        for(int i=0; i<oldEntries.length; i++) {
            if(oldEntries[i] == null) {
                continue;
            }
            var entryToMove = (Entry<K, V>) oldEntries[i];
            do {
                var newIndex = calcIndex(entryToMove.getKey());
                var entryInTarget = (Entry<K, V>)entries[newIndex];
                var copyOfNext = entryToMove.next;
                /*insert as first element --> reversing order*/
                entryToMove.next = entryInTarget;
                entries[newIndex] = entryToMove;
                entryToMove = copyOfNext;
            } while (entryToMove != null);
        }
    }

    @Override
    public boolean isEmpty() {
        return mapSize == 0;
    }

    /**
     * Returns {@code true} if this map contains a mapping for the specified
     * key.  More formally, returns {@code true} if and only if
     * this map contains a mapping for a key {@code k} such that
     * {@code Objects.equals(key, k)}.  (There can be
     * at most one such mapping.)
     *
     * @param key key whose presence in this map is to be tested
     * @return {@code true} if this map contains a mapping for the specified
     * key
     * @throws ClassCastException   if the key is of an inappropriate type for
     *                              this map
     *                              (<a href="{@docRoot}/java.base/java/util/Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException if the specified key is null and this map
     *                              does not permit null keys
     *                              (<a href="{@docRoot}/java.base/java/util/Collection.html#optional-restrictions">optional</a>)
     */
    @Override
    public boolean containsKey(Object key) {
        final var hashCode = getHashCode((K)key);
        var index =  calcIndex(hashCode);
        var existingEntry = (Entry<K, V>)entries[index];
        if(existingEntry == null) {
            return false;
        }
        if(existingEntry.getKey().equals(key)) {
            return true;
        }
        while(existingEntry.next != null) {
            existingEntry = existingEntry.next;
            if(existingEntry.getKey().equals(key)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns {@code true} if this map maps one or more keys to the
     * specified value.  More formally, returns {@code true} if and only if
     * this map contains at least one mapping to a value {@code v} such that
     * {@code Objects.equals(value, v)}.  This operation
     * will probably require time linear in the map size for most
     * implementations of the {@code Map} interface.
     *
     * @param value value whose presence in this map is to be tested
     * @return {@code true} if this map maps one or more keys to the
     * specified value
     * @throws ClassCastException   if the value is of an inappropriate type for
     *                              this map
     *                              (<a href="{@docRoot}/java.base/java/util/Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException if the specified value is null and this
     *                              map does not permit null values
     *                              (<a href="{@docRoot}/java.base/java/util/Collection.html#optional-restrictions">optional</a>)
     */
    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    /**
     * Returns the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     *
     * <p>More formally, if this map contains a mapping from a key
     * {@code k} to a value {@code v} such that
     * {@code Objects.equals(key, k)},
     * then this method returns {@code v}; otherwise
     * it returns {@code null}.  (There can be at most one such mapping.)
     *
     * <p>If this map permits null values, then a return value of
     * {@code null} does not <i>necessarily</i> indicate that the map
     * contains no mapping for the key; it's also possible that the map
     * explicitly maps the key to {@code null}.  The {@link #containsKey
     * containsKey} operation may be used to distinguish these two cases.
     *
     * @param key the key whose associated value is to be returned
     * @return the value to which the specified key is mapped, or
     * {@code null} if this map contains no mapping for the key
     * @throws ClassCastException   if the key is of an inappropriate type for
     *                              this map
     *                              (<a href="{@docRoot}/java.base/java/util/Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException if the specified key is null and this map
     *                              does not permit null keys
     *                              (<a href="{@docRoot}/java.base/java/util/Collection.html#optional-restrictions">optional</a>)
     */
    @Override
    public V get(Object key) {
        final var hashCode = getHashCode((K)key);
        var index =  calcIndex(hashCode);
        var existingEntry = (Entry<K, V>)entries[index];
        if(existingEntry == null) {
            return null;
        }
        if(existingEntry.getKey().equals(key)) {
            return existingEntry.value;
        }
        while(existingEntry.next != null) {
            existingEntry = existingEntry.next;
            if(existingEntry.getKey().equals(key)) {
                return existingEntry.value;
            }
        }
        return null;
    }

    /**
     * Associates the specified value with the specified key in this map
     * (optional operation).  If the map previously contained a mapping for
     * the key, the old value is replaced by the specified value.  (A map
     * {@code m} is said to contain a mapping for a key {@code k} if and only
     * if {@link #containsKey(Object) m.containsKey(k)} would return
     * {@code true}.)
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with {@code key}, or
     * {@code null} if there was no mapping for {@code key}.
     * (A {@code null} return can also indicate that the map
     * previously associated {@code null} with {@code key},
     * if the implementation supports {@code null} values.)
     * @throws UnsupportedOperationException if the {@code put} operation
     *                                       is not supported by this map
     * @throws ClassCastException            if the class of the specified key or value
     *                                       prevents it from being stored in this map
     * @throws NullPointerException          if the specified key or value is null
     *                                       and this map does not permit null keys or values
     * @throws IllegalArgumentException      if some property of the specified key
     *                                       or value prevents it from being stored in this map
     */
    @Override
    public V put(K key, V value) {
        final var hashCode = getHashCode(key);
        final var index = calcIndex(hashCode);
        var existingEntry = (Entry<K, V>)entries[index];
        if(existingEntry == null) {
            var newKeyEntry = new Entry<>(key, value);
            entries[index] = newKeyEntry;
            mapSize++;
            grow();
            return value;
        }
        if(existingEntry.getKey().equals(key)) {
            var oldValue = existingEntry.value;
            existingEntry.setValue(value);
            return oldValue;
        }
        while(existingEntry.next != null) {
            existingEntry = existingEntry.next;
            if(existingEntry.getKey().equals(key)) {
                var oldValue = existingEntry.value;
                existingEntry.setValue(value);
                return oldValue;
            }
        }
        var newKeyEntry = new Entry<>(key, value);
        /*insert as last element*/
        existingEntry.next = newKeyEntry;
        mapSize++;
        grow();
        return value;
    }

    /**
     * Removes the mapping for a key from this map if it is present
     * (optional operation).   More formally, if this map contains a mapping
     * from key {@code k} to value {@code v} such that
     * {@code Objects.equals(key, k)}, that mapping
     * is removed.  (The map can contain at most one such mapping.)
     *
     * <p>Returns the value to which this map previously associated the key,
     * or {@code null} if the map contained no mapping for the key.
     *
     * <p>If this map permits null values, then a return value of
     * {@code null} does not <i>necessarily</i> indicate that the map
     * contained no mapping for the key; it's also possible that the map
     * explicitly mapped the key to {@code null}.
     *
     * <p>The map will not contain a mapping for the specified key once the
     * call returns.
     *
     * @param key key whose mapping is to be removed from the map
     * @return the previous value associated with {@code key}, or
     * {@code null} if there was no mapping for {@code key}.
     * @throws UnsupportedOperationException if the {@code remove} operation
     *                                       is not supported by this map
     * @throws ClassCastException            if the key is of an inappropriate type for
     *                                       this map
     *                                       (<a href="{@docRoot}/java.base/java/util/Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException          if the specified key is null and this
     *                                       map does not permit null keys
     *                                       (<a href="{@docRoot}/java.base/java/util/Collection.html#optional-restrictions">optional</a>)
     */
    @Override
    public V remove(Object key) {
        final var hashCode = getHashCode((K)key);
        final var index =  calcIndex(hashCode);
        var existingEntry = (Entry<K, V>)entries[index];
        if(existingEntry == null) {
            return null;
        }
        if(existingEntry.getKey().equals(key)) {
            entries[index] = existingEntry.next;
            mapSize--;
            return existingEntry.value;
        }
        while(existingEntry.next != null) {
            var previousEntry = existingEntry;
            existingEntry = existingEntry.next;
            if(existingEntry.getKey().equals(key)) {
                previousEntry.next = existingEntry.next;
                mapSize--;
                return existingEntry.value;
            }
        }
        return null;
    }

    /**
     * Copies all of the mappings from the specified map to this map
     * (optional operation).  The effect of this call is equivalent to that
     * of calling {@link #put(Object, Object) put(k, v)} on this map once
     * for each mapping from key {@code k} to value {@code v} in the
     * specified map.  The behavior of this operation is undefined if the
     * specified map is modified while the operation is in progress.
     *
     * @param m mappings to be stored in this map
     * @throws UnsupportedOperationException if the {@code putAll} operation
     *                                       is not supported by this map
     * @throws ClassCastException            if the class of a key or value in the
     *                                       specified map prevents it from being stored in this map
     * @throws NullPointerException          if the specified map is null, or if
     *                                       this map does not permit null keys or values, and the
     *                                       specified map contains null keys or values
     * @throws IllegalArgumentException      if some property of a key or value in
     *                                       the specified map prevents it from being stored in this map
     */
    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        boolean modified = false;
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
            this.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {
        entries = new Object[DEFAULT_INITIAL_CAPACITY];
        mapSize = 0;
    }

    /**
     * Returns a {@link Set} view of the keys contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  If the map is modified
     * while an iteration over the set is in progress (except through
     * the iterator's own {@code remove} operation), the results of
     * the iteration are undefined.  The set supports element removal,
     * which removes the corresponding mapping from the map, via the
     * {@code Iterator.remove}, {@code Set.remove},
     * {@code removeAll}, {@code retainAll}, and {@code clear}
     * operations.  It does not support the {@code add} or {@code addAll}
     * operations.
     *
     * @return a set view of the keys contained in this map
     */
    @Override
    public Set<K> keySet() {
        return StreamSupport.stream(new EntriesSpliterator<K, V>(entries, mapSize), false)
                .map(e -> e.getKey())
                .collect(Collectors.toSet());
    }

    /**
     * Returns a {@link Collection} view of the values contained in this map.
     * The collection is backed by the map, so changes to the map are
     * reflected in the collection, and vice-versa.  If the map is
     * modified while an iteration over the collection is in progress
     * (except through the iterator's own {@code remove} operation),
     * the results of the iteration are undefined.  The collection
     * supports element removal, which removes the corresponding
     * mapping from the map, via the {@code Iterator.remove},
     * {@code Collection.remove}, {@code removeAll},
     * {@code retainAll} and {@code clear} operations.  It does not
     * support the {@code add} or {@code addAll} operations.
     *
     * @return a collection view of the values contained in this map
     */
    @Override
    public Collection<V> values() {
        return StreamSupport.stream(new EntriesSpliterator<K, V>(entries, mapSize), false)
                .map(e -> e.getValue())
                .collect(Collectors.toList());
    }

    /**
     * Returns a {@link Set} view of the mappings contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  If the map is modified
     * while an iteration over the set is in progress (except through
     * the iterator's own {@code remove} operation, or through the
     * {@code setValue} operation on a map entry returned by the
     * iterator) the results of the iteration are undefined.  The set
     * supports element removal, which removes the corresponding
     * mapping from the map, via the {@code Iterator.remove},
     * {@code Set.remove}, {@code removeAll}, {@code retainAll} and
     * {@code clear} operations.  It does not support the
     * {@code add} or {@code addAll} operations.
     *
     * @return a set view of the mappings contained in this map
     */
    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return StreamSupport.stream(new EntriesSpliterator<K, V>(entries, mapSize), false)
                .collect(Collectors.toSet());
    }

    @Override
    public int size() {
        return mapSize;
    }

    private class EntriesSpliterator<K, V> implements Spliterator<Entry<K, V>> {

        private final Object[] entries;
        private int pos = 0;
        private int remainingEntries;

        public EntriesSpliterator(Object[] entries, int mapSize) {
            this.entries = entries;
            this.remainingEntries = mapSize;
        }


        @Override
        public boolean tryAdvance(Consumer<? super Entry<K, V>> action) {
            if(entries.length <= pos || 0 == remainingEntries) {
                return false;
            }
            Entry<K, V> entry;
            while (remainingEntries > 0 && pos < entries.length) {
                if((entry = (Entry<K, V>)entries[pos++]) != null) {
                    action.accept(entry);
                    remainingEntries--;
                    while (null != entry.next) {
                        action.accept(entry = entry.next);
                        remainingEntries--;
                    }
                }
            }
            return true;
        }


        @Override
        public Spliterator<Entry<K, V>> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return remainingEntries;
        }

        @Override
        public int characteristics() {
            return DISTINCT | NONNULL | SIZED;
        }
    }
}

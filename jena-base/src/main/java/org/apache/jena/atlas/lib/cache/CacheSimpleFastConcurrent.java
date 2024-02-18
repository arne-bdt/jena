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

package org.apache.jena.atlas.lib.cache;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.Cache;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

import static java.util.Arrays.asList;

/**
 * A simple fixed size cache that uses the hash code to address a slot.
 * The cache uses weak references to avoid holding on to objects that are not used elsewhere.
 * The clash policy is to overwrite.
 * The size is always a power of two.
 * <p>
 * The cache has very low overhead - there is no object creation during lookup or insert.
 * <p>
 * This implementation is thread safe.
 */
public class CacheSimpleFastConcurrent<K, V> implements Cache<K, V> {
    private final Entry<K, V>[] entries;
    private final Entry<K, V> empty = new EmptyEntry<>();
    private final int sizeMinusOne;
    private static class Entry<K, V> {
        private final WeakReference<K> k;
        private final WeakReference<V> v;

        public Entry(K key, V value) {
            this.k = new WeakReference<>(key);
            this.v = new WeakReference<>(value);
        }

        public K getKey() {
            return k.get();
        }

        public V getValue() {
            return v.get();
        }

        public boolean keyEquals(K key) {
            return key.equals(k.get());
        }

        public V getValueIfValid(final K key) {
            if (key.equals(k.get())) {
                return v.get();
            }
            return null;
        }
    }

    private static class EmptyEntry<K, V> extends Entry<K, V> {

        public EmptyEntry() {
            super(null, null);
        }

        @Override
        public K getKey() {
            return null;
        }

        @Override
        public V getValue() {
            return null;
        }

        @Override
        public V getValueIfValid(K key) {
            return null;
        }

        @Override
        public boolean keyEquals(K key) {
            return false;
        }
    }


    /**
     * Create a cache with a minimal size.
     * If the size is not a power of two, the size will be the next power of two.
     *
     * @param minSize the minimum size of the cache
     */
    public CacheSimpleFastConcurrent(final int minSize) {
        int size = Integer.highestOneBit(minSize);
        if (size < minSize){
            size <<= 1;
        }
        this.sizeMinusOne = size-1;
        @SuppressWarnings("unchecked")
        final Entry<K, V>[] e = (Entry<K, V>[])new Entry[size];
        Arrays.fill(e, empty);
        this.entries = e;
    }

    private int calcIndex(K key) {
        return key.hashCode() & sizeMinusOne;
    }

    @Override
    public boolean containsKey(K k) {
        return entries[calcIndex(k)].keyEquals(k);
    }

    @Override
    public V getIfPresent(K k) {
        return entries[calcIndex(k)].getValueIfValid(k);
    }

    @Override
    public V get(K k, Function<K, V> callable) {
        final int idx = calcIndex(k);
        final var value = entries[idx].getValueIfValid(k);
        if(value != null) {
            return value;
        }
        final var node = callable.apply(k);
        entries[idx] = new Entry<>(k, node);
        return node;
    }

    @Override
    public void put(K k, V thing) {
        final int idx = calcIndex(k);
        if(thing != null) {
            entries[idx] = new Entry<>(k, thing);
        } else {
            entries[idx] = empty;
        }
    }

    @Override
    public void remove(K k) {
        entries[calcIndex(k)] = empty;
    }

    @Override
    public Iterator<K> keys() {
        return Iter.iter(asList(entries))
                .filter(Objects::nonNull)
                .map(Entry::getKey)
                .filter(Objects::nonNull);
    }

    public Iterator<V> values() {
        return Iter.iter(asList(entries))
                .filter(Objects::nonNull)
                .map(Entry::getValue)
                .filter(Objects::nonNull);
    }

    @Override
    public boolean isEmpty() {
        return !values().hasNext();
    }

    @Override
    public void clear() {
        Arrays.fill(entries, empty);
    }

    @Override
    public long size() {
        return Iter.count(values());
    }
}

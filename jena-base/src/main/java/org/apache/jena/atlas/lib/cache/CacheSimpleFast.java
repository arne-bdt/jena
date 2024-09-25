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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

import static java.util.Arrays.asList;

/**
 * A simple fixed size cache that uses the hash code to address a slot.
 * The clash policy is to overwrite.
 * <p>
 * The cache has very low overhead - there is no object creation during lookup or insert.
 * <p>
 * This cache is not thread safe.
 */
public class CacheSimpleFast<K, V> implements Cache<K, V> {

    private final K[] keys;
    private final V[] values;

    private final int sizeMinusOne;

    private int currentSize = 0;

    public CacheSimpleFast(int miniumSize) {
        var size = Integer.highestOneBit(miniumSize);
        if (size < miniumSize){
            size <<= 1;
        }
        this.sizeMinusOne = size-1;
        @SuppressWarnings("unchecked")
        K[] x = (K[]) new Object[size];
        keys = x;
        @SuppressWarnings("unchecked")
        V[] z =  (V[]) new Object[size];
        this.values = z;
    }

    private int calcIndex(K key) {
        return key.hashCode() & sizeMinusOne;
    }

    @Override
    public boolean containsKey(K k) {
        return k.equals(keys[calcIndex(k)]);
    }

    @Override
    public V getIfPresent(K k) {
        final int idx = calcIndex(k);
        if (k.equals(keys[idx])) {
            return values[idx];
        }
        return null;
    }

    @Override
    public V get(K k, Function<K, V> callable) {
        final int idx = calcIndex(k);
        if(!k.equals(keys[idx])) {
            if(keys[idx] == null) {
                currentSize++;
            }
            keys[idx] = k;
            values[idx] = callable.apply(k);
        }
        return values[idx];
    }

    @Override
    public void put(K k, V thing) {
        final int idx = calcIndex(k);
        if(thing != null) {
            if(keys[idx] == null) {
                currentSize++;
            }
            keys[idx] = k;
            values[idx] = thing;
        } else if (keys[idx] != null) {
            keys[idx] = null;
            values[idx] = null;
            currentSize--;
        }
    }

    @Override
    public void remove(K k) {
        final int idx = calcIndex(k);
        if (k.equals(keys[idx])) {
            keys[idx] = null;
            values[idx] = null;
            currentSize--;
        }
    }

    @Override
    public Iterator<K> keys() {
        return Iter.iter(asList(keys)).filter(Objects::nonNull);
    }

    @Override
    public boolean isEmpty() {
        return currentSize == 0;
    }

    @Override
    public void clear() {
        Arrays.fill(keys, null);
        Arrays.fill(values, null);
        currentSize = 0;
    }

    @Override
    public long size() {
        return currentSize;
    }
}

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

package org.apache.jena.cimxml.utils;

import org.apache.jena.atlas.lib.Cache;
import org.apache.jena.atlas.lib.CacheFactory;

public class MapIndexedByLenghtFirst<K extends MapIndexedByLenghtFirst.HasLength, V> {
    private final int expectedMaxEntriesWithSameLength;
    private Cache<K, V>[] entriesWithSameLength;

    public interface HasLength {
        int length();
    }

    public MapIndexedByLenghtFirst(int expectedMaxByteLength, int expectedEntriesWithSameLength) {
        var positionsSize = Integer.highestOneBit(expectedMaxByteLength << 1);
        if (positionsSize < expectedMaxByteLength << 1) {
            positionsSize <<= 1;
        }
        this.entriesWithSameLength = new Cache[positionsSize];
        this.expectedMaxEntriesWithSameLength = expectedEntriesWithSameLength;
    }

    private void grow(final int minimumLength) {
        var newLength = entriesWithSameLength.length << 1;
        while (newLength < minimumLength) {
            newLength = minimumLength << 1;
        }
        final var oldValues = entriesWithSameLength;
        entriesWithSameLength = new Cache[newLength];
        System.arraycopy(oldValues, 0, entriesWithSameLength, 0, oldValues.length);
    }

    public void put(K key, V value) {
        final Cache<K, V> map;
        // Ensure the array is large enough
        if (entriesWithSameLength.length < key.length()) {
            grow(key.length());
            map = CacheFactory.createSimpleCache(expectedMaxEntriesWithSameLength);
            entriesWithSameLength[key.length()] = map;
            map.put(key, value);
            return;
        }
        if (entriesWithSameLength[key.length()] == null) {
            map = CacheFactory.createSimpleCache(expectedMaxEntriesWithSameLength);
            entriesWithSameLength[key.length()] = map;
        } else {
            map = entriesWithSameLength[key.length()];
        }
        map.put(key, value);
    }

    public V get(K key) {
        return getIfPresent(key);
    }

    public V getIfPresent(K key) {
        final var keyLength = key.length();
        if (entriesWithSameLength.length < keyLength || entriesWithSameLength[keyLength] == null) {
            return null;
        }
        return entriesWithSameLength[keyLength].getIfPresent(key);
    }

    public boolean containsKey(K key) {
        final var keyLength = key.length();
        if (entriesWithSameLength.length < keyLength || entriesWithSameLength[keyLength] == null) {
            return false;
        }
        return entriesWithSameLength[keyLength].containsKey(key);
    }

}

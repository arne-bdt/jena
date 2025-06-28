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


public class PrefixMap<K, V> {
    private K[] keys;
    private V[] values;
    private int size = 0;
    private final int initialSize;

    @SuppressWarnings({"unchecked"})
    public PrefixMap(int initialSize)  {
        this.initialSize = initialSize;
        this.keys = (K[]) new Object[initialSize];
        this.values = (V[]) new Object[initialSize];
    }

    private void growIfNeeded() {
        if(size >= keys.length) {
            final var newLength = keys.length + initialSize;
            final var oldKeys = keys;
            final var oldValues = values;
            keys = (K[]) new Object[newLength];
            values = (V[]) new Object[newLength];
            System.arraycopy(oldKeys, 0, keys, 0, oldKeys.length);
            System.arraycopy(oldValues, 0, values, 0, oldValues.length);
        }
    }

    public void put(K key, V value) {
        growIfNeeded();
        keys[size] = key;
        values[size] = value;
        size++;
    }

    public V get(K key) {
        for(int i = 0; i < size; i++) {
            if(keys[i].equals(key)) {
                return values[i];
            }
        }
        return null;
    }
}

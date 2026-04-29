/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *   SPDX-License-Identifier: Apache-2.0
 */
package org.apache.jena.mem2.collection;

import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * Extension of {@link JenaMap} that exposes index-based access and lets callers
 * supply a precomputed hash code for the key. Indices are stable handles to
 * entries (returned by {@link #putAndGetIndex(Object, Object)}) and remain
 * valid until the corresponding entry is removed.
 * <p>
 * The hash-code overloads are a performance shortcut for callers that already
 * have the hash at hand (typically because the same key is stored in several
 * collections). The supplied hash code MUST equal {@code key.hashCode()}, or
 * the map will misbehave.
 *
 * @param <K> the type of the keys in the map
 * @param <V> the type of the values in the map
 */
public interface JenaMapOptimized<K, V> extends JenaMap<K, V> {

    /**
     * Removes the entry stored at the given index.
     * The caller is responsible for using a valid index obtained from
     * {@link #indexOf(Object)} or {@link #putAndGetIndex(Object, Object)}.
     *
     * @param index the index of the entry to remove
     */
    void removeAt(int index);

    /**
     * Returns the index of the entry with the given key, or a negative value
     * if no such entry exists.
     *
     * @param key the key to look up
     * @return the index of the entry, or a negative value if absent
     */
    int indexOf(K key);

    /**
     * Returns the key stored at the given index.
     *
     * @param index the index of the entry
     * @return the key at that index
     */
    K getKeyAt(int index);

    /**
     * Returns the value stored at the given index.
     *
     * @param index the index of the entry
     * @return the value at that index
     */
    V getValueAt(int index);

    /**
     * Try to put a key-value pair into the map, supplying the precomputed
     * hash code of the key. If the key is already present, the value is updated.
     *
     * @param key      the key to put
     * @param hashCode {@code key.hashCode()} - must be consistent with {@link Object#hashCode()}
     * @param value    the value to put
     * @return {@code true} if the key was newly inserted; {@code false} if the
     *         key was already present and only the value was updated
     */
    boolean tryPut(K key, int hashCode, V value);

    /**
     * Put a key-value pair and return the index of the affected entry.
     * If the key is already present, its value is updated and the existing
     * index is returned.
     *
     * @param key   the key to put
     * @param value the value to put
     * @return the index of the entry holding {@code key}
     */
    int putAndGetIndex(K key, V value);

    /**
     * Put a key-value pair into the map, supplying the precomputed hash code
     * of the key. If the key is already present, the value is updated.
     *
     * @param key      the key to put
     * @param hashCode {@code key.hashCode()} - must be consistent with {@link Object#hashCode()}
     * @param value    the value to put
     */
    void put(K key, int hashCode, V value);

    /**
     * Get the value associated with the provided key, supplying the
     * precomputed hash code of the key.
     *
     * @param key      the key to look up
     * @param hashCode {@code key.hashCode()} - must be consistent with {@link Object#hashCode()}
     * @return the value associated with the key, or {@code null} if not present
     */
    V get(K key, int hashCode);

    /**
     * Get the value associated with the provided key, or {@code defaultValue}
     * if the key is not present, supplying the precomputed hash code of the key.
     *
     * @param key          the key to look up
     * @param hashCode     {@code key.hashCode()} - must be consistent with {@link Object#hashCode()}
     * @param defaultValue the default value to return if the key is not present
     * @return the value associated with the key, or {@code defaultValue} if absent
     */
    V getOrDefault(K key, int hashCode, V defaultValue);

    /**
     * Compute a value for a key if the key is not present, supplying the
     * precomputed hash code of the key. The value is automatically put into
     * the map.
     *
     * @param key                 the key whose value is to retrieved or computed
     * @param hashCode            {@code key.hashCode()} - must be consistent with {@link Object#hashCode()}
     * @param absentValueSupplier supplies a value for the key when it is missing
     * @return the existing value, or the newly computed value if the key was absent
     */
    V computeIfAbsent(K key, int hashCode, Supplier<V> absentValueSupplier);

    /**
     * Compute a (possibly new) value for a key, supplying the precomputed
     * hash code of the key.
     *
     * @param key            the key to compute a value for
     * @param hashCode       {@code key.hashCode()} - must be consistent with {@link Object#hashCode()}
     * @param valueProcessor receives the current value (or {@code null} if absent) and
     *                       returns the new value, or {@code null} to remove the entry
     */
    void compute(K key, int hashCode, UnaryOperator<V> valueProcessor);
}

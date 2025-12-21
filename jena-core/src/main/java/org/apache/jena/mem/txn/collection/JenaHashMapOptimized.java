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
package org.apache.jena.mem.txn.collection;


import org.apache.jena.mem.collection.JenaMap;
import org.apache.jena.mem.collection.JenaSet;

/**
 * Extension of {@link JenaSet} that allows to add and remove elements
 * with a given hash code.
 * This is useful if the hash code is already known.
 * Attention: The hash code must be consistent with E::hashCode().
 */
public interface JenaHashMapOptimized<K, V> extends JenaMap<K, V> {
    V get(K key, int hashCode);
    boolean tryPut(K key, int hashCode, V value);
    boolean put(K key, int hashCode, V value);
    int findPosition(final K e, final int hashCode);
}

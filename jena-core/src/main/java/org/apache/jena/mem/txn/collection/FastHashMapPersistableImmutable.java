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

import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public final class FastHashMapPersistableImmutable<K, V> extends FastHashMapPersistable<K, V> {

    private final FastHashMapPersistable<K, V> mutableParent;

    public FastHashMapPersistableImmutable(FastHashMapPersistable<K, V> base, boolean createImmutableChild) {
        super(base, createImmutableChild);
        this.mutableParent = createImmutableChild ? base : base.getMutableParent();
    }

    @Override
    public boolean isImmutable() {
        return true;
    }

    @Override
    public FastHashMapPersistable<K, V> getMutableParent() {
        return mutableParent;
    }

    @Override
    public final FastHashMapPersistable<K, V> createImmutableChild() {
        throw new UnsupportedOperationException("This map is already immutable");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("This map is read-only");
    }

    @Override
    public boolean tryPut(K key, V value) {
        throw new UnsupportedOperationException("This map is read-only");
    }

    @Override
    public void put(K key, V value) {
        throw new UnsupportedOperationException("This map is read-only");
    }

    @Override
    public V computeIfAbsent(K key, Supplier<V> absentValueSupplier) {
        throw new UnsupportedOperationException("This map is read-only");
    }

    @Override
    public void compute(K key, UnaryOperator<V> valueProcessor) {
        throw new UnsupportedOperationException("This map is read-only");
    }

    @Override
    public boolean tryRemove(K o) {
        throw new UnsupportedOperationException("This map is read-only");
    }

    @Override
    public boolean tryRemove(K e, int hashCode) {
        throw new UnsupportedOperationException("This map is read-only");
    }

    @Override
    public int removeAndGetIndex(K e) {
        throw new UnsupportedOperationException("This map is read-only");
    }

    @Override
    public int removeAndGetIndex(K e, int hashCode) {
        throw new UnsupportedOperationException("This map is read-only");
    }

    @Override
    public void removeUnchecked(K e) {
        throw new UnsupportedOperationException("This map is read-only");
    }

    @Override
    public void removeUnchecked(K e, int hashCode) {
        throw new UnsupportedOperationException("This map is read-only");
    }
}

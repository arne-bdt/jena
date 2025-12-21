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

public final class FastHashSetPersistableImmutable<K> extends FastHashSetPersistable<K> {

    private final FastHashSetPersistable<K> mutableParent;

    public FastHashSetPersistableImmutable(FastHashSetPersistable<K> base, boolean createImmutableChild) {
        super(base, createImmutableChild);
        this.mutableParent = createImmutableChild ? base : base.getMutableParent();
    }

    @Override
    public boolean isImmutable() {
        return true;
    }

    @Override
    public FastHashSetPersistable<K> getMutableParent() {
        return mutableParent;
    }

    @Override
    public FastHashSetPersistable<K> createImmutableChild() {
        throw new UnsupportedOperationException("This map is already immutable");
    }

    @Override
    public boolean tryAdd(K key) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public boolean tryAdd(K value, int hashCode) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public int addAndGetIndex(K value) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public int addAndGetIndex(K value, int hashCode) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public void addUnchecked(K key) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public void addUnchecked(K value, int hashCode) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public boolean tryRemove(K o) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public boolean tryRemove(K e, int hashCode) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public int removeAndGetIndex(K e) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public int removeAndGetIndex(K e, int hashCode) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public void removeUnchecked(K e) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public void removeUnchecked(K e, int hashCode) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("This set is read-only");
    }
}

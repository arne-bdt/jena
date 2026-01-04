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
package org.apache.jena.mem.txn.store;

import org.apache.jena.atlas.lib.Copyable;
import org.apache.jena.graph.Node;
import org.apache.jena.mem.txn.Persistable;
import org.apache.jena.mem.txn.collection.FastHashMapPersistable;
import org.apache.jena.mem.txn.collection.FastHashMapPersistableImmutable;

import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * Map from nodes to triple bunches.
 */
public class FastHashedBunchMapPersistableImmutable
        extends FastHashedBunchMapPersistable {

    private final FastHashedBunchMapPersistable mutableParent;

    public FastHashedBunchMapPersistableImmutable(FastHashedBunchMapPersistable base, boolean createImmutableChild) {
        super(base, createImmutableChild);
        this.mutableParent = createImmutableChild ? base : base.getMutableParent();
    }

    @Override
    public boolean isImmutable() {
        return true;
    }

    public FastHashedBunchMapPersistable getMutableParent() {
        return mutableParent;
    }

    @Override
    public FastHashedBunchMapPersistableImmutable createImmutableChild() {
        throw new UnsupportedOperationException("This map is already immutable");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("This map is read-only");
    }

    @Override
    public boolean tryPut(Node key, int hashCode, FastTripleBunchPersistable value) {
        throw new UnsupportedOperationException("This map is read-only");
    }

    @Override
    public boolean put(Node key, int hashCode, FastTripleBunchPersistable value) {
        throw new UnsupportedOperationException("This map is read-only");
    }

    @Override
    public boolean tryPut(Node key, FastTripleBunchPersistable value) {
        throw new UnsupportedOperationException("This map is read-only");
    }

    @Override
    public void put(Node key, FastTripleBunchPersistable value) {
        throw new UnsupportedOperationException("This map is read-only");
    }

    @Override
    public FastTripleBunchPersistable computeIfAbsent(Node key, Supplier<FastTripleBunchPersistable> absentValueSupplier) {
        throw new UnsupportedOperationException("This map is read-only");
    }

    @Override
    public void compute(Node key, UnaryOperator<FastTripleBunchPersistable> valueProcessor) {
        throw new UnsupportedOperationException("This map is read-only");
    }

    @Override
    public boolean tryRemove(Node o) {
        throw new UnsupportedOperationException("This map is read-only");
    }

    @Override
    public boolean tryRemove(Node e, int hashCode) {
        throw new UnsupportedOperationException("This map is read-only");
    }

    @Override
    public int removeAndGetIndex(Node e) {
        throw new UnsupportedOperationException("This map is read-only");
    }

    @Override
    public int removeAndGetIndex(Node e, int hashCode) {
        throw new UnsupportedOperationException("This map is read-only");
    }

    @Override
    public void removeUnchecked(Node e) {
        throw new UnsupportedOperationException("This map is read-only");
    }

    @Override
    public void removeUnchecked(Node e, int hashCode) {
        throw new UnsupportedOperationException("This map is read-only");
    }

    @Override
    public FastHashedBunchMapPersistable copy() {
        return new FastHashedBunchMapPersistableImmutable(this, false);
    }
}

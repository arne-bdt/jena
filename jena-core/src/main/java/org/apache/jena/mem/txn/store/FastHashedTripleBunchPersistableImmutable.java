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

import org.apache.jena.graph.Triple;
import org.apache.jena.mem.collection.FastHashSet;
import org.apache.jena.mem.collection.JenaSet;
import org.apache.jena.mem.txn.collection.FastHashSetPersistable;

/**
 * A set of triples - backed by {@link FastHashSet}.
 */
public class FastHashedTripleBunchPersistableImmutable extends FastHashedTripleBunchPersistable {

    private final FastHashedTripleBunchPersistable mutableParentBunch;

    /**
     * Copy constructor.
     * The new bunch will contain all the same triples of the bunch to copy.
     *
     * @param bunchToCopy
     */
    private FastHashedTripleBunchPersistableImmutable(final FastHashedTripleBunchPersistable bunchToCopy, boolean createImmutableChild) {
        super(bunchToCopy, createImmutableChild);
        this.mutableParentBunch = createImmutableChild ? bunchToCopy : bunchToCopy.getMutableParentBunch();
    }

    @Override
    public FastHashedTripleBunchPersistable getMutableParentBunch() {
        return mutableParentBunch;
    }

    @Override
    public FastHashedTripleBunchPersistable createImmutableChildBunch() {
        throw new UnsupportedOperationException("This bunch is already immutable.");
    }

    @Override
    public FastHashedTripleBunchPersistableImmutable copy() {
        return new FastHashedTripleBunchPersistableImmutable(this, false);
    }

    @Override
    public boolean tryAdd(Triple key) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public boolean tryAdd(Triple value, int hashCode) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public int addAndGetIndex(Triple value) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public int addAndGetIndex(Triple value, int hashCode) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public void addUnchecked(Triple key) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public void addUnchecked(Triple value, int hashCode) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public boolean tryRemove(Triple o) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public boolean tryRemove(Triple e, int hashCode) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public int removeAndGetIndex(Triple e) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public int removeAndGetIndex(Triple e, int hashCode) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public void removeUnchecked(Triple e) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public void removeUnchecked(Triple e, int hashCode) {
        throw new UnsupportedOperationException("This set is read-only");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("This set is read-only");
    }
}

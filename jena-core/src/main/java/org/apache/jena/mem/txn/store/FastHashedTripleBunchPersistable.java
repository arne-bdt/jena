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
import org.apache.jena.mem.txn.Persistable;
import org.apache.jena.mem.txn.collection.FastHashSetPersistable;

/**
 * A set of triples - backed by {@link FastHashSet}.
 */
public class FastHashedTripleBunchPersistable extends FastHashSetPersistable<Triple> implements FastTripleBunchPersistable {
    /**
     * Create a new triple bunch from the given set of triples.
     *
     * @param set the set of triples
     */
    public FastHashedTripleBunchPersistable(final JenaSet<Triple> set) {
        super((set.size() >> 1) + set.size()); //it should not only fit but also have some space for growth
        set.keyIterator().forEachRemaining(this::addUnchecked);
    }

    /**
     * Copy constructor.
     * The new bunch will contain all the same triples of the bunch to copy.
     *
     * @param bunchToCopy
     */
    protected FastHashedTripleBunchPersistable(final FastHashedTripleBunchPersistable bunchToCopy, boolean createImmutableChild) {
        super(bunchToCopy, createImmutableChild);
    }

    public FastHashedTripleBunchPersistable() {
        super();
    }

    @Override
    public boolean isArray() {
        return false;
    }

    @Override
    public FastHashedTripleBunchPersistable getMutableParentBunch() {
        throw new UnsupportedOperationException("This bunch is already mutable.");
    }

    @Override
    public FastHashedTripleBunchPersistable createImmutableChildBunch() {
        return null;
    }

    @Override
    public FastHashedTripleBunchPersistable copy() {
        return new FastHashedTripleBunchPersistable(this, false);
    }
}

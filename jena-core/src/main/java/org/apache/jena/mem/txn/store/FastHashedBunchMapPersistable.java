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
import org.apache.jena.mem.collection.FastHashMap;
import org.apache.jena.mem.txn.Persistable;
import org.apache.jena.mem.txn.collection.FastHashMapPersistable;
import org.apache.jena.mem.txn.collection.FastHashSetPersistable;

import java.util.function.UnaryOperator;

/**
 * Map from nodes to triple bunches.
 */
public class FastHashedBunchMapPersistable
        extends FastHashMapPersistable<Node, FastTripleBunchPersistable>
        implements Copyable<FastHashedBunchMapPersistable> {

    public FastHashedBunchMapPersistable() {
        super();
    }

    /**
     * Copy constructor.
     * The new map will contain all the same nodes as keys of the map to copy, but copies of the bunches as values .
     *
     * @param mapToCopy
     */
    protected FastHashedBunchMapPersistable(final FastHashedBunchMapPersistable mapToCopy, final UnaryOperator<FastTripleBunchPersistable> valueProcessor) {
        super(mapToCopy, valueProcessor);
    }

    protected FastHashedBunchMapPersistable(final FastHashedBunchMapPersistable base, boolean createRevision) {
        super(base, createRevision);
    }

    @Override
    public FastHashedBunchMapPersistable copy() {
        return new FastHashedBunchMapPersistable(this, FastTripleBunchPersistable::copy);
    }

    @Override
    public FastHashedBunchMapPersistable createImmutableChild() {
        return new FastHashedBunchMapPersistableImmutable(this, true);
    }
}

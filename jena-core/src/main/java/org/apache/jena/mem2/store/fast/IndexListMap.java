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
package org.apache.jena.mem2.store.fast;

import org.apache.jena.atlas.lib.Copyable;
import org.apache.jena.graph.Node;
import org.apache.jena.mem2.collection.FastHashMap;
import org.apache.jena.mem2.store.roaring.IndexList;

/**
 * Map from nodes to triple bunches.
 */
public class IndexListMap
        extends FastHashMap<Node, DoubleIndexList>
        implements Copyable<IndexListMap> {

    public IndexListMap() {
        super();
    }

    /**
     * Copy constructor.
     * The new map will contain all the same nodes as keys of the map to copy, but copies of the bunches as values .
     *
     * @param mapToCopy
     */
    private IndexListMap(final IndexListMap mapToCopy) {
        super(mapToCopy, DoubleIndexList::copy);
    }

    @Override
    protected Node[] newKeysArray(int size) {
        return new Node[size];
    }

    @Override
    protected DoubleIndexList[] newValuesArray(int size) {
        return new DoubleIndexList[size];
    }

    @Override
    public IndexListMap copy() {
        return new IndexListMap(this);
    }
}

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

package org.apache.jena.mem2;

import org.apache.jena.mem2.collection.FastHashBase;
import org.apache.jena.mem2.store.TripleStore;
import org.apache.jena.mem2.store.fast.FastTripleStore;

/**
 * In-memory {@link GraphMem} implementation that uses a {@link FastTripleStore}
 * built on top of {@link FastHashBase}-based maps and sets.
 * This class is not thread-safe.
 * <p>
 * Compared to the legacy {@code GraphMem} implementation, this graph is
 * significantly faster for {@code add}, {@code find} and {@code stream}
 * operations. {@code delete} can be slightly slower, and memory consumption
 * is moderately higher (typically 6-35%) because of the more elaborate
 * data structures. See {@link FastTripleStore} for details on the
 * optimizations and on the index layout (three node-keyed maps for subjects,
 * predicates and objects, with array bunches that promote to hashed bunches
 * once they exceed a threshold).
 */
public class GraphMemFast extends GraphMem {

    /**
     * Creates a new, empty graph backed by a fresh {@link FastTripleStore}.
     */
    public GraphMemFast() {
        super(new FastTripleStore());
    }

    /**
     * Internal constructor used by {@link #copy()} to wrap an already
     * populated triple store.
     *
     * @param tripleStore the (already populated) triple store to wrap
     */
    private GraphMemFast(final TripleStore tripleStore) {
        super(tripleStore);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns an independent {@link GraphMemFast} instance whose store is a
     * deep-enough copy of this graph's store. Since {@link org.apache.jena.graph.Triple}
     * and {@link org.apache.jena.graph.Node} are immutable, the copy and the
     * original may share the same triple and node instances.
     */
    @Override
    public GraphMemFast copy() {
        return new GraphMemFast(this.tripleStore.copy());
    }
}

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

package org.apache.jena.mem2.store.indexed;

import org.apache.jena.atlas.lib.Copyable;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.collection.FastHashSet;

import java.util.function.IntConsumer;

/**
 * Set of triples that is backed by a {@link TripleSet}.
 */
public class TripleSet
        extends FastHashSet<Triple>
        implements Copyable<TripleSet> {

    private IntConsumer onKeysGrowHook = null;

    public void setOnKeysGrowHook(IntConsumer onKeysGrowHook) {
        this.onKeysGrowHook = onKeysGrowHook;
    }

    public TripleSet() {
        super();
    }

    private TripleSet(final TripleSet setToCopy) {
        super(setToCopy);
    }

    @Override
    protected void growKeysAndHashCodeArrays() {
        super.growKeysAndHashCodeArrays();
        if(onKeysGrowHook != null) {
            onKeysGrowHook.accept(getInternalKeysLength());
        }
    }

    /**
     * Create a copy of this set.
     *
     * @return TripleSet
     */
    @Override
    public TripleSet copy() {
        return new TripleSet(this);
    }
}

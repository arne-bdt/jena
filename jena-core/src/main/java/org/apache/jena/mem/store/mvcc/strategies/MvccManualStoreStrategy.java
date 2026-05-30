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

package org.apache.jena.mem.store.mvcc.strategies;

import org.apache.jena.graph.Triple;

/**
 * Manual MVCC strategy: keeps no index and answers every lookup by a dense scan
 * until the index is explicitly built (under the writer lock), after which it
 * behaves exactly like {@link MvccEagerStoreStrategy}. Useful for the
 * bulk-load-then-index pattern: load with no index maintenance overhead, then
 * build the index once.
 * <p>
 * The built delegate is published through a {@code volatile} so that the
 * write-time build is visible to lock-free readers once installed.
 */
public final class MvccManualStoreStrategy implements MvccStoreStrategy {

    /** Non-null once the index has been built; published volatile. */
    private volatile MvccEagerStoreStrategy delegate = null;

    /**
     * Install a freshly built eager index. Called by the store under the writer
     * lock from {@code initializeIndex()}.
     *
     * @param built the populated eager strategy to delegate to
     */
    public void install(final MvccEagerStoreStrategy built) {
        this.delegate = built;
    }

    @Override
    public Candidates candidates(final Triple match) {
        final MvccEagerStoreStrategy d = delegate;
        return d == null ? Candidates.DENSE : d.candidates(match);
    }

    @Override
    public void onCommitAdd(final Triple t, final int slot) {
        final MvccEagerStoreStrategy d = delegate;
        if (d != null) {
            d.onCommitAdd(t, slot);
        }
    }

    @Override
    public boolean isIndexInitialized() {
        return delegate != null;
    }

    @Override
    public void clear() {
        delegate = null;
    }
}

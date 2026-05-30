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
 * Minimal MVCC strategy: maintains no auxiliary index. Every lookup scans the
 * whole dense slot range and is answered by the store's version filter plus
 * full-pattern match. Lowest memory; suited to small graphs or memory-critical
 * deployments.
 */
public final class MvccMinimalStoreStrategy implements MvccStoreStrategy {

    @Override
    public Candidates candidates(final Triple match) {
        return Candidates.DENSE;
    }

    @Override
    public void onCommitAdd(final Triple t, final int slot) {
        // no index to maintain
    }

    @Override
    public boolean isIndexInitialized() {
        return false;
    }

    @Override
    public void clear() {
        // nothing to clear
    }
}

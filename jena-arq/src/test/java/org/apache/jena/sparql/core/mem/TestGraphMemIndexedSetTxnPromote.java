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

package org.apache.jena.sparql.core.mem;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.Transactional;
import org.junit.jupiter.api.Test;

/**
 * {@code promote()} on a plain {@code READ} transaction must return
 * {@code false} (per the {@code Transactional} contract's default-method
 * dispatch), not throw. The two transactional graph implementations
 * ({@link GraphMemIndexedSetTxn} and {@link GraphMemIndexedSetCowTxn}) must
 * agree on this behaviour.
 */
public class TestGraphMemIndexedSetTxnPromote {

    @Test
    public void plainReadPromoteReturnsFalse_nonCow() {
        GraphMemIndexedSetTxn g = new GraphMemIndexedSetTxn();
        g.begin(TxnType.READ);
        try {
            assertFalse(g.promote());
            assertFalse(g.promote(Transactional.Promote.ISOLATED));
            assertFalse(g.promote(Transactional.Promote.READ_COMMITTED));
        } finally {
            g.end();
        }
    }

    @Test
    public void plainReadPromoteReturnsFalse_cow() {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();
        g.begin(TxnType.READ);
        try {
            assertFalse(g.promote());
            assertFalse(g.promote(Transactional.Promote.ISOLATED));
            assertFalse(g.promote(Transactional.Promote.READ_COMMITTED));
        } finally {
            g.end();
        }
    }

    @Test
    public void readPromoteIsPromotable_nonCow() {
        GraphMemIndexedSetTxn g = new GraphMemIndexedSetTxn();
        g.begin(TxnType.READ_PROMOTE);
        try {
            assertTrue(g.promote());
            g.commit();
        } finally {
            g.end();
        }
    }

    @Test
    public void readPromoteIsPromotable_cow() {
        GraphMemIndexedSetCowTxn g = new GraphMemIndexedSetCowTxn();
        g.begin(TxnType.READ_PROMOTE);
        try {
            assertTrue(g.promote());
            g.commit();
        } finally {
            g.end();
        }
    }
}

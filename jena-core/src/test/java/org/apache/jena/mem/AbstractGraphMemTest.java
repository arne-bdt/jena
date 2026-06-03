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

package org.apache.jena.mem;

import static org.apache.jena.testing_framework.GraphHelper.triple;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Contract tests for {@link GraphMem} implementations: the full {@link org.apache.jena.graph.Graph}
 * surface inherited from {@link AbstractGraphTest}, plus the
 * {@link GraphMem}-specific {@link org.apache.jena.atlas.lib.Copyable#copy() copy()}
 * behaviour exercised here.
 */
public abstract class AbstractGraphMemTest extends AbstractGraphTest<GraphMem> {

    @Test
    public void testCopy() {
        sut.add(triple("s p o"));
        sut.add(triple("s1 p1 o1"));
        sut.add(triple("s2 p2 o2"));
        assertEquals(3, sut.size());

        var copy = sut.copy();
        assertEquals(3, copy.size());
        assertTrue(copy.contains(triple("s p o")));
        assertTrue(copy.contains(triple("s1 p1 o1")));
        assertTrue(copy.contains(triple("s2 p2 o2")));
        assertFalse(copy.contains(triple("s3 p3 o3")));
    }

    @Test
    public void testCopyHasNoSideEffects() {
        sut.add(triple("s p o"));
        sut.add(triple("s1 p1 o1"));
        sut.add(triple("s2 p2 o2"));
        assertEquals(3, sut.size());

        var copy = sut.copy();
        copy.delete(triple("s1 p1 o1"));
        copy.add(triple("s3 p3 o3"));
        copy.add(triple("s4 p4 o4"));

        assertEquals(4, copy.size());
        assertTrue(copy.contains(triple("s p o")));
        assertFalse(copy.contains(triple("s1 p1 o1")));
        assertTrue(copy.contains(triple("s2 p2 o2")));
        assertTrue(copy.contains(triple("s3 p3 o3")));
        assertTrue(copy.contains(triple("s4 p4 o4")));


        assertEquals(3, sut.size());
        assertTrue(sut.contains(triple("s p o")));
        assertTrue(sut.contains(triple("s1 p1 o1")));
        assertTrue(sut.contains(triple("s2 p2 o2")));
        assertFalse(sut.contains(triple("s3 p3 o3")));
    }
}

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
package org.apache.jena.mem2.pattern;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Unit tests for the {@link MatchPattern} enum: verifies the eight
 * concrete/wildcard combinations are present and that round-tripping
 * through {@link Enum#valueOf(Class, String)} works as expected.
 */
public class MatchPatternTest {

    @Test
    public void hasExactlyEightValuesCoveringAllSPOCombinations() {
        // The enum must enumerate every combination of three slots, each of
        // which is either concrete or a wildcard: 2^3 = 8 values.
        assertEquals(8, MatchPattern.values().length);
    }

    @Test
    public void allEightExpectedValuesArePresent() {
        // Spot-check every name; if any disappears or is renamed, the
        // PatternClassifier dispatch will silently break.
        assertNotNull(MatchPattern.valueOf("SUB_PRE_OBJ"));
        assertNotNull(MatchPattern.valueOf("SUB_PRE_ANY"));
        assertNotNull(MatchPattern.valueOf("SUB_ANY_OBJ"));
        assertNotNull(MatchPattern.valueOf("SUB_ANY_ANY"));
        assertNotNull(MatchPattern.valueOf("ANY_PRE_OBJ"));
        assertNotNull(MatchPattern.valueOf("ANY_PRE_ANY"));
        assertNotNull(MatchPattern.valueOf("ANY_ANY_OBJ"));
        assertNotNull(MatchPattern.valueOf("ANY_ANY_ANY"));
    }

    @Test
    public void valueOfRoundTripsForEveryConstant() {
        for (MatchPattern p : MatchPattern.values()) {
            assertEquals(p, MatchPattern.valueOf(p.name()));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void valueOfRejectsUnknownNames() {
        MatchPattern.valueOf("NOT_A_PATTERN");
    }
}

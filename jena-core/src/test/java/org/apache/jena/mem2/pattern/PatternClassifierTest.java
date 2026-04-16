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

import static org.apache.jena.testing_framework.GraphHelper.node;
import static org.apache.jena.testing_framework.GraphHelper.triple;
import static org.junit.Assert.assertEquals;

public class PatternClassifierTest {

    @Test
    public void testClassifyTriple() {
        assertEquals(org.apache.jena.mem2.pattern.MatchPattern.SUB_PRE_OBJ, org.apache.jena.mem2.pattern.PatternClassifier.classify(triple("s p o")));
        assertEquals(org.apache.jena.mem2.pattern.MatchPattern.SUB_PRE_ANY, org.apache.jena.mem2.pattern.PatternClassifier.classify(triple("s p ??")));
        assertEquals(org.apache.jena.mem2.pattern.MatchPattern.SUB_ANY_OBJ, org.apache.jena.mem2.pattern.PatternClassifier.classify(triple("s ?? o")));
        assertEquals(org.apache.jena.mem2.pattern.MatchPattern.SUB_ANY_ANY, org.apache.jena.mem2.pattern.PatternClassifier.classify(triple("s ?? ??")));
        assertEquals(org.apache.jena.mem2.pattern.MatchPattern.ANY_PRE_OBJ, org.apache.jena.mem2.pattern.PatternClassifier.classify(triple("?? p o")));
        assertEquals(org.apache.jena.mem2.pattern.MatchPattern.ANY_PRE_ANY, org.apache.jena.mem2.pattern.PatternClassifier.classify(triple("?? p ??")));
        assertEquals(org.apache.jena.mem2.pattern.MatchPattern.ANY_ANY_OBJ, org.apache.jena.mem2.pattern.PatternClassifier.classify(triple("?? ?? o")));
        assertEquals(org.apache.jena.mem2.pattern.MatchPattern.ANY_ANY_ANY, org.apache.jena.mem2.pattern.PatternClassifier.classify(triple("?? ?? ??")));
    }

    @Test
    public void testClassifyNodes() {
        assertEquals(org.apache.jena.mem2.pattern.MatchPattern.SUB_PRE_OBJ, org.apache.jena.mem2.pattern.PatternClassifier.classify(node("s"), node("p"), node("o")));
        assertEquals(org.apache.jena.mem2.pattern.MatchPattern.SUB_PRE_ANY, org.apache.jena.mem2.pattern.PatternClassifier.classify(node("s"), node("p"), node("??")));
        assertEquals(org.apache.jena.mem2.pattern.MatchPattern.SUB_ANY_OBJ, org.apache.jena.mem2.pattern.PatternClassifier.classify(node("s"), node("??"), node("o")));
        assertEquals(org.apache.jena.mem2.pattern.MatchPattern.SUB_ANY_ANY, org.apache.jena.mem2.pattern.PatternClassifier.classify(node("s"), node("??"), node("??")));
        assertEquals(org.apache.jena.mem2.pattern.MatchPattern.ANY_PRE_OBJ, org.apache.jena.mem2.pattern.PatternClassifier.classify(node("??"), node("p"), node("o")));
        assertEquals(org.apache.jena.mem2.pattern.MatchPattern.ANY_PRE_ANY, org.apache.jena.mem2.pattern.PatternClassifier.classify(node("??"), node("p"), node("??")));
        assertEquals(org.apache.jena.mem2.pattern.MatchPattern.ANY_ANY_OBJ, org.apache.jena.mem2.pattern.PatternClassifier.classify(node("??"), node("??"), node("o")));
        assertEquals(MatchPattern.ANY_ANY_ANY, PatternClassifier.classify(node("??"), node("??"), node("??")));
    }

}
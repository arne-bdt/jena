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

import org.apache.jena.mem2.collection.*;
import org.apache.jena.mem2.iterator.IteratorOfJenaSetsTest;
import org.apache.jena.mem2.iterator.SparseArrayIndexedIteratorTest;
import org.apache.jena.mem2.iterator.SparseArrayIteratorTest;
import org.apache.jena.mem2.pattern.MatchPatternTest;
import org.apache.jena.mem2.pattern.PatternClassifierTest;
import org.apache.jena.mem2.spliterator.ArraySpliteratorTest;
import org.apache.jena.mem2.spliterator.ArraySubSpliteratorTest;
import org.apache.jena.mem2.spliterator.SparseArrayIndexedSpliteratorTest;
import org.apache.jena.mem2.spliterator.SparseArraySpliteratorTest;
import org.apache.jena.mem2.spliterator.SparseArraySubSpliteratorTest;
import org.apache.jena.mem2.store.fast.FastArrayBunchTest;
import org.apache.jena.mem2.store.fast.FastHashedBunchMapTest;
import org.apache.jena.mem2.store.fast.FastHashedTripleBunchTest;
import org.apache.jena.mem2.store.fast.FastTripleStoreTest;
import org.apache.jena.mem2.store.indexed.IndexListIteratorTest;
import org.apache.jena.mem2.store.indexed.IndexListSpliteratorTest;
import org.apache.jena.mem2.store.indexed.IndexListTest;
import org.apache.jena.mem2.store.indexed.IndexListsIteratorTest;
import org.apache.jena.mem2.store.indexed.IndexListsSpliteratorTest;
import org.apache.jena.mem2.store.indexed.IndexedSetTripleStoreTest;
import org.apache.jena.mem2.store.indexed.NodesToIndicesTest;
import org.apache.jena.mem2.store.indexed.TripleSetTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Aggregating JUnit 4 suite for the {@code org.apache.jena.mem2} module.
 * Lists every concrete test class that exercises the in-memory graph stack
 * (spliterators, iterators, hash collections, triple-store implementations,
 * pattern classification and the {@code GraphMem} variants).
 * <p>
 * Abstract test bases (e.g. {@code AbstractGraphMemTest},
 * {@code AbstractTripleStoreTest}) are intentionally not listed here; they are
 * executed via their concrete subclasses.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses( {
    // spliterator/
    SparseArraySubSpliteratorTest.class,
    ArraySubSpliteratorTest.class,
    ArraySpliteratorTest.class,
    SparseArraySpliteratorTest.class,
    SparseArrayIndexedSpliteratorTest.class,

    // iterator/
    IteratorOfJenaSetsTest.class,
    SparseArrayIteratorTest.class,
    SparseArrayIndexedIteratorTest.class,

    // collection/
    FastHashMapTest.class,
    FastHashMapTest2.class,
    FastHashSetTest.class,
    FastHashSetTest2.class,

    // store/fast
    FastTripleStoreTest.class,
    FastArrayBunchTest.class,
    FastHashedTripleBunchTest.class,
    FastHashedBunchMapTest.class,

    // store/indexed
    IndexedSetTripleStoreTest.class,
    IndexListTest.class,
    IndexListIteratorTest.class,
    IndexListSpliteratorTest.class,
    IndexListsIteratorTest.class,
    IndexListsSpliteratorTest.class,
    NodesToIndicesTest.class,
    TripleSetTest.class,

    // pattern/
    PatternClassifierTest.class,
    MatchPatternTest.class,

    // mem2/
    GraphMemFastTest.class,
    GraphMemIndexedSetTest.class,
    GraphMemTest.class,
    IndexingStrategyTest.class
} )
public class TS4_GraphMem {}

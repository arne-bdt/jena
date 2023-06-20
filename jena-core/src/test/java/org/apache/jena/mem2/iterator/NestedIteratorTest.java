/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jena.mem2.iterator;

import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.function.Function;

import static org.junit.Assert.*;

public class NestedIteratorTest {
    private static final List<String> EMPTY_LIST = Collections.emptyList();
    private NestedIterator<String, List<String>> nestedIterator;
    private Iterator<List<String>> parentIterator;
    private Function<List<String>, ExtendedIterator<String>> mapper;

    @Before
    public void setUp() {
        parentIterator = Arrays.asList(
                Arrays.asList("1.1", "1.2"),
                Arrays.asList("2.1", "2.2"),
                Arrays.asList("3.1", "3.2")
        ).iterator();
        mapper = l -> WrappedIterator.create(l.iterator());
    }

    @Test
    public void testHasNext() {
        nestedIterator = new NestedIterator<>(parentIterator, mapper);
        assertTrue(nestedIterator.hasNext());
    }

    @Test
    public void testNext() {
        nestedIterator = new NestedIterator<>(parentIterator, mapper);
        assertTrue(nestedIterator.hasNext());
        assertEquals("1.1", nestedIterator.next());
        assertEquals("1.2", nestedIterator.next());
        assertEquals("2.1", nestedIterator.next());

    }

    @Test(expected = NoSuchElementException.class)
    public void testNextWithNoElements() {
        parentIterator = Arrays.asList(
                EMPTY_LIST,
                EMPTY_LIST,
                EMPTY_LIST
        ).iterator();
        nestedIterator = new NestedIterator<>(parentIterator, mapper);
        nestedIterator.next();  // should throw NoSuchElementException
    }

    @Test
    public void testForEachRemaining() {
        nestedIterator = new NestedIterator<>(parentIterator, mapper);
        int[] count = {0};
        nestedIterator.forEachRemaining(element -> {
            assertTrue(element.endsWith(".1") || element.endsWith(".2"));
            count[0]++;
        });
        assertEquals(6, count[0]);
    }

    @Test
    public void testHasNextNext() {
        nestedIterator = new NestedIterator<>(parentIterator, mapper);
        assertTrue(nestedIterator.hasNext());
        assertEquals("1.1", nestedIterator.next());
        assertTrue(nestedIterator.hasNext());
        assertEquals("1.2", nestedIterator.next());
        assertTrue(nestedIterator.hasNext());
        assertEquals("2.1", nestedIterator.next());
        assertTrue(nestedIterator.hasNext());
        assertEquals("2.2", nestedIterator.next());
        assertTrue(nestedIterator.hasNext());
        assertEquals("3.1", nestedIterator.next());
        assertTrue(nestedIterator.hasNext());
        assertEquals("3.2", nestedIterator.next());
        assertFalse(nestedIterator.hasNext());
    }
}
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
package org.apache.jena.mem.map;

import org.apache.jena.mem2.collection.HashCommonMap;
import org.apache.jena.mem2.collection.JenaMap;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Original code comes from: https://gist.github.com/seadowg/1431727
 */
public class TestHashCommonMap {
    private JenaMap<String, Integer> map;

    // Set up an empty map before each test
    @Before
    public void setUp() {
        this.map = new HashCommonMap<String, Integer>(10) {
            @Override
            public void clear() {
                super.clear(10);
            }

            @Override
            protected Integer[] newValuesArray(int size) {
                return new Integer[size];
            }

            @Override
            protected String[] newKeysArray(int size) {
                return new String[size];
            }
        };
    }

    // Check that a new HashMap returns 'true' for isEmpty
    @Test
    public void testIsEmptyForNewMap() {
        assertTrue(map.isEmpty());
    }

    // Test adding an element makes isEmpty return 'false'
    @Test
    public void testAddMakesIsEmptyFalse() {
        map.put("Hello", 5);
        assertFalse(map.isEmpty());
    }

    // Check that size returns 0 for new HashMaps
    @Test
    public void testSizeForNewMap() {
        assertEquals(0, map.size());
    }

    // Test size increases as elements are added
    @Test
    public void testSizeIncrementsWhenAddingElements() {
        map.put("Hello", 5);
        assertEquals(1, map.size());

        map.put("Goodbye", 5);
        assertEquals(2, map.size());
    }

    // Make sure get returns the values added under keys
    @Test
    public void testGetReturnsCorrectValue() {
        map.put("Hello", 5);
        map.put("Goodbye", 6);
        assertEquals(5, map.get("Hello").intValue());
        assertEquals(6, map.get("Goodbye").intValue());
    }

    // Test that an exception is thrown if a key does not exist
    @Test
    public void testThrowsExceptionIfKeyDoesNotExist() {
        assertNull(map.get("Hello"));
    }

    // Test thats an added element replaces another with the same key
    @Test
    public void testReplacesValueWithSameKey() {
        map.put("Hello", 5);
        map.put("Hello", 6);

        assertEquals(6, map.get("Hello").intValue());
    }

    // Make sure that two (non-equal) keys with the same hash do not overwrite each other
    @Test
    public void testDoesNotOverwriteSeperateKeysWithSameHash() {
        map.put("Ea", 5);
        map.put("FB", 6);

        assertEquals(5, map.get("Ea").intValue());
        assertEquals(6, map.get("FB").intValue());
    }

    // Make sure size doesn't decrement below 0
    @Test
    public void testRemoveDoesNotEffectNewMap() {
        map.tryRemove("Hello");

        assertEquals(0, map.size());
    }

    // Make sure that size decrements as elements are used
    @Test
    public void testRemoveDecrementsSize() {
        map.put("Hello", 5);
        map.put("Goodbye", 6);

        map.tryRemove("Hello");

        assertEquals(1, map.size());

        map.removeUnchecked("Goodbye");

        assertEquals(0, map.size());
    }

    // Test elements are actually removed when remove is called
    @Test
    public void testRemoveDeletesElement() {
        map.put("Hello", 5);
        map.tryRemove("Hello");

        assertNull(map.get("Hello"));
    }

    // Test that contains is 'false' for new maps
    @Test
    public void testContainsKeyForNewMap() {
        assertFalse(map.containsKey("Hello"));
    }

    // Test that contains returns 'false' when key doesn't exist
    @Test
    public void testContainsKeyForNonExistingKey() {
        map.put("Hello", 5);
        assertFalse(map.containsKey("Goodbye"));
    }

    // Make sure that contains returns 'true' when the key does exist
    @Test
    public void testContainsKeyForExistingKey() {
        map.put("Hello", 5);
        assertTrue(map.containsKey("Hello"));
    }

    // Check that contains is not fooled by equivalent hash codes
    @Test
    public void testContainsKeyForKeyWithEquivalentHash() {
        map.put("Ea", 5);

        assertFalse(map.containsKey("FB"));
    }
}

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

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

/**
 * Original code comes from: https://gist.github.com/seadowg/1431727
 */
public class TestHashMap {
    private HashMap map;

    // Set up an empty map before each test
    @Before
    public void setUp() {
        this.map = new HashMap();
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
        assertEquals(5, map.get("Hello"));
        assertEquals(6, map.get("Goodbye"));
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

        assertEquals(6, map.get("Hello"));
    }

    // Make sure that two (non-equal) keys with the same hash do not overwrite each other
    @Test
    public void testDoesNotOverwriteSeperateKeysWithSameHash() {
        map.put("Ea", 5);
        map.put("FB", 6);

        assertEquals(5, map.get("Ea"));
        assertEquals(6, map.get("FB"));
    }

    // Make sure size doesn't decrement below 0
    @Test
    public void testRemoveDoesNotEffectNewMap() {
        map.remove("Hello");

        assertEquals(0, map.size());
    }

    // Make sure that size decrements as elements are used
    @Test
    public void testRemoveDecrementsSize() {
        map.put("Hello", 5);
        map.put("Goodbye", 6);

        map.remove("Hello");

        assertEquals(1, map.size());

        map.remove("Goodbye");

        assertEquals(0, map.size());
    }

    // Test elements are actually removed when remove is called
    @Test
    public void testRemoveDeletesElement() {
        map.put("Hello", 5);
        map.remove("Hello");

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
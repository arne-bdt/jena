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
package org.apache.jena.mem.txn.collection;

import org.apache.jena.graph.Node;
import org.junit.Test;

import static org.apache.jena.testing_framework.GraphHelper.node;
import static org.junit.Assert.*;

public class FastHashMapTest2 {

    @Test
    public void testConstructWithInitialSizeAndAdd() {
        var sut = new FastHashMapRevision<Node, Object>(3);
        sut.put(node("s"), null);
        sut.put(node("s1"), null);
        sut.put(node("s2"), null);
        sut.put(node("s3"), null);
        sut.put(node("s4"), null);
        assertEquals(5, sut.size());
    }

    @Test
    public void testGetValueAt() {
        var sut = new FastHashMapRevision<Node, Integer>();
        sut.put(node("s"), 0);
        sut.put(node("s1"), 1);
        sut.put(node("s2"), 2);

        assertEquals(0, (int) sut.getValueAt(0));
        assertEquals(1, (int) sut.getValueAt(1));
        assertEquals(2, (int) sut.getValueAt(2));
    }

    @Test
    public void testCopyConstructor() {
        var original = new FastHashMapRevision<Node, Integer>();
        original.put(node("s"), 0);
        original.put(node("s1"), 1);
        original.put(node("s2"), 2);
        assertEquals(3, original.size());

        var copy = new FastHashMapRevision<>(original, false);
        assertEquals(3, copy.size());
        assertEquals(0, (int) copy.get(node("s")));
        assertEquals(1, (int) copy.get(node("s1")));
        assertEquals(2, (int) copy.get(node("s2")));
    }

    @Test
    public void testCopyConstructorWithValueMapping() {
        var original = new FastHashMapRevision<Node, Integer>();
        original.put(node("s"), 0);
        original.put(node("s1"), 1);
        original.put(node("s2"), 2);
        assertEquals(3, original.size());

        var copy = new FastHashMapRevision<Node, Integer>(original, i -> (int) i + 1);
        assertEquals(3, copy.size());
        assertEquals(1, (int) copy.get(node("s")));
        assertEquals(2, (int) copy.get(node("s1")));
        assertEquals(3, (int) copy.get(node("s2")));

        assertEquals(0, (int) original.get(node("s")));
        assertEquals(1, (int) original.get(node("s1")));
        assertEquals(2, (int) original.get(node("s2")));
    }

    @Test
    public void testCopyConstructorAddAndDeleteHasNoSideEffects() {
        var original = new FastHashMapRevision<Node, Integer>();
        original.put(node("s"), 0);
        original.put(node("s1"), 1);
        original.put(node("s2"), 2);
        assertEquals(3, original.size());

        var copy = new FastHashMapRevision<Node, Integer>(original, false);
        copy.removeAndGetIndex(node("s1"));
        copy.put(node("s3"), 3);
        copy.put(node("s4"), 4);

        assertEquals(4, copy.size());
        assertEquals(0, (int) copy.get(node("s")));
        assertEquals(2, (int) copy.get(node("s2")));
        assertEquals(3, (int) copy.get(node("s3")));
        assertEquals(4, (int) copy.get(node("s4")));


        assertEquals(3, original.size());
        assertEquals(0, (int) original.get(node("s")));
        assertEquals(1, (int) original.get(node("s1")));
        assertEquals(2, (int) original.get(node("s2")));
    }

    @Test
    public void testRevisionsHaveNoSideEffects() {
        var original = new FastHashMapRevision<Node, Integer>();
        original.put(node("s"), 0);
        original.put(node("s1"), 1);
        original.put(node("s2"), 2);
        assertEquals(3, original.size());

        assertEquals(0, original.getRevisionNumber());

        assertEquals(3, original.size());
        assertEquals(0, (int) original.get(node("s")));
        assertEquals(1, (int) original.get(node("s1")));
        assertEquals(2, (int) original.get(node("s2")));

        var rev0 = original.getSnapshot(1);

        assertEquals(0, rev0.getRevisionNumber());
        assertEquals(1, original.getRevisionNumber());

        assertEquals(3, original.size());
        assertEquals(0, (int) original.get(node("s")));
        assertEquals(1, (int) original.get(node("s1")));
        assertEquals(2, (int) original.get(node("s2")));

        assertEquals(3, rev0.size());
        assertEquals(0, (int) rev0.get(node("s")));
        assertEquals(1, (int) rev0.get(node("s1")));
        assertEquals(2, (int) rev0.get(node("s2")));

        original.removeAndGetIndex(node("s2"));
        original.put(node("s5"), 5);
        original.put(node("s6"), 6);

        assertEquals(4, original.size());
        assertEquals(0, (int) original.get(node("s")));
        assertEquals(1, (int) original.get(node("s1")));
        assertEquals(5, (int) original.get(node("s5")));
        assertEquals(6, (int) original.get(node("s6")));

        assertEquals(3, rev0.size());
        assertEquals(0, (int) rev0.get(node("s")));
        assertEquals(1, (int) rev0.get(node("s1")));
        assertEquals(2, (int) rev0.get(node("s2")));
    }

    @Test
    public void testRevisionsHaveNoSideEffects1() {
        var original = new FastHashMapRevision<Node, Integer>();
        assertEquals(0, original.size());
        assertEquals(0, original.getRevisionNumber());

        var rev0 = original.getSnapshot(1);
        assertEquals(0, rev0.getRevisionNumber());
        assertEquals(1, original.getRevisionNumber());

        original.put(node("s1"), 1);

        assertEquals(1, original.size());
        assertEquals(0, rev0.size());

        assertTrue(original.containsKey(node("s1")));
        assertFalse(rev0.containsKey(node("s1")));
    }

    @Test
    public void testRevisionsHaveNoSideEffects2() {
        var original = new FastHashMapRevision<Node, Integer>();
        original.put(node("s1"), 1);

        var snapshot = original.getSnapshot(1);

        assertEquals(1, original.size());
        assertEquals(1, snapshot.size());

        assertEquals(Integer.valueOf(1), original.get(node("s1")));
        assertEquals(Integer.valueOf(1), snapshot.get(node("s1")));

        original.put(node("s1"), 2);

        assertEquals(Integer.valueOf(2), original.get(node("s1")));
        assertEquals(Integer.valueOf(1), snapshot.get(node("s1")));
    }



    @Test
    public void overrideSideEffects() {
        class KeyWithCollidingHash {

            public final String key;

            public KeyWithCollidingHash(String key) {
                this.key = key;
            }

            @Override
            public int hashCode() {
                return 42;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) return true;
                if (obj == null || getClass() != obj.getClass()) return false;
                KeyWithCollidingHash that = (KeyWithCollidingHash) obj;
                return key.equals(that.key);
            }
        }
        var sut = new FastHashMapRevision<KeyWithCollidingHash, Integer>(4);
        sut.put(new KeyWithCollidingHash("A"), 1);
        assertEquals(1, sut.size());
        assertEquals(Integer.valueOf(1), sut.get(new KeyWithCollidingHash("A")));

        sut.put(new KeyWithCollidingHash("A"), 11);
        assertEquals(1, sut.size());
        assertEquals(Integer.valueOf(11), sut.get(new KeyWithCollidingHash("A")));

        sut.put(new KeyWithCollidingHash("B"), 2);
        assertEquals(2, sut.size());
        assertEquals(Integer.valueOf(2), sut.get(new KeyWithCollidingHash("B")));

        sut.put(new KeyWithCollidingHash("A"), 111);
        assertEquals(2, sut.size());
        assertEquals(Integer.valueOf(111), sut.get(new KeyWithCollidingHash("A")));
        assertEquals(Integer.valueOf(2), sut.get(new KeyWithCollidingHash("B")));

        sut.put(new KeyWithCollidingHash("B"), 22);
        assertEquals(2, sut.size());
        assertEquals(Integer.valueOf(111), sut.get(new KeyWithCollidingHash("A")));
        assertEquals(Integer.valueOf(22), sut.get(new KeyWithCollidingHash("B")));
    }

    @Test
    public void overrideSideEffects2() {
        class KeyWithCollidingHash {

            public final String key;

            public KeyWithCollidingHash(String key) {
                this.key = key;
            }

            @Override
            public int hashCode() {
                return 42;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) return true;
                if (obj == null || getClass() != obj.getClass()) return false;
                KeyWithCollidingHash that = (KeyWithCollidingHash) obj;
                return key.equals(that.key);
            }
        }
        var sut = new FastHashMapRevision<KeyWithCollidingHash, Integer>(16);
        sut.put(new KeyWithCollidingHash("A"), 1);
        sut.put(new KeyWithCollidingHash("B"), 2);
        sut.put(new KeyWithCollidingHash("C"), 3);
        assertEquals(3, sut.size());
        assertEquals(Integer.valueOf(1), sut.get(new KeyWithCollidingHash("A")));
        assertEquals(Integer.valueOf(2), sut.get(new KeyWithCollidingHash("B")));
        assertEquals(Integer.valueOf(3), sut.get(new KeyWithCollidingHash("C")));

        sut.tryRemove(new KeyWithCollidingHash("A"));
        sut.tryRemove(new KeyWithCollidingHash("B"));
        sut.tryRemove(new KeyWithCollidingHash("C"));
        assertEquals(0, sut.size());
        assertFalse(sut.containsKey(new KeyWithCollidingHash("A")));
        assertFalse(sut.containsKey(new KeyWithCollidingHash("B")));
        assertFalse(sut.containsKey(new KeyWithCollidingHash("C")));

        sut.put(new KeyWithCollidingHash("B"), 2);
        sut.put(new KeyWithCollidingHash("C"), 3);
        assertEquals(2, sut.size());
        assertEquals(Integer.valueOf(2), sut.get(new KeyWithCollidingHash("B")));
        assertEquals(Integer.valueOf(3), sut.get(new KeyWithCollidingHash("C")));
    }


}
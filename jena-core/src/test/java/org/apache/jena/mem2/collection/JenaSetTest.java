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

package org.apache.jena.mem2.collection;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.store.fast.FastArrayBunch;
import org.apache.jena.mem2.store.fast.FastHashedTripleBunch;
import org.apache.jena.mem2.store.legacy.ArrayBunch;
import org.apache.jena.mem2.store.legacy.HashedTripleBunch;
import org.hamcrest.collection.IsEmptyCollection;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.jena.testing_framework.GraphHelper.triple;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class JenaSetTest {

    final Class<JenaSet<Triple>> setClass;
    JenaSet<Triple> sut;

    public JenaSetTest(String className, Class<JenaSet<Triple>> setClass) {
        this.setClass = setClass;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection setImplementations() {
        return Arrays.asList(new Object[][]{
                {HashCommonSet.class.getName(), HashCommonTripleSet.class},
                {FastHashSet.class.getName(), FastTripleHashSet.class},
                {ArrayBunch.class.getName(), ArrayBunch.class},
                {FastArrayBunch.class.getName(), FastArrayTripleBunch.class},
                {HashedTripleBunch.class.getName(), HashedTripleBunch.class},
                {FastHashedTripleBunch.class.getName(), FastHashedTripleBunch.class},
        });
    }

    @Before
    public void setUp() throws Exception {
        sut = setClass.newInstance();
    }

    @Test
    public void isEmpty() {
        assertTrue(sut.isEmpty());
        sut.tryAdd(triple("s o p"));
        assertFalse(sut.isEmpty());
    }

    @Test
    public void testTryAdd() {
        assertTrue(sut.tryAdd(triple("s o p")));
        assertFalse(sut.tryAdd(triple("s o p")));
        assertEquals(sut.size(), 1);
    }

    @Test
    public void testAddUnchecked() {
        sut.addUnchecked(triple("s o p"));
        assertEquals(sut.size(), 1);
    }

    @Test
    public void testTryRemove() {
        sut.tryAdd(triple("s o p"));
        assertTrue(sut.tryRemove(triple("s o p")));
        assertEquals(sut.size(), 0);
        assertFalse(sut.tryRemove(triple("s o p")));
    }

    @Test
    public void testRemoveUnchecked() {
        sut.tryAdd(triple("s o p"));
        sut.removeUnchecked(triple("s o p"));
        assertEquals(sut.size(), 0);
    }

    @Test
    public void testClear() {
        sut.tryAdd(triple("s o p"));
        sut.clear();
        assertEquals(sut.size(), 0);
    }

    @Test
    public void testContainKey() {
        assertFalse(sut.containsKey(triple("s o p")));
        sut.tryAdd(triple("s o p"));
        assertTrue(sut.containsKey(triple("s o p")));
        assertFalse(sut.containsKey(triple("s o p2")));
    }

    @Test(expected = NoSuchElementException.class)
    public void testKeyIteratorEmpty() {
        var iter = sut.keyIterator();
        assertFalse(iter.hasNext());
        iter.next(); // throws NoSuchElementException
    }

    @Test
    public void testKeyIterator1() {
        final var t0 = triple("s o p");

        sut.tryAdd(t0);

        final var iter = sut.keyIterator();
        assertThat(iter.toList(), IsIterableContainingInAnyOrder.containsInAnyOrder(t0));
    }

    @Test
    public void testKeyIterator2() {
        final var t0 = triple("s o p");
        final var t1 = triple("s o p2");

        sut.tryAdd(t0);
        sut.tryAdd(t1);

        final var iter = sut.keyIterator();
        assertThat(iter.toList(), IsIterableContainingInAnyOrder.containsInAnyOrder(t0, t1));
    }

    @Test
    public void testKeyIterator3() {
        final var t0 = triple("s o p");
        final var t1 = triple("s o p2");
        final var t2 = triple("s o p3");

        sut.tryAdd(t0);
        sut.tryAdd(t1);
        sut.tryAdd(t2);

        final var iter = sut.keyIterator();
        assertThat(iter.toList(), IsIterableContainingInAnyOrder.containsInAnyOrder(t0, t1, t2));
    }

    @Test
    public void testKeySpliteratorEmpty() {
        var spliterator = sut.keySpliterator();
        assertFalse(spliterator.tryAdvance(t -> fail()));
    }

    @Test
    public void testKeySpliterator1() {
        final var t0 = triple("s o p");

        sut.tryAdd(t0);

        final var spliterator = sut.keySpliterator();
        final var list = StreamSupport.stream(spliterator, false).collect(Collectors.toList());
        assertThat(list, IsIterableContainingInAnyOrder.containsInAnyOrder(t0));
    }

    @Test
    public void testKeySpliterator2() {
        final var t0 = triple("s o p");
        final var t1 = triple("s o p2");

        sut.tryAdd(t0);
        sut.tryAdd(t1);

        final var spliterator = sut.keySpliterator();
        final var list = StreamSupport.stream(spliterator, false).collect(Collectors.toList());
        assertThat(list, IsIterableContainingInAnyOrder.containsInAnyOrder(t0, t1));
    }

    @Test
    public void testKeySpliterator3() {
        final var t0 = triple("s o p");
        final var t1 = triple("s o p2");
        final var t2 = triple("s o p3");

        sut.tryAdd(t0);
        sut.tryAdd(t1);
        sut.tryAdd(t2);

        final var spliterator = sut.keySpliterator();
        final var list = StreamSupport.stream(spliterator, false).collect(Collectors.toList());
        assertThat(list, IsIterableContainingInAnyOrder.containsInAnyOrder(t0, t1, t2));
    }

    @Test
    public void testKeyStreamEmpty() {
        var stream = sut.keyStream();
        assertThat(stream.collect(Collectors.toList()), IsEmptyCollection.empty());
    }

    @Test
    public void testKeyStream1() {
        final var t0 = triple("s o p");

        sut.tryAdd(t0);

        final var stream = sut.keyStream();
        assertThat(stream.collect(Collectors.toList()), IsIterableContainingInAnyOrder.containsInAnyOrder(t0));
    }

    @Test
    public void testKeyStream2() {
        final var t0 = triple("s o p");
        final var t1 = triple("s o p2");

        sut.tryAdd(t0);
        sut.tryAdd(t1);

        final var stream = sut.keyStream();
        assertThat(stream.collect(Collectors.toList()), IsIterableContainingInAnyOrder.containsInAnyOrder(t0, t1));
    }

    @Test
    public void testKeyStream3() {
        final var t0 = triple("s o p");
        final var t1 = triple("s o p2");
        final var t2 = triple("s o p3");

        sut.tryAdd(t0);
        sut.tryAdd(t1);
        sut.tryAdd(t2);

        final var stream = sut.keyStream();
        assertThat(stream.collect(Collectors.toList()), IsIterableContainingInAnyOrder.containsInAnyOrder(t0, t1, t2));
    }

    @Test
    public void testKeyStreamParallelEmpty() {
        var stream = sut.keyStreamParallel();
        assertThat(stream.collect(Collectors.toList()), IsEmptyCollection.empty());
    }

    @Test
    public void testKeyStreamParallel1() {
        final var t0 = triple("s o p");

        sut.tryAdd(t0);

        final var stream = sut.keyStreamParallel();
        assertThat(stream.collect(Collectors.toList()), IsIterableContainingInAnyOrder.containsInAnyOrder(t0));
    }

    @Test
    public void testKeyStreamParallel2() {
        final var t0 = triple("s o p");
        final var t1 = triple("s o p2");

        sut.tryAdd(t0);
        sut.tryAdd(t1);

        final var stream = sut.keyStreamParallel();
        assertThat(stream.collect(Collectors.toList()), IsIterableContainingInAnyOrder.containsInAnyOrder(t0, t1));
    }

    @Test
    public void testKeyStreamParallel3() {
        final var t0 = triple("s o p");
        final var t1 = triple("s o p2");
        final var t2 = triple("s o p3");

        sut.tryAdd(t0);
        sut.tryAdd(t1);
        sut.tryAdd(t2);

        final var stream = sut.keyStreamParallel();
        assertThat(stream.collect(Collectors.toList()), IsIterableContainingInAnyOrder.containsInAnyOrder(t0, t1, t2));
    }

    @Test
    public void testSize() {
        assertEquals(0, sut.size());
        sut.tryAdd(triple("s o p"));
        assertEquals(1, sut.size());
        sut.tryAdd(triple("s o p2"));
        assertEquals(2, sut.size());
        sut.tryAdd(triple("s o p3"));
        assertEquals(3, sut.size());
    }

    @Test
    public void testAnyMatch() {
        assertFalse(sut.anyMatch(t -> true));
        assertFalse(sut.anyMatch(t -> false));

        sut.tryAdd(triple("s o p"));
        assertTrue(sut.anyMatch(t -> true));
        assertFalse(sut.anyMatch(t -> false));
    }

    @Test
    public void testAnyMatchIsCalledForEveryElement() {
        final var t0 = triple("s o p");
        final var t1 = triple("s o p2");
        final var t2 = triple("s o p3");

        sut.tryAdd(t0);
        sut.tryAdd(t1);
        sut.tryAdd(t2);

        final var list = new ArrayList<Triple>();
        sut.anyMatch(t -> {
            list.add(t);
            return false;
        });
        assertThat(list, IsIterableContainingInAnyOrder.containsInAnyOrder(t0, t1, t2));
    }

    @Test
    public void add1000Triples() {
        for (int i = 0; i < 1000; i++) {
            sut.tryAdd(triple("s o " + i));
        }
        assertEquals(1000, sut.size());
    }

    @Test
    public void addAndRemove1000Triples() {
        final var triples = new ArrayList<Triple>();
        for (int i = 0; i < 1000; i++) {
            final var t = triple("s o " + i);
            sut.tryAdd(t);
            triples.add(t);
        }
        assertEquals(1000, sut.size());
        Collections.shuffle(triples);
        for (final var t : triples) {
            assertTrue(sut.tryRemove(t));
        }
        assertTrue(sut.isEmpty());
    }


    private static class HashCommonTripleSet extends HashCommonSet<Triple> {
        public HashCommonTripleSet() {
            super(10);
        }

        @Override
        protected Triple[] newKeysArray(int size) {
            return new Triple[size];
        }

        @Override
        public void clear() {
            super.clear(10);
        }
    }

    private static class FastTripleHashSet extends FastHashSet<Triple> {
        @Override
        protected Triple[] newKeysArray(int size) {
            return new Triple[size];
        }
    }

    private static class FastArrayTripleBunch extends FastArrayBunch {
        @Override
        public boolean areEqual(Triple a, Triple b) {
            return a.equals(b);
        }
    }


}
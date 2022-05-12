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

package org.apache.jena.mem.hash_no_entry;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.hash_no_entry.ArrayValueMap;
import org.apache.jena.testing_framework.NodeCreateUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;


public class ArrayValueMapTest {

    @Test
    public void testAddIfNotExists() {
        var sut = ArrayValueMap.forTriples.get();
        Assert.assertEquals(0, sut.size());

        var t1 = NodeCreateUtils.createTriple("A x 4711");
        var result = sut.addIfNotExists(t1);
        Assert.assertNotNull(result);
        Assert.assertEquals(1, sut.size());

        var t2 = NodeCreateUtils.createTriple("A x 4711");
        result = sut.addIfNotExists(t2);
        Assert.assertFalse(result);
        Assert.assertEquals(1, sut.size());
    }

    @Test
    public void testAddDefinitetly() {
        var sut = ArrayValueMap.forTriples.get();
        var t1 = NodeCreateUtils.createTriple("A x 4711");
        var t2 = NodeCreateUtils.createTriple("A x 4711");
        sut.addDefinitetly(t1);
        Assert.assertEquals(1, sut.size());
        sut.addDefinitetly(t2);
        Assert.assertEquals(2, sut.size());
    }

    @Test
    public void testRemoveIfExits() {
        var sut = ArrayValueMap.forTriples.get();
        var t1 = NodeCreateUtils.createTriple("A x 4711");
        sut.addIfNotExists(t1);
        Assert.assertEquals(1, sut.size());

        var result = sut.removeIfExits(t1);
        Assert.assertNotNull(result);
        Assert.assertEquals(0, sut.size());
    }

    @Test
    public void testRemoveExisting() {
        var sut = ArrayValueMap.forTriples.get();
        var t1 = NodeCreateUtils.createTriple("A x 4711");
        var t2 = NodeCreateUtils.createTriple("A x 4711");
        Assert.assertTrue(sut.addIfNotExists(t1));
        sut.addDefinitetly(t2);
        Assert.assertEquals(2, sut.size());

        sut.removeExisting(t1);
        Assert.assertEquals(1, sut.size());
        sut.removeExisting(t2);
        Assert.assertEquals(0, sut.size());
    }

    @Test
    public void testContains() {
        var sut = ArrayValueMap.forTriples.get();

        var t1 = NodeCreateUtils.createTriple("A x 4711");
        var t2 = NodeCreateUtils.createTriple("B y 4712");
        sut.addIfNotExists(t1);

        Assert.assertTrue(sut.contains(t1));
        Assert.assertFalse(sut.contains(t2));
    }

    @Test
    public void testIsEmpty() {
        var sut = ArrayValueMap.forTriples.get();
        Assert.assertTrue(sut.isEmpty());

        var t1 = NodeCreateUtils.createTriple("A x 4711");
        sut.addDefinitetly(t1);
        Assert.assertFalse(sut.isEmpty());

        sut.removeIfExits(t1);
        Assert.assertTrue(sut.isEmpty());
    }

    @Test
    public void testStream() {
        var sut = ArrayValueMap.forTriples.get();
        Assert.assertTrue(sut.isEmpty());

        var t1 = NodeCreateUtils.createTriple("A x 4711");
        var t2 = NodeCreateUtils.createTriple("A x 4712");
        var t3 = NodeCreateUtils.createTriple("A y 4711");
        var t4 = NodeCreateUtils.createTriple("B x 4711");
        var t5 = NodeCreateUtils.createTriple("B x 4712");
        var t6 = NodeCreateUtils.createTriple("B y 4711");
        var t7 = NodeCreateUtils.createTriple("C x 4711");
        var t8 = NodeCreateUtils.createTriple("C x 4712");
        var t9 = NodeCreateUtils.createTriple("C y 4711");
        var list = Arrays.asList(t1, t2, t3, t4, t5, t6, t7, t8, t9);
        list.forEach(sut::addIfNotExists);

        var streamdTriples = sut.stream().collect(Collectors.toList());
        assertThat(streamdTriples, containsInAnyOrder(t1, t2, t3, t4, t5, t6, t7, t8, t9));
    }

    @Test
    public void testStreamWithTripleMatch() {
        var sut = ArrayValueMap.forTriples.get();
        Assert.assertTrue(sut.isEmpty());

        var t1 = NodeCreateUtils.createTriple("A x 4711");
        var t2 = NodeCreateUtils.createTriple("A x 4712");
        var t3 = NodeCreateUtils.createTriple("A y 4711");
        var t4 = NodeCreateUtils.createTriple("B x 4711");
        var t5 = NodeCreateUtils.createTriple("B x 4712");
        var t6 = NodeCreateUtils.createTriple("B y 4711");
        var t7 = NodeCreateUtils.createTriple("C x 4711");
        var t8 = NodeCreateUtils.createTriple("C x 4712");
        var t9 = NodeCreateUtils.createTriple("C y 4711");
        var list = Arrays.asList(t1, t2, t3, t4, t5, t6, t7, t8, t9);
        list.forEach(sut::addIfNotExists);

        var streamdTriples = sut.stream(new Triple(t4.getSubject(), Node.ANY, Node.ANY))
                .collect(Collectors.toList());
        /*this implementation does not filter*/
        assertThat(streamdTriples, containsInAnyOrder(t1, t2, t3, t4, t5, t6, t7, t8, t9));
    }

    @Test
    public void testIterator() {
        var sut = ArrayValueMap.forTriples.get();
        Assert.assertTrue(sut.isEmpty());

        var t1 = NodeCreateUtils.createTriple("A x 4711");
        var t2 = NodeCreateUtils.createTriple("A x 4712");
        var t3 = NodeCreateUtils.createTriple("A y 4711");
        var t4 = NodeCreateUtils.createTriple("B x 4711");
        var t5 = NodeCreateUtils.createTriple("B x 4712");
        var t6 = NodeCreateUtils.createTriple("B y 4711");
        var t7 = NodeCreateUtils.createTriple("C x 4711");
        var t8 = NodeCreateUtils.createTriple("C x 4712");
        var t9 = NodeCreateUtils.createTriple("C y 4711");
        var list = Arrays.asList(t1, t2, t3, t4, t5, t6, t7, t8, t9);
        list.forEach(sut::addIfNotExists);

        var iterator = sut.iterator();
        var triplesFound = new ArrayList<>();
        iterator.forEachRemaining(triplesFound::add);

        assertThat(triplesFound, containsInAnyOrder(t1, t2, t3, t4, t5, t6, t7, t8, t9));
    }

    @Test
    public void testIteratorWithTripleMatch() {
        var sut = ArrayValueMap.forTriples.get();
        Assert.assertTrue(sut.isEmpty());

        var t1 = NodeCreateUtils.createTriple("A x 4711");
        var t2 = NodeCreateUtils.createTriple("A x 4712");
        var t3 = NodeCreateUtils.createTriple("A y 4711");
        var t4 = NodeCreateUtils.createTriple("B x 4711");
        var t5 = NodeCreateUtils.createTriple("B x 4712");
        var t6 = NodeCreateUtils.createTriple("B y 4711");
        var t7 = NodeCreateUtils.createTriple("C x 4711");
        var t8 = NodeCreateUtils.createTriple("C x 4712");
        var t9 = NodeCreateUtils.createTriple("C y 4711");
        var list = Arrays.asList(t1, t2, t3, t4, t5, t6, t7, t8, t9);
        list.forEach(sut::addIfNotExists);

        var iterator = sut.iterator(new Triple(t4.getSubject(), Node.ANY, Node.ANY));
        var triplesFound = new ArrayList<>();
        iterator.forEachRemaining(triplesFound::add);

        /*this implementation does not filter*/
        assertThat(triplesFound, containsInAnyOrder(t1, t2, t3, t4, t5, t6, t7, t8, t9));
    }
}
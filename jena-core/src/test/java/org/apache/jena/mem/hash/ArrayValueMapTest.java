package org.apache.jena.mem.hash;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
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
        var sut = ArrayValueMap.forSubject.get();
        Assert.assertEquals(0, sut.size());

        var t1 = NodeCreateUtils.createTriple("A x 4711");
        var result = sut.addIfNotExists(t1);
        Assert.assertNotNull(result);
        Assert.assertEquals(1, sut.size());

        var t2 = NodeCreateUtils.createTriple("A x 4711");
        result = sut.addIfNotExists(t1);
        Assert.assertNull(result);
        Assert.assertEquals(1, sut.size());
    }

    @Test
    public void testAddDefinitetly() {
        var sut = ArrayValueMap.forSubject.get();
        var t1 = NodeCreateUtils.createTriple("A x 4711");
        var t2 = NodeCreateUtils.createTriple("A x 4711");
        sut.addDefinitetly(t1);
        Assert.assertEquals(1, sut.size());
        sut.addDefinitetly(t2);
        Assert.assertEquals(2, sut.size());
    }

    @Test
    public void testRemoveIfExits() {
        var sut = ArrayValueMap.forSubject.get();
        var t1 = NodeCreateUtils.createTriple("A x 4711");
        sut.addIfNotExists(t1);
        Assert.assertEquals(1, sut.size());

        var result = sut.removeIfExits(t1);
        Assert.assertNotNull(result);
        Assert.assertEquals(0, sut.size());
    }

    @Test
    public void testRemoveExisting() {
        var sut = ArrayValueMap.forSubject.get();
        var t1 = NodeCreateUtils.createTriple("A x 4711");
        var t2 = NodeCreateUtils.createTriple("A x 4711");
        var e1 = sut.addIfNotExists(t1);
        var e2 = sut.addDefinitetly(t2);
        Assert.assertEquals(2, sut.size());

        sut.removeExisting(e1);
        Assert.assertEquals(1, sut.size());
        sut.removeExisting(e2);
        Assert.assertEquals(0, sut.size());
    }

    @Test
    public void testContains() {
        var sut = ArrayValueMap.forSubject.get();

        var t1 = NodeCreateUtils.createTriple("A x 4711");
        var e1 = sut.addIfNotExists(t1);

        var t2 = NodeCreateUtils.createTriple("B y 4712");

        Assert.assertTrue(sut.contains(t1));
        Assert.assertTrue(sut.contains(e1));
        Assert.assertFalse(sut.contains(t2));
    }

    @Test
    public void testIsEmpty() {
        var sut = ArrayValueMap.forSubject.get();
        Assert.assertTrue(sut.isEmpty());

        var t1 = NodeCreateUtils.createTriple("A x 4711");
        sut.addDefinitetly(t1);
        Assert.assertFalse(sut.isEmpty());

        sut.removeIfExits(t1);
        Assert.assertTrue(sut.isEmpty());
    }

    @Test
    public void testStream() {
        var sut = ArrayValueMap.forSubject.get();
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
        var sut = ArrayValueMap.forSubject.get();
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
        var sut = ArrayValueMap.forSubject.get();
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
        var sut = ArrayValueMap.forSubject.get();
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
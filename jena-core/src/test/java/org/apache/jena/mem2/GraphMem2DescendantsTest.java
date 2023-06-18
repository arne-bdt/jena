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

package org.apache.jena.mem2;

import org.apache.jena.datatypes.xsd.impl.XSDDouble;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import static org.apache.jena.testing_framework.GraphHelper.node;
import static org.apache.jena.testing_framework.GraphHelper.triple;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class GraphMem2DescendantsTest {

    final Class<Graph> graphClass;
    Graph sut;

    public GraphMem2DescendantsTest(String className, Class<Graph> graphClass) {
        this.graphClass = graphClass;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection setImplementations() {
        return Arrays.asList(new Object[][]{
                {GraphMem2Fast.class.getName(), GraphMem2Fast.class},
                {GraphMem2Legacy.class.getName(), GraphMem2Legacy.class},
                {GraphMem2Roaring.class.getName(), GraphMem2Roaring.class}
        });
    }

    @Before
    public void setUp() throws Exception {
        sut = graphClass.getDeclaredConstructor().newInstance();
    }

    @Test
    public void testClear() {
        sut.add(triple("x R y"));
        assertEquals(1, sut.size());
        sut.clear();
        assertEquals(0, sut.size());
        assertTrue(sut.isEmpty());
    }


    @Test
    public void testDelete() {
        sut.add(triple("x R y"));
        assertEquals(1, sut.size());
        sut.delete(triple("x R y"));
        assertEquals(0, sut.size());
        assertTrue(sut.isEmpty());
    }

    @Test
    public void testFind() {
        sut.add(triple("x R y"));
        assertEquals(1, sut.find(triple("x R y")).toList().size());
        assertEquals(0, sut.find(triple("x R z")).toList().size());
    }

    @Test
    public void testFind1() {
        sut.add(triple("x R y"));
        assertEquals(1, sut.find(null, null, null).toList().size());
        assertEquals(1, sut.find(null, null, node("y")).toList().size());
        assertEquals(1, sut.find(null, node("R"), null).toList().size());
        assertEquals(1, sut.find(null, node("R"), node("y")).toList().size());
        assertEquals(1, sut.find(node("x"), null, null).toList().size());
        assertEquals(1, sut.find(node("x"), null, node("y")).toList().size());
        assertEquals(1, sut.find(node("x"), node("R"), null).toList().size());
        assertEquals(1, sut.find(node("x"), node("R"), node("y")).toList().size());
    }

    @Test
    public void testFind2() {
        sut.add(triple("x R y"));
        assertEquals(1, sut.find(null, null, null).toList().size());
        assertEquals(0, sut.find(null, null, node("z")).toList().size());
        assertEquals(0, sut.find(null, node("S"), null).toList().size());
        assertEquals(0, sut.find(null, node("S"), node("y")).toList().size());
        assertEquals(0, sut.find(node("y"), null, null).toList().size());
        assertEquals(0, sut.find(node("y"), null, node("y")).toList().size());
        assertEquals(0, sut.find(node("y"), node("R"), null).toList().size());
        assertEquals(0, sut.find(node("y"), node("R"), node("y")).toList().size());
    }

    @Test
    public void testFindWithIteratorHasNextNext() {
        sut.add(triple("x R y"));
        var iter = sut.find(triple("x R y"));
        assertTrue(iter.hasNext());
        assertEquals(triple("x R y"), iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void testFindSPO() {
        sut.add(triple("x R y"));
        assertEquals(1, sut.find(node("x"), node("R"), node("y")).toList().size());
        assertEquals(0, sut.find(node("x"), node("R"), node("z")).toList().size());
    }

    @Test
    public void testFind___() {
        final var aAa = triple("a A a");
        final var aAb = triple("a A b");
        final var aAc = triple("a A c");
        final var bAa = triple("b A a");
        final var bAb = triple("b A b");
        final var bAc = triple("b A c");
        final var cBa = triple("c B a");
        final var cBb = triple("c B b");
        final var cBc = triple("c B c");

        sut.add(aAa);
        sut.add(aAb);
        sut.add(aAc);
        sut.add(bAa);
        sut.add(bAb);
        sut.add(bAc);
        sut.add(cBa);
        sut.add(cBb);
        sut.add(cBc);

        var findings = sut.find(null, null, null).toList();
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(aAa, aAb, aAc, bAa, bAb, bAc, cBa, cBb, cBc));
    }

    @Test
    public void testFindS__() {
        final var aAa = triple("a A a");
        final var aAb = triple("a A b");
        final var aAc = triple("a A c");
        final var bAa = triple("b A a");
        final var bAb = triple("b A b");
        final var bAc = triple("b A c");
        final var cBa = triple("c B a");
        final var cBb = triple("c B b");
        final var cBc = triple("c B c");

        sut.add(aAa);
        sut.add(aAb);
        sut.add(aAc);
        sut.add(bAa);
        sut.add(bAb);
        sut.add(bAc);
        sut.add(cBa);
        sut.add(cBb);
        sut.add(cBc);

        var findings = sut.find(node("a"), null, null).toList();
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(aAa, aAb, aAc));

        findings = sut.find(node("b"), null, null).toList();
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(bAa, bAb, bAc));

        findings = sut.find(node("c"), null, null).toList();
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(cBa, cBb, cBc));
    }

    @Test
    public void testFindP__() {
        final var aAa = triple("a A a");
        final var aAb = triple("a A b");
        final var aAc = triple("a A c");
        final var bAa = triple("b A a");
        final var bAb = triple("b A b");
        final var bAc = triple("b A c");
        final var cBa = triple("c B a");
        final var cBb = triple("c B b");
        final var cBc = triple("c B c");

        sut.add(aAa);
        sut.add(aAb);
        sut.add(aAc);
        sut.add(bAa);
        sut.add(bAb);
        sut.add(bAc);
        sut.add(cBa);
        sut.add(cBb);
        sut.add(cBc);

        var findings = sut.find(null, node("A"), null).toList();
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(aAa, aAb, aAc, bAa, bAb, bAc));

        findings = sut.find(null, node("B"), null).toList();
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(cBa, cBb, cBc));
    }

    @Test
    public void testFind__P() {
        final var aAa = triple("a A a");
        final var aAb = triple("a A b");
        final var aAc = triple("a A c");
        final var bAa = triple("b A a");
        final var bAb = triple("b A b");
        final var bAc = triple("b A c");
        final var cBa = triple("c B a");
        final var cBb = triple("c B b");
        final var cBc = triple("c B c");

        sut.add(aAa);
        sut.add(aAb);
        sut.add(aAc);
        sut.add(bAa);
        sut.add(bAb);
        sut.add(bAc);
        sut.add(cBa);
        sut.add(cBb);
        sut.add(cBc);

        var findings = sut.find(null, null, node("a")).toList();
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(aAa, bAa, cBa));

        findings = sut.find(null, null, node("b")).toList();
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(aAb, bAb, cBb));

        findings = sut.find(null, null, node("c")).toList();
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(aAc, bAc, cBc));
    }

    @Test
    public void testFindSP_() {
        final var aAa = triple("a A a");
        final var aAb = triple("a A b");
        final var aAc = triple("a A c");
        final var bAa = triple("b A a");
        final var bAb = triple("b A b");
        final var bAc = triple("b A c");
        final var cBa = triple("c B a");
        final var cBb = triple("c B b");
        final var cBc = triple("c B c");

        sut.add(aAa);
        sut.add(aAb);
        sut.add(aAc);
        sut.add(bAa);
        sut.add(bAb);
        sut.add(bAc);
        sut.add(cBa);
        sut.add(cBb);
        sut.add(cBc);

        var findings = sut.find(node("a"), node("A"), null).toList();
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(aAa, aAb, aAc));

        findings = sut.find(node("b"), node("A"), null).toList();
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(bAa, bAb, bAc));

        findings = sut.find(node("c"), node("B"), null).toList();
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(cBa, cBb, cBc));
    }

    @Test
    public void testFindS_O() {
        final var aAa = triple("a A a");
        final var aAb = triple("a A b");
        final var aAc = triple("a A c");
        final var bAa = triple("b A a");
        final var bAb = triple("b A b");
        final var bAc = triple("b A c");
        final var cBa = triple("c B a");
        final var cBb = triple("c B b");
        final var cBc = triple("c B c");

        sut.add(aAa);
        sut.add(aAb);
        sut.add(aAc);
        sut.add(bAa);
        sut.add(bAb);
        sut.add(bAc);
        sut.add(cBa);
        sut.add(cBb);
        sut.add(cBc);

        var findings = sut.find(node("a"), null, node("a")).toList();
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(aAa));

        findings = sut.find(node("b"), null, node("a")).toList();
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(bAa));

        findings = sut.find(node("c"), null, node("a")).toList();
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(cBa));
    }

    @Test
    public void testFind_OP() {
        final var aAa = triple("a A a");
        final var aAb = triple("a A b");
        final var aAc = triple("a A c");
        final var bAa = triple("b A a");
        final var bAb = triple("b A b");
        final var bAc = triple("b A c");
        final var cBa = triple("c B a");
        final var cBb = triple("c B b");
        final var cBc = triple("c B c");

        sut.add(aAa);
        sut.add(aAb);
        sut.add(aAc);
        sut.add(bAa);
        sut.add(bAb);
        sut.add(bAc);
        sut.add(cBa);
        sut.add(cBb);
        sut.add(cBc);

        var findings = sut.find(null, node("A"), node("a")).toList();
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(aAa, bAa));

        findings = sut.find(null, node("B"), node("a")).toList();
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(cBa));
    }

    @Test
    public void testStream() {
        sut.add(triple("x R y"));
        assertEquals(1, sut.stream().count());
    }

    @Test
    public void testStreamEmpty() {
        assertEquals(0, sut.stream().count());
    }

    @Test
    public void testStreamSPO() {
        var t = triple("x R y");
        sut.add(t);
        var findings = sut.stream(t.getSubject(), t.getPredicate(), t.getObject()).collect(Collectors.toList());
        assertEquals(1, findings.size());
        assertEquals(findings.get(0), t);
    }

    @Test
    public void testStream___() {
        final var aAa = triple("a A a");
        final var aAb = triple("a A b");
        final var aAc = triple("a A c");
        final var bAa = triple("b A a");
        final var bAb = triple("b A b");
        final var bAc = triple("b A c");
        final var cBa = triple("c B a");
        final var cBb = triple("c B b");
        final var cBc = triple("c B c");

        sut.add(aAa);
        sut.add(aAb);
        sut.add(aAc);
        sut.add(bAa);
        sut.add(bAb);
        sut.add(bAc);
        sut.add(cBa);
        sut.add(cBb);
        sut.add(cBc);

        var findings = sut.stream(null, null, null).collect(Collectors.toList());
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(aAa, aAb, aAc, bAa, bAb, bAc, cBa, cBb, cBc));
    }

    @Test
    public void testStreamS__() {
        final var aAa = triple("a A a");
        final var aAb = triple("a A b");
        final var aAc = triple("a A c");
        final var bAa = triple("b A a");
        final var bAb = triple("b A b");
        final var bAc = triple("b A c");
        final var cBa = triple("c B a");
        final var cBb = triple("c B b");
        final var cBc = triple("c B c");

        sut.add(aAa);
        sut.add(aAb);
        sut.add(aAc);
        sut.add(bAa);
        sut.add(bAb);
        sut.add(bAc);
        sut.add(cBa);
        sut.add(cBb);
        sut.add(cBc);

        var findings = sut.stream(aAa.getSubject(), null, null).collect(Collectors.toList());
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(aAa, aAb, aAc));

        findings = sut.stream(bAa.getSubject(), null, null).collect(Collectors.toList());
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(bAa, bAb, bAc));

        findings = sut.stream(cBa.getSubject(), null, null).collect(Collectors.toList());
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(cBa, cBb, cBc));
    }

    @Test
    public void testStream_P_() {
        final var aAa = triple("a A a");
        final var aAb = triple("a A b");
        final var aAc = triple("a A c");
        final var bAa = triple("b A a");
        final var bAb = triple("b A b");
        final var bAc = triple("b A c");
        final var cBa = triple("c B a");
        final var cBb = triple("c B b");
        final var cBc = triple("c B c");

        sut.add(aAa);
        sut.add(aAb);
        sut.add(aAc);
        sut.add(bAa);
        sut.add(bAb);
        sut.add(bAc);
        sut.add(cBa);
        sut.add(cBb);
        sut.add(cBc);

        var findings = sut.stream(null, aAa.getPredicate(), null).collect(Collectors.toList());
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(aAa, aAb, aAc, bAa, bAb, bAc));

        findings = sut.stream(null, cBa.getPredicate(), null).collect(Collectors.toList());
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(cBa, cBb, cBc));
    }

    @Test
    public void testStream__O() {
        final var aAa = triple("a A a");
        final var aAb = triple("a A b");
        final var aAc = triple("a A c");
        final var bAa = triple("b A a");
        final var bAb = triple("b A b");
        final var bAc = triple("b A c");
        final var cBa = triple("c B a");
        final var cBb = triple("c B b");
        final var cBc = triple("c B c");

        sut.add(aAa);
        sut.add(aAb);
        sut.add(aAc);
        sut.add(bAa);
        sut.add(bAb);
        sut.add(bAc);
        sut.add(cBa);
        sut.add(cBb);
        sut.add(cBc);

        var findings = sut.stream(null, null, aAa.getObject()).collect(Collectors.toList());
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(aAa, bAa, cBa));

        findings = sut.stream(null, null, aAb.getObject()).collect(Collectors.toList());
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(aAb, bAb, cBb));

        findings = sut.stream(null, null, aAc.getObject()).collect(Collectors.toList());
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(aAc, bAc, cBc));
    }

    @Test
    public void testStreamSP_() {
        final var aAa = triple("a A a");
        final var aAb = triple("a A b");
        final var aAc = triple("a A c");
        final var bAa = triple("b A a");
        final var bAb = triple("b A b");
        final var bAc = triple("b A c");
        final var cBa = triple("c B a");
        final var cBb = triple("c B b");
        final var cBc = triple("c B c");

        sut.add(aAa);
        sut.add(aAb);
        sut.add(aAc);
        sut.add(bAa);
        sut.add(bAb);
        sut.add(bAc);
        sut.add(cBa);
        sut.add(cBb);
        sut.add(cBc);

        var findings = sut.stream(aAa.getSubject(), aAa.getPredicate(), null).collect(Collectors.toList());
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(aAa, aAb, aAc));

        findings = sut.stream(bAa.getSubject(), bAa.getPredicate(), null).collect(Collectors.toList());
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(bAa, bAb, bAc));

        findings = sut.stream(cBa.getSubject(), cBa.getPredicate(), null).collect(Collectors.toList());
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(cBa, cBb, cBc));
    }

    @Test
    public void testStreamS_O() {
        final var aAa = triple("a A a");
        final var aAb = triple("a A b");
        final var aAc = triple("a A c");
        final var bAa = triple("b A a");
        final var bAb = triple("b A b");
        final var bAc = triple("b A c");
        final var cBa = triple("c B a");
        final var cBb = triple("c B b");
        final var cBc = triple("c B c");

        sut.add(aAa);
        sut.add(aAb);
        sut.add(aAc);
        sut.add(bAa);
        sut.add(bAb);
        sut.add(bAc);
        sut.add(cBa);
        sut.add(cBb);
        sut.add(cBc);

        var findings = sut.stream(aAa.getSubject(), null, aAa.getObject()).collect(Collectors.toList());
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(aAa));

        findings = sut.stream(bAa.getSubject(), null, bAa.getObject()).collect(Collectors.toList());
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(bAa));

        findings = sut.stream(cBa.getSubject(), null, cBa.getObject()).collect(Collectors.toList());
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(cBa));
    }

    @Test
    public void testStream_PO() {
        final var aAa = triple("a A a");
        final var aAb = triple("a A b");
        final var aAc = triple("a A c");
        final var bAa = triple("b A a");
        final var bAb = triple("b A b");
        final var bAc = triple("b A c");
        final var cBa = triple("c B a");
        final var cBb = triple("c B b");
        final var cBc = triple("c B c");

        sut.add(aAa);
        sut.add(aAb);
        sut.add(aAc);
        sut.add(bAa);
        sut.add(bAb);
        sut.add(bAc);
        sut.add(cBa);
        sut.add(cBb);
        sut.add(cBc);

        var findings = sut.stream(null, aAa.getPredicate(), aAa.getObject()).collect(Collectors.toList());
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(aAa, bAa));

        findings = sut.stream(null, bAa.getPredicate(), bAa.getObject()).collect(Collectors.toList());
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(aAa, bAa));

        findings = sut.stream(null, cBa.getPredicate(), cBa.getObject()).collect(Collectors.toList());
        assertThat(findings, IsIterableContainingInAnyOrder.containsInAnyOrder(cBa));
    }

    @Test
    public void testContains() {
        sut.add(triple("x R y"));
        assertTrue(sut.contains(triple("x R y")));
        assertFalse(sut.contains(triple("x R z")));
    }

    @Test
    public void testContains1() {
        sut.add(triple("x R y"));
        sut.add(triple("y S z"));
        sut.add(triple("z T a"));
        assertTrue(sut.contains(null, null, null));
        assertTrue(sut.contains(null, null, node("y")));
        assertTrue(sut.contains(null, node("R"), null));
        assertTrue(sut.contains(null, node("R"), node("y")));
        assertTrue(sut.contains(node("x"), null, null));
        assertTrue(sut.contains(node("x"), null, node("y")));
        assertTrue(sut.contains(node("x"), node("R"), null));
        assertTrue(sut.contains(node("x"), node("R"), node("y")));
    }

    @Test
    public void testContains2() {
        sut.add(triple("x R y"));
        sut.add(triple("y S z"));
        sut.add(triple("z T a"));
        assertTrue(sut.contains(null, null, null));
        assertFalse(sut.contains(null, null, node("x")));
        assertFalse(sut.contains(null, node("U"), null));
        assertFalse(sut.contains(null, node("R"), node("z")));
        assertFalse(sut.contains(node("a"), null, null));
        assertFalse(sut.contains(node("x"), null, node("x")));
        assertFalse(sut.contains(node("y"), node("R"), null));
        assertFalse(sut.contains(node("y"), node("T"), node("a")));
    }



    @Test
    public void testContainsValueObject() {
        sut.add(Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"),
                NodeFactory.createLiteral("0.1", XSDDouble.XSDdouble)));
        assertTrue(sut.contains(Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"),
                NodeFactory.createLiteral("0.1", XSDDouble.XSDdouble))));
        assertFalse(sut.contains(Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"),
                NodeFactory.createLiteral("0.10", XSDDouble.XSDdouble))));
        assertFalse(sut.contains(Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"),
                NodeFactory.createLiteral("0.11", XSDDouble.XSDdouble))));
    }

    @Test
    public void testContainsValueSubject() {
        var containedTriple = Triple.create(
                NodeFactory.createLiteral("0.1", XSDDouble.XSDdouble),
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"));
        sut.add(containedTriple);

        var match = Triple.create(
                NodeFactory.createLiteral("0.1", XSDDouble.XSDdouble),
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"));
        assertTrue(sut.contains(match));
        assertEquals(containedTriple, sut.find(match).next());

        match = Triple.create(
                NodeFactory.createLiteral("0.10", XSDDouble.XSDdouble),
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"));
        assertFalse(sut.contains(match));
        assertFalse(sut.find(match).hasNext());

        match = Triple.create(
                NodeFactory.createLiteral("0.11", XSDDouble.XSDdouble),
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"));
        assertFalse(sut.contains(match));
        assertFalse(sut.find(match).hasNext());
    }

    @Test
    public void testContainsValuePredicate() {
        sut.add(Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createLiteral("0.1", XSDDouble.XSDdouble),
                NodeFactory.createURI("R")));
        assertTrue(sut.contains(Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createLiteral("0.1", XSDDouble.XSDdouble),
                NodeFactory.createURI("R"))));
        assertFalse(sut.contains(Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createLiteral("0.10", XSDDouble.XSDdouble),
                NodeFactory.createURI("R"))));
        assertFalse(sut.contains(Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createLiteral("0.11", XSDDouble.XSDdouble),
                NodeFactory.createURI("R"))));
    }

}

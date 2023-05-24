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
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.GraphMem;
import org.apache.jena.memB.GraphMemB;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.testing_framework.NodeCreateUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.jena.testing_framework.GraphHelper.triple;

public class TestGraphImplementations {

    private static Graph createGraph() {
        return new GraphMemB();
    }

    @Test
    public void testDelete() {
        var sut = createGraph();
        sut.add(triple("x R y"));
        Assert.assertEquals(1, sut.size());
        sut.delete(triple("x R y"));
        Assert.assertEquals(0, sut.size());
        Assert.assertTrue(sut.isEmpty());
    }

    @Test
    public void testFind() {
        var sut = createGraph();
        sut.add(triple("x R y"));
        Assert.assertEquals(1, sut.find(triple("x R y")).toList().size());
        Assert.assertEquals(0, sut.find(triple("x R z")).toList().size());
    }

    @Test
    public void testStream() {
        var sut = createGraph();
        sut.add(triple("x R y"));
        Assert.assertEquals(1, sut.stream().count());
    }

    @Test
    public void testFind1() {
        var sut = createGraph();
        sut.add(triple("x R y"));
        Assert.assertEquals(1, sut.find(null, null, null).toList().size());
        Assert.assertEquals(1, sut.find(null, null, node("y")).toList().size());
        Assert.assertEquals(1, sut.find(null, node("R"), null).toList().size());
        Assert.assertEquals(1, sut.find(null, node("R"), node("y")).toList().size());
        Assert.assertEquals(1, sut.find(node("x"), null, null).toList().size());
        Assert.assertEquals(1, sut.find(node("x"), null, node("y")).toList().size());
        Assert.assertEquals(1, sut.find(node("x"), node("R"), null).toList().size());
        Assert.assertEquals(1, sut.find(node("x"), node("R"), node("y")).toList().size());
    }

    @Test
    public void testFind2() {
        var sut = createGraph();
        sut.add(triple("x R y"));
        Assert.assertEquals(1, sut.find(null, null, null).toList().size());
        Assert.assertEquals(0, sut.find(null, null, node("z")).toList().size());
        Assert.assertEquals(0, sut.find(null, node("S"), null).toList().size());
        Assert.assertEquals(0, sut.find(null, node("S"), node("y")).toList().size());
        Assert.assertEquals(0, sut.find(node("y"), null, null).toList().size());
        Assert.assertEquals(0, sut.find(node("y"), null, node("y")).toList().size());
        Assert.assertEquals(0, sut.find(node("y"), node("R"), null).toList().size());
        Assert.assertEquals(0, sut.find(node("y"), node("R"), node("y")).toList().size());
    }

    public static Node node(String n) {
        return NodeCreateUtils.create(PrefixMapping.Standard, n);
    }

    @Test
    public void testContains() {
        var sut = createGraph();
        sut.add(triple("x R y"));
        Assert.assertTrue(sut.contains(triple("x R y")));
        Assert.assertFalse(sut.contains(triple("x R z")));
    }

    @Test
    public void testContainsValueObject() {
        var sut = createGraph();
        sut.add(Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"),
                NodeFactory.createLiteral("0.1", XSDDouble.XSDdouble)));
        Assert.assertTrue(sut.contains(Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"),
                NodeFactory.createLiteral("0.1", XSDDouble.XSDdouble))));
        Assert.assertTrue(sut.contains(Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"),
                NodeFactory.createLiteral("0.10", XSDDouble.XSDdouble))));
        Assert.assertFalse(sut.contains(Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"),
                NodeFactory.createLiteral("0.11", XSDDouble.XSDdouble))));
    }

    @Test
    public void testContainsValueSubject() {
        var sut = createGraph();
        var containedTriple = Triple.create(
                NodeFactory.createLiteral("0.1", XSDDouble.XSDdouble),
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"));
        sut.add(containedTriple);

        var match = Triple.create(
                NodeFactory.createLiteral("0.1", XSDDouble.XSDdouble),
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"));
        Assert.assertTrue(sut.contains(match));
        Assert.assertEquals(containedTriple, sut.find(match).next());

        match = Triple.create(
                NodeFactory.createLiteral("0.10", XSDDouble.XSDdouble),
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"));
        Assert.assertTrue(sut.contains(match));
        Assert.assertEquals(containedTriple, sut.find(match).next());

        match = Triple.create(
                NodeFactory.createLiteral("0.11", XSDDouble.XSDdouble),
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"));
        Assert.assertFalse(sut.contains(match));
        Assert.assertFalse(sut.find(match).hasNext());
    }

    @Test
    public void testContainsValuePredicate() {
        var sut = createGraph();
        sut.add(Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createLiteral("0.1", XSDDouble.XSDdouble),
                NodeFactory.createURI("R")));
        Assert.assertTrue(sut.contains(Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createLiteral("0.1", XSDDouble.XSDdouble),
                NodeFactory.createURI("R"))));
        Assert.assertTrue(sut.contains(Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createLiteral("0.10", XSDDouble.XSDdouble),
                NodeFactory.createURI("R"))));
        Assert.assertFalse(sut.contains(Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createLiteral("0.11", XSDDouble.XSDdouble),
                NodeFactory.createURI("R"))));
    }

}

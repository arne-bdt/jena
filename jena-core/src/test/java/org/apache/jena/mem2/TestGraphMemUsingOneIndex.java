package org.apache.jena.mem2;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.impl.XSDDouble;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.GraphMem;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.jena.testing_framework.GraphHelper.triple;

public class TestGraphMemUsingOneIndex {

    @Test
    public void testDelete() {
        var sut = new GraphMemUsingOneIndex();
        sut.add(triple("x R y"));
        Assert.assertEquals(1, sut.size());
        sut.delete(triple("x R y"));
        Assert.assertEquals(0, sut.size());
        Assert.assertTrue(sut.isEmpty());
    }

    @Test
    public void testContains() {
        var sut = new GraphMemUsingOneIndex();
        sut.add(triple("x R y"));
        Assert.assertTrue(sut.contains(triple("x R y")));
        Assert.assertFalse(sut.contains(triple("x R z")));
    }

    @Test
    public void testContainsValueObject() {
        var sut = new GraphMemUsingOneIndex();
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
        var sut = new GraphMem2LowMemory();
        sut.add(Triple.create(
                NodeFactory.createLiteral("0.1", XSDDouble.XSDdouble),
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R")));
        Assert.assertTrue(sut.contains(Triple.create(
                NodeFactory.createLiteral("0.1", XSDDouble.XSDdouble),
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"))));
        Assert.assertTrue(sut.contains(Triple.create(
                NodeFactory.createLiteral("0.10", XSDDouble.XSDdouble),
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"))));
        Assert.assertFalse(sut.contains(Triple.create(
                NodeFactory.createLiteral("0.11", XSDDouble.XSDdouble),
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"))));
    }

    @Test
    public void testContainsValuePredicate() {
        var sut = new GraphMemUsingOneIndex();
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

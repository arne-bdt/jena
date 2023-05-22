package org.apache.jena.mem.graph;

import org.apache.jena.atlas.iterator.ActionCount;
import org.apache.jena.datatypes.xsd.impl.XSDDouble;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.GraphMem;
import org.apache.jena.mem.graph.helper.Context;
import org.apache.jena.mem.graph.helper.Releases;
import org.apache.jena.mem2.GraphMem2;
import org.apache.jena.mem2.GraphMemWithAdaptiveTripleStore;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

public class TestGraphConsistentBehavior {

    Context trialContext = new Context("GraphMem2SG (current)");

    @Test
    public void testFillGraph() {
        var triples = Releases.current.readTriples("C:/temp/res_test/xxx_CGMES_EQ.xml");
        var sut = Releases.current.createGraph(trialContext.getGraphClass());
        triples.forEach(sut::add);
        Assert.assertEquals(triples.size(), sut.size());
    }

    @Test
    public void testFindByS__() {
        var triples = Releases.current.readTriples("../testing/cheeses-0.1.ttl");
        var sut = Releases.current.createGraph(trialContext.getGraphClass());
        triples.forEach(sut::add);
        var triplesToFind = Releases.current.cloneTriples(triples);
        for(int i=0; i< triplesToFind.size(); i++) {
            var iterator = sut.find(triplesToFind.get(i).getSubject(), null, null);
            Assert.assertTrue(iterator.hasNext());
        }
    }

    @Test
    public void testFindByS_O() {
        this.trialContext = new Context("GraphMem2 (current)");
        var triples = Releases.current.readTriples("C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml");
        var sut = Releases.current.createGraph(trialContext.getGraphClass());
        triples.forEach(sut::add);
        var triplesToFind = Releases.current.cloneTriples(triples);
        for(int i=0; i< triplesToFind.size(); i++) {
            var iterator = sut.find(triplesToFind.get(i).getSubject(), null, triplesToFind.get(i).getObject());
            Assert.assertTrue(iterator.hasNext());
        }
    }

    @Test
    public void testFindBy_PO() {
        var triples = Releases.current.readTriples("C:/temp/res_test/xxx_CGMES_SSH.xml");

        var graphMem = new GraphMem();
        triples.forEach(graphMem::add);

        var graphMem2 = new GraphMem2();
        triples.forEach(graphMem2::add);

        var triplesToFind = Releases.current.cloneTriples(triples);

        testFindBy_POGraphMem2(graphMem2, triplesToFind);

        testFindBy_POGraphMem(graphMem, triplesToFind);
    }

    private void testFindBy_POGraphMem(GraphMem graphMem, List<Triple> triplesToFind) {
        ActionCount count = new ActionCount();
        for(int i=0; i<1; i++) {
            for (var triple : triplesToFind) {
                var iterator = graphMem.find(null, triple.getPredicate(), triple.getObject());
                iterator.forEachRemaining(count);
            }
        }
        System.out.println("GraphMem:" + count.getCount());
    }

    private void testFindBy_POGraphMem2(GraphMem2 graphMem2, List<Triple> triplesToFind) {
        ActionCount count = new ActionCount();
        for(int i=0; i<1; i++) {
            for (var triple : triplesToFind) {
                var iterator = graphMem2.find(null, triple.getPredicate(), triple.getObject());
                iterator.forEachRemaining(count);
            }
        }
        System.out.println("GraphMem2:"  + count.getCount());
    }

    @Ignore
    @Test
    public void graphRace() {
        var triples = Releases.current.readTriples("../testing/BSBM/bsbm-1m.nt.gz");
        fillGraphMem(triples);
        fillGraphMemWithAdaptiveTripleStore(triples);
    }

    private void fillGraphMemWithAdaptiveTripleStore(List<Triple> triples) {
        for(int i=0; i<10; i++) {
            var sut = new GraphMemWithAdaptiveTripleStore();
            triples.forEach(sut::add);
            Assert.assertEquals(triples.size(), sut.size());
        }
    }

    private void fillGraphMem(List<Triple> triples) {
        for(int i=0; i<10; i++) {
            var sut = new GraphMem2();
            triples.forEach(sut::add);
            Assert.assertEquals(triples.size(), sut.size());
        }
    }

    @Test
    public void testConsistentContainsFindForLiteralSubjects() {
        switch (trialContext.getJenaVersion())
        {
            case CURRENT:
                testConsistentContainsFindForLiteralSubjectsCurrent();
                break;
            case JENA_4_8_0:
                testConsistentContainsFindForLiteralSubjects480();
                break;
            default:
                throw new RuntimeException("Unknown Jena version: " + trialContext.getJenaVersion());
        }
    }

    private void testConsistentContainsFindForLiteralSubjectsCurrent() {
        var sut = Releases.current.createGraph(trialContext.getGraphClass());

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
        Assert.assertFalse(sut.contains(match));
        Assert.assertFalse(sut.find(match).hasNext());

        match = Triple.create(
                NodeFactory.createLiteral("0.11", XSDDouble.XSDdouble),
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"));
        Assert.assertFalse(sut.contains(match));
        Assert.assertFalse(sut.find(match).hasNext());
    }

    private void testConsistentContainsFindForLiteralSubjects480() {
        var sut = Releases.v480.createGraph(trialContext.getGraphClass());

        var containedTriple = org.apache.shadedJena480.graph.Triple.create(
                org.apache.shadedJena480.graph.NodeFactory.createLiteral("0.1",
                        org.apache.shadedJena480.datatypes.xsd.impl.XSDDouble.XSDdouble),
                org.apache.shadedJena480.graph.NodeFactory.createURI("x"),
                org.apache.shadedJena480.graph.NodeFactory.createURI("R"));
        sut.add(containedTriple);

        var match = org.apache.shadedJena480.graph.Triple.create(
                org.apache.shadedJena480.graph.NodeFactory.createLiteral("0.1",
                        org.apache.shadedJena480.datatypes.xsd.impl.XSDDouble.XSDdouble),
                org.apache.shadedJena480.graph.NodeFactory.createURI("x"),
                org.apache.shadedJena480.graph.NodeFactory.createURI("R"));
        Assert.assertTrue(sut.contains(match));
        Assert.assertEquals(containedTriple, sut.find(match).next());

        match = org.apache.shadedJena480.graph.Triple.create(
                org.apache.shadedJena480.graph.NodeFactory.createLiteral("0.10",
                        org.apache.shadedJena480.datatypes.xsd.impl.XSDDouble.XSDdouble),
                org.apache.shadedJena480.graph.NodeFactory.createURI("x"),
                org.apache.shadedJena480.graph.NodeFactory.createURI("R"));
        Assert.assertFalse(sut.contains(match));
        Assert.assertFalse(sut.find(match).hasNext());

        match = org.apache.shadedJena480.graph.Triple.create(
                org.apache.shadedJena480.graph.NodeFactory.createLiteral("0.11",
                        org.apache.shadedJena480.datatypes.xsd.impl.XSDDouble.XSDdouble),
                org.apache.shadedJena480.graph.NodeFactory.createURI("x"),
                org.apache.shadedJena480.graph.NodeFactory.createURI("R"));
        Assert.assertFalse(sut.contains(match));
        Assert.assertFalse(sut.find(match).hasNext());
    }

    @Test
    public void testConsistentContainsFindForLiteralPredicates() {
        switch (trialContext.getJenaVersion())
        {
            case CURRENT:
                testConsistentContainsFindForLiteralPredicatesCurrent();
                break;
            case JENA_4_8_0:
                testConsistentContainsFindForLiteralPredicates480();
                break;
            default:
                throw new RuntimeException("Unknown Jena version: " + trialContext.getJenaVersion());
        }
    }

    private void testConsistentContainsFindForLiteralPredicatesCurrent() {
        var sut = Releases.current.createGraph(trialContext.getGraphClass());

        var containedTriple = Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createLiteral("0.1", XSDDouble.XSDdouble),
                NodeFactory.createURI("R"));
        sut.add(containedTriple);

        var match = Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createLiteral("0.1", XSDDouble.XSDdouble),
                NodeFactory.createURI("R"));
        Assert.assertTrue(sut.contains(match));
        Assert.assertEquals(containedTriple, sut.find(match).next());

        match = Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createLiteral("0.10", XSDDouble.XSDdouble),
                NodeFactory.createURI("R"));
        Assert.assertFalse(sut.contains(match));
        Assert.assertFalse(sut.find(match).hasNext());

        match = Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createLiteral("0.11", XSDDouble.XSDdouble),
                NodeFactory.createURI("R"));
        Assert.assertFalse(sut.contains(match));
        Assert.assertFalse(sut.find(match).hasNext());
    }

    private void testConsistentContainsFindForLiteralPredicates480() {
        var sut = Releases.v480.createGraph(trialContext.getGraphClass());

        var containedTriple = org.apache.shadedJena480.graph.Triple.create(
                org.apache.shadedJena480.graph.NodeFactory.createURI("x"),
                org.apache.shadedJena480.graph.NodeFactory.createLiteral("0.1",
                        org.apache.shadedJena480.datatypes.xsd.impl.XSDDouble.XSDdouble),
                org.apache.shadedJena480.graph.NodeFactory.createURI("R"));
        sut.add(containedTriple);

        var match = org.apache.shadedJena480.graph.Triple.create(
                org.apache.shadedJena480.graph.NodeFactory.createURI("x"),
                org.apache.shadedJena480.graph.NodeFactory.createLiteral("0.1",
                        org.apache.shadedJena480.datatypes.xsd.impl.XSDDouble.XSDdouble),
                org.apache.shadedJena480.graph.NodeFactory.createURI("R"));
        Assert.assertTrue(sut.contains(match));
        Assert.assertEquals(containedTriple, sut.find(match).next());

        match = org.apache.shadedJena480.graph.Triple.create(
                org.apache.shadedJena480.graph.NodeFactory.createURI("x"),
                org.apache.shadedJena480.graph.NodeFactory.createLiteral("0.10",
                        org.apache.shadedJena480.datatypes.xsd.impl.XSDDouble.XSDdouble),
                org.apache.shadedJena480.graph.NodeFactory.createURI("R"));
        Assert.assertFalse(sut.contains(match));
        Assert.assertFalse(sut.find(match).hasNext());

        match = org.apache.shadedJena480.graph.Triple.create(
                org.apache.shadedJena480.graph.NodeFactory.createURI("x"),
                org.apache.shadedJena480.graph.NodeFactory.createLiteral("0.11",
                        org.apache.shadedJena480.datatypes.xsd.impl.XSDDouble.XSDdouble),
                org.apache.shadedJena480.graph.NodeFactory.createURI("R"));
        Assert.assertFalse(sut.contains(match));
        Assert.assertFalse(sut.find(match).hasNext());
    }

    @Test
    public void testConsistentContainsFindForLiteralObjects() {
        switch (trialContext.getJenaVersion())
        {
            case CURRENT:
                testConsistentContainsFindForLiteralObjectsCurrent();
                break;
            case JENA_4_8_0:
                testConsistentContainsFindForLiteralObjects480();
                break;
            default:
                throw new RuntimeException("Unknown Jena version: " + trialContext.getJenaVersion());
        }
    }

    private void testConsistentContainsFindForLiteralObjectsCurrent() {
        var sut = Releases.current.createGraph(trialContext.getGraphClass());

        var containedTriple = Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"),
                NodeFactory.createLiteral("0.1", XSDDouble.XSDdouble));
        sut.add(containedTriple);

        var match = Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"),
                NodeFactory.createLiteral("0.1", XSDDouble.XSDdouble));
        Assert.assertTrue(sut.contains(match));
        Assert.assertEquals(containedTriple, sut.find(match).next());

        match = Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"),
                NodeFactory.createLiteral("0.10", XSDDouble.XSDdouble));
        Assert.assertFalse(sut.contains(match));
        Assert.assertFalse(sut.find(match).hasNext());

        match = Triple.create(
                NodeFactory.createURI("x"),
                NodeFactory.createURI("R"),
                NodeFactory.createLiteral("0.11", XSDDouble.XSDdouble));
        Assert.assertFalse(sut.contains(match));
        Assert.assertFalse(sut.find(match).hasNext());
    }

    private void testConsistentContainsFindForLiteralObjects480() {
        var sut = Releases.v480.createGraph(trialContext.getGraphClass());
        var containedTriple = org.apache.shadedJena480.graph.Triple.create(
                org.apache.shadedJena480.graph.NodeFactory.createURI("x"),
                org.apache.shadedJena480.graph.NodeFactory.createURI("R"),
                org.apache.shadedJena480.graph.NodeFactory.createLiteral("0.1",
                        org.apache.shadedJena480.datatypes.xsd.impl.XSDDouble.XSDdouble));
        sut.add(containedTriple);

        var match = org.apache.shadedJena480.graph.Triple.create(
                org.apache.shadedJena480.graph.NodeFactory.createURI("x"),
                org.apache.shadedJena480.graph.NodeFactory.createURI("R"),
                org.apache.shadedJena480.graph.NodeFactory.createLiteral("0.1",
                        org.apache.shadedJena480.datatypes.xsd.impl.XSDDouble.XSDdouble));
        Assert.assertTrue(sut.contains(match));
        Assert.assertEquals(containedTriple, sut.find(match).next());

        match = org.apache.shadedJena480.graph.Triple.create(
                org.apache.shadedJena480.graph.NodeFactory.createURI("x"),
                org.apache.shadedJena480.graph.NodeFactory.createURI("R"),
                org.apache.shadedJena480.graph.NodeFactory.createLiteral("0.10",
                        org.apache.shadedJena480.datatypes.xsd.impl.XSDDouble.XSDdouble));
        Assert.assertFalse(sut.contains(match));
        Assert.assertFalse(sut.find(match).hasNext());

        match = org.apache.shadedJena480.graph.Triple.create(
                org.apache.shadedJena480.graph.NodeFactory.createURI("x"),
                org.apache.shadedJena480.graph.NodeFactory.createURI("R"),
                org.apache.shadedJena480.graph.NodeFactory.createLiteral("0.11",
                        org.apache.shadedJena480.datatypes.xsd.impl.XSDDouble.XSDdouble));
        Assert.assertFalse(sut.contains(match));
        Assert.assertFalse(sut.find(match).hasNext());
    }
}

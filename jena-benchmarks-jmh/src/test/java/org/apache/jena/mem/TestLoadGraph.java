package org.apache.jena.mem;

import org.apache.jena.graph.Triple;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.jena.mem.jmh.AbstractTestGraphBaseWithFilledGraph.cloneNode;

@Ignore
public class TestLoadGraph {

    @Test
    public void testLoadGraph() {
        var triples = TripleReaderReadingCGMES_2_4_15_WithTypedLiterals.read("./testing/pizza.owl.rdf");
        var sut = new GraphMem();
        triples.forEach(t -> sut.add(Triple.create(cloneNode(t.getSubject()), cloneNode(t.getPredicate()), cloneNode(t.getObject()))));
    }

}

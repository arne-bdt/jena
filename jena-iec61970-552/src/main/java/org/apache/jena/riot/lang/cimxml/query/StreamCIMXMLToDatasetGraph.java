package org.apache.jena.riot.lang.cimxml.query;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.GraphMem2Roaring;
import org.apache.jena.mem2.IndexingStrategy;
import org.apache.jena.riot.lang.cimxml.CIMXMLDocumentContext;
import org.apache.jena.riot.lang.cimxml.StreamCIMXML;
import org.apache.jena.riot.lang.cimxml.graph.ModelHeader;
import org.apache.jena.sparql.core.Quad;

import javax.xml.XMLConstants;

public class StreamCIMXMLToDatasetGraph implements StreamCIMXML {

    private final LinkedCIMDatasetGraph linkedCIMDatasetGraph = new LinkedCIMDatasetGraph();
    private String versionOfCIMXML = null;
    private Graph currentGraph = null;


    public String getVersionOfCIMXML() {
        return versionOfCIMXML;
    }


    public CIMDatasetGraph getCIMDatasetGraph() {
        return linkedCIMDatasetGraph;
    }

    private static GraphMem2Roaring createLazyGraph() {
        return new GraphMem2Roaring(IndexingStrategy.LAZY_PARALLEL);
    }

    private void createAndAddNewCurrentGraph(Node graphName) {
        if(linkedCIMDatasetGraph.containsGraph(graphName)) {
            throw new IllegalArgumentException("Graph with name " + graphName + " already exists in the dataset.");
        }
        currentGraph = createLazyGraph();
        linkedCIMDatasetGraph.addGraph(graphName, currentGraph);
    }

    @Override
    public void start() {
        currentGraph = createLazyGraph();
        linkedCIMDatasetGraph.addGraph(Quad.defaultGraphIRI, currentGraph);
    }

    @Override
    public void triple(Triple triple) {
        currentGraph.add(triple);
    }

    @Override
    public void quad(Quad quad) {
        throw new UnsupportedOperationException("Quads are not supported in this context.");
    }

    @Override
    public void base(String base) {
        linkedCIMDatasetGraph.prefixes.add(XMLConstants.DEFAULT_NS_PREFIX, base);
    }

    @Override
    public void prefix(String prefix, String iri) {
        linkedCIMDatasetGraph.prefixes.add(prefix, iri);
    }

    @Override
    public void finish() {
        linkedCIMDatasetGraph.getGraphs().parallelStream().forEach(graph -> {
            if (graph instanceof GraphMem2Roaring roaring && !roaring.isIndexInitialized()) {
                roaring.initializeIndexParallel();
            }
        });
    }

    @Override
    public void setVersionOfCIMXML(String versionOfCIMXML) {
        this.versionOfCIMXML = versionOfCIMXML;
    }

    @Override
    public void switchContext(CIMXMLDocumentContext cimDocumentContext) {
        switch (cimDocumentContext) {
            case fullModel
                    -> createAndAddNewCurrentGraph(ModelHeader.TYPE_FULL_MODEL);
            case body
                    -> currentGraph = linkedCIMDatasetGraph.getDefaultGraph();
            case differenceModel
                    -> createAndAddNewCurrentGraph(ModelHeader.TYPE_DIFFERENCE_MODEL);
            case forwardDifferences
                    -> createAndAddNewCurrentGraph(CIMXMLDocumentContext.GRAPH_FORWARD_DIFFERENCES);
            case reverseDifferences
                    -> createAndAddNewCurrentGraph(CIMXMLDocumentContext.GRAPH_REVERSE_DIFFERENCES);
            case preconditions
                    -> createAndAddNewCurrentGraph(CIMXMLDocumentContext.GRAPH_PRECONDITIONS);
        }
    }
}

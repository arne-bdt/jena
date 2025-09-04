package org.apache.jena.cimxml.graph;

import org.apache.jena.graph.Graph;
import org.apache.jena.cimxml.CimVersion;

import java.util.Objects;

public interface CIMGraph extends Graph {

    default CimVersion getCIMVersion() {
        return getCIMXMLVersion(this);
    }

    static CimVersion getCIMXMLVersion(Graph graph) {
        Objects.requireNonNull(graph, "graph is null");
        var cimURI = graph.getPrefixMapping().getNsPrefixURI("cim");
        if(cimURI == null)
            return CimVersion.NO_CIM;

        return CimVersion.fromCimNamespacePrefixUri(graph.getPrefixMapping().getNsPrefixURI("cim"));
    }
}

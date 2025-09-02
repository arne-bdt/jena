package org.apache.jena.riot.lang.cimxml.graph;

import org.apache.jena.graph.Graph;
import org.apache.jena.riot.lang.cimxml.CIMVersion;

import java.util.Objects;

public interface CIMGraph extends Graph {

    default CIMVersion getCIMVersion() {
        return getCIMXMLVersion(this);
    }

    static CIMVersion getCIMXMLVersion(Graph graph) {
        Objects.requireNonNull(graph, "graph is null");
        return CIMVersion.fromCimNamespacePrefixUri(graph.getPrefixMapping().getNsPrefixURI("cim"));
    }
}

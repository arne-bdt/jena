package org.apache.jena.cimxml.graph;

import org.apache.jena.graph.Graph;
import org.apache.jena.cimxml.CimVersion;

import java.util.Objects;

/**
 * A specialization of {@link Graph} that provides methods to determine the CIM version
 * of the graph based on its namespace prefixes.
 */
public interface CIMGraph extends Graph {

    /**
     * Determines the CIM version of this graph based on its namespace prefixes.
     * If the graph does not use a CIM namespace, {@link CimVersion#NO_CIM} is returned.
     * @return The CIM version of the graph, or {@link CimVersion#NO_CIM} if no CIM namespace is used.
     */
    default CimVersion getCIMVersion() {
        return getCIMXMLVersion(this);
    }

    /**
     * Determines the CIM version of the given graph based on its namespace prefixes.
     * If the graph does not use a CIM namespace, {@link CimVersion#NO_CIM} is returned.
     * @param graph The graph to determine the CIM version for. Must not be null.
     * @return The CIM version of the graph, or {@link CimVersion#NO_CIM} if no CIM namespace is used.
     * @throws NullPointerException if the graph is null.
     */
    static CimVersion getCIMXMLVersion(Graph graph) {
        Objects.requireNonNull(graph, "graph is null");
        var cimURI = graph.getPrefixMapping().getNsPrefixURI("cim");
        if(cimURI == null)
            return CimVersion.NO_CIM;

        return CimVersion.fromCimNamespacePrefixUri(graph.getPrefixMapping().getNsPrefixURI("cim"));
    }
}

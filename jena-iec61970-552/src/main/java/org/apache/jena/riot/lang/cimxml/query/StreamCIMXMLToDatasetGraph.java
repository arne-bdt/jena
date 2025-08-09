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

    private void createAndAddNewCurrentGraph(Node graphName, IndexingStrategy indexingStrategy) {
        if(linkedCIMDatasetGraph.containsGraph(graphName)) {
            throw new IllegalArgumentException("Graph with name " + graphName + " already exists in the dataset.");
        }
        currentGraph = new GraphMem2Roaring(indexingStrategy);
        linkedCIMDatasetGraph.addGraph(graphName, currentGraph);
    }

    @Override
    public void start() {
        currentGraph = new GraphMem2Roaring(IndexingStrategy.LAZY_PARALLEL);
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
                    // The metadata is usually very small, so we use a minimal indexing strategy.
                    -> createAndAddNewCurrentGraph(ModelHeader.TYPE_FULL_MODEL, IndexingStrategy.MINIMAL);
            case body
                    -> currentGraph = linkedCIMDatasetGraph.getDefaultGraph();
            case differenceModel
                    // The metadata is usually very small, so we use a minimal indexing strategy.
                    -> createAndAddNewCurrentGraph(ModelHeader.TYPE_DIFFERENCE_MODEL, IndexingStrategy.MINIMAL);
            case forwardDifferences
                    -> createAndAddNewCurrentGraph(CIMXMLDocumentContext.GRAPH_FORWARD_DIFFERENCES, IndexingStrategy.LAZY_PARALLEL);
            case reverseDifferences
                    -> createAndAddNewCurrentGraph(CIMXMLDocumentContext.GRAPH_REVERSE_DIFFERENCES, IndexingStrategy.LAZY_PARALLEL);
            case preconditions
                    -> createAndAddNewCurrentGraph(CIMXMLDocumentContext.GRAPH_PRECONDITIONS, IndexingStrategy.LAZY_PARALLEL);
        }
    }
}

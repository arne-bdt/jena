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
import org.apache.jena.riot.lang.cimxml.CIMVersion;
import org.apache.jena.riot.lang.cimxml.StreamCIMXML;
import org.apache.jena.riot.system.ErrorHandler;
import org.apache.jena.riot.system.ErrorHandlerFactory;
import org.apache.jena.sparql.core.Quad;

public class StreamCIMXMLToDatasetGraph implements StreamCIMXML {

    public final ErrorHandler errorHandler;
    private final LinkedCIMDatasetGraph linkedCIMDatasetGraph;
    private String versionOfIEC61970_552 = null;
    private Graph currentGraph;
    private CIMXMLDocumentContext currentContext;
    private CIMVersion versionOfCIMXML = CIMVersion.NO_CIM;

    public StreamCIMXMLToDatasetGraph() {
        this(ErrorHandlerFactory.errorHandlerStd);
    }

    public StreamCIMXMLToDatasetGraph(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
        // init default graph for body context
        currentContext = CIMXMLDocumentContext.body;
        currentGraph = new GraphMem2Roaring(IndexingStrategy.LAZY_PARALLEL);
        linkedCIMDatasetGraph = new LinkedCIMDatasetGraph(currentGraph);
    }

    @Override
    public String getVersionOfIEC61970_552() {
        return versionOfIEC61970_552;
    }

    @Override
    public CIMVersion getVersionOfCIMXML() {
        return versionOfCIMXML;
    }

    @Override
    public void setVersionOfCIMXML(CIMVersion versionOfCIMXML) {
        this.versionOfCIMXML = versionOfCIMXML;
    }

    public CIMDatasetGraph getCIMDatasetGraph() {
        return linkedCIMDatasetGraph;
    }

    private void setCurrentGraphAndCreateIfNecessary(Node graphName, IndexingStrategy indexingStrategy) {
        if(linkedCIMDatasetGraph.containsGraph(graphName)) {
            currentGraph = linkedCIMDatasetGraph.getGraph(graphName);
        } else {
            currentGraph = new GraphMem2Roaring(indexingStrategy);
            linkedCIMDatasetGraph.addGraph(graphName, currentGraph);
        }
    }

    @Override
    public void start() {
        // Nothing to do
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
        // Nothing to do
    }

    @Override
    public void prefix(String prefix, String iri) {
        linkedCIMDatasetGraph.prefixes.add(prefix, iri);
    }

    @Override
    public void finish() {
        // Initialize indexes in parallel for all graphs that use LAZY_PARALLEL indexing strategy.
        linkedCIMDatasetGraph.getGraphs().parallelStream().forEach(graph -> {
            if (graph instanceof GraphMem2Roaring roaring && !roaring.isIndexInitialized()) {
                roaring.initializeIndexParallel();
            }
            graph.getPrefixMapping().setNsPrefixes(linkedCIMDatasetGraph.prefixes.getMapping());
        });
    }

    @Override
    public void setVersionOfIEC61970_552(String versionOfIEC61970_552) {
        this.versionOfIEC61970_552 = versionOfIEC61970_552;
    }

    @Override
    public CIMXMLDocumentContext getCurrentContext() {
        return currentContext;
    }

    @Override
    public void setCurrentContext(CIMXMLDocumentContext context) {
        switchContext(context);
    }

    public void switchContext(CIMXMLDocumentContext cimDocumentContext) {
        var indexingStrategy = switch (cimDocumentContext) {
            // The metadata is usually very small, so we use a minimal indexing strategy.
            case fullModel, differenceModel -> IndexingStrategy.MINIMAL;
            // The data parts can be large, so we use a lazy parallel indexing strategy.
            default -> IndexingStrategy.LAZY_PARALLEL;
        };
        var graphName = CIMXMLDocumentContext.getGraphName(cimDocumentContext);
        setCurrentGraphAndCreateIfNecessary(graphName, indexingStrategy);
        currentContext = cimDocumentContext;
    }
}

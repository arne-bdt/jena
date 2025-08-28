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

package org.apache.jena.riot.lang.cimxml.graph;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.graph.GraphWrapper;
import org.apache.jena.vocabulary.RDF;

import java.util.List;
import java.util.stream.Stream;

public class ProfileOntologyCIM100 extends GraphWrapper implements ProfileOntology {

    final static String NS_EUMD = "https://ap.cim4.eu/DocumentHeader#";
    final static String NS_OWL = "http://www.w3.org/2002/07/owl#";
    final static String NS_DCAT = "http://www.w3.org/ns/dcat#";
    final static Node CLASS_ONTOLOGY = NodeFactory.createURI(NS_OWL + "Ontology");
    final static Node PREDICATE_DCAT_KEYWORD = NodeFactory.createURI(NS_DCAT + "keyword");
    final static Node PREDICATE_OWL_VERSION_IRI = NodeFactory.createURI(NS_OWL + "versionIRI");
    final static Node PREDICATE_OWL_VERSION_INFO = NodeFactory.createURI(NS_OWL + "versionInfo");

    /**
     * Find the ontology node in the graph.
     * The ontology node is defined as a subject with type owl:Ontology.
     * If no such node is found, null is returned.
     *
     * @param graph The graph to search in.
     * @return The ontology node or null if not found.
     */
    static boolean hasOntology(Graph graph) {
        return graph.find(Node.ANY, RDF.type.asNode(), CLASS_ONTOLOGY).hasNext();
    }

    public Node getOntology() {
        return stream(Node.ANY, RDF.type.asNode(), CLASS_ONTOLOGY).findAny().map(Triple::getSubject).orElseThrow();
    }

    public static boolean hasVersionIRIAndKeyword(Graph graph) {
        return graph.find(Node.ANY, PREDICATE_DCAT_KEYWORD, Node.ANY).hasNext()
                && graph.find(Node.ANY, PREDICATE_OWL_VERSION_IRI, Node.ANY).hasNext();
    }

    public ProfileOntologyCIM100(Graph graph) {
        super(graph);
    }

    @Override
    public MetadataStyle getMetadataStyle() {
        return MetadataStyle.ONTOLOGY;
    }

    @Override
    public boolean isHeaderProfile() {
        // look for https://ap.cim4.eu/DocumentHeader# without # in version IRIs
        return stream(getOntology(), PREDICATE_OWL_VERSION_IRI, Node.ANY)
                .anyMatch(t
                        -> t.getObject().isURI()
                        && t.getObject().getURI().regionMatches(0, NS_EUMD, 0, NS_EUMD.length()-1));
    }

    @Override
    public String getDcatKeyword() {
        var iter = find(getOntology(), PREDICATE_DCAT_KEYWORD, Node.ANY);
        return iter.hasNext() ? iter.next().getObject().getLiteralValue().toString() : null;
    }

    @Override
    public Stream<Node> getOwlVersionIRIs() {
        return stream(getOntology(), PREDICATE_OWL_VERSION_IRI, Node.ANY)
                .map(Triple::getObject);
    }

    @Override
    public String getOwlVersionInfo() {
        var iter = find(getOntology(), PREDICATE_OWL_VERSION_INFO, Node.ANY);
        return iter.hasNext() ? iter.next().getObject().getLiteralValue().toString() : null;
    }
}

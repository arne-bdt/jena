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

package org.apache.jena.cimxml.graph;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.graph.GraphWrapper;
import org.apache.jena.vocabulary.RDF;

import java.util.Set;
import java.util.stream.Collectors;

public class CimProfile17 extends GraphWrapper implements CimProfile {

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

    public static Node getOntology(Graph graph) {
        return graph.stream(Node.ANY, RDF.type.asNode(), CLASS_ONTOLOGY).findAny().map(Triple::getSubject).orElseThrow();
    }

    public Node getOntology() {
        return getOntology(this);
    }

    public static boolean hasVersionIRIAndKeyword(Graph graph) {
        return graph.find(Node.ANY, PREDICATE_DCAT_KEYWORD, Node.ANY).hasNext()
                && graph.find(Node.ANY, PREDICATE_OWL_VERSION_IRI, Node.ANY).hasNext();
    }

    private final boolean isHeaderProfile;

    public CimProfile17(Graph graph, boolean isHeaderProfile) {
        super(graph);
        this.isHeaderProfile = isHeaderProfile;
    }

    @Override
    public boolean isHeaderProfile() {
        return this.isHeaderProfile;
    }

    @Override
    public String getDcatKeyword() {
        if(isHeaderProfile) {
            // CGMES v3.0 file header profiles do not have a keyword.
            return "DH"; // Use "DH" for compatibility with old CGMES 2.4.15 file header profiles.
        }
        var iter = find(getOntology(), PREDICATE_DCAT_KEYWORD, Node.ANY);
        return iter.hasNext() ? iter.next().getObject().getLiteralValue().toString() : null;
    }

    @Override
    public Set<Node> getOwlVersionIRIs() {
        return stream(getOntology(), PREDICATE_OWL_VERSION_IRI, Node.ANY)
                .map(Triple::getObject)
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public String getOwlVersionInfo() {
        var iter = find(getOntology(), PREDICATE_OWL_VERSION_INFO, Node.ANY);
        return iter.hasNext() ? iter.next().getObject().getLiteralValue().toString() : null;
    }

    @Override
    public final boolean equals(Object other) {
        if (!(other instanceof CimProfile17 that)) return false;

        return this.equals(that);
    }

    @Override
    public int hashCode() {
        return this.calculateHashCode();
    }

    public static boolean isHeaderProfile(Graph graph) {
        return graph.stream(Node.ANY, RDF.type.asNode(), TYPE_CLASS_CATEGORY)
                .anyMatch(t
                        -> t.getSubject().isURI()
                        && t.getSubject().getURI().endsWith(PACKAGE_FILE_HEADER_PROFILE));
    }
}

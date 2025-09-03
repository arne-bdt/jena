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
import org.apache.jena.vocabulary.RDF;

import java.util.Set;

public interface ProfileOntology extends CIMGraph {

    String NS_CIMS = "http://iec.ch/TC57/1999/rdf-schema-extensions-19990926#";
    String CLASS_CLASS_CATEGORY = "ClassCategory";
    String PACKAGE_FILE_HEADER_PROFILE = "#Package_FileHeaderProfile";

    Node TYPE_CLASS_CATEGORY = NodeFactory.createURI(NS_CIMS + CLASS_CLASS_CATEGORY);

    /**
     * The header profile describes the RDF schema for a CIM model header or document header.
     * These profiles are not references by the model. So in the profile registry header profiles usually are not
     * selected by their versionIRI.
     *
     * @return true if this profile is a header profile, false otherwise.
     */
    boolean isHeaderProfile();

    /**
     * Abbreviation or keyword for the profile.
     * This is usually dcat:keyword.
     * For CGMES 2.4.15 profiles it is "{Profile}Version.shortName cims:isFixed ?keyword".
     * <p>
     * In CGMES 2.4.15 file header profiles do not contain a "shortName" or "keyword". But the new ontology document
     * header typically contain "DH" as keyword. To maintain compatibility, "DH" shall be used for old CGMES 2.4.15
     * file header profiles.
     *
     * @return The keyword for the profile, or null if no keyword is defined.
     */
    String getDcatKeyword();

    /**
     * The version IRIs of the profile.
     * This is usually owl:versionIRI.
     * For CGMES 2.4.15 profiles it is
     *  "{Profile}Version.baseURI.{*} cims:isFixed ?versionIRI"
     *  and
     *  "{Profile}Version.entsoeURI{*} cims:isFixed ?versionIRI".
     *  <p>
     *  One profile can have multiple version IRIs, at least in CGMES 2.4.15 profiles.
     *
     * @return The version IRI of the profile, or null if no version IRI is defined.
     */
    Set<Node> getOwlVersionIRIs();

    /**
     * Return owl:versionInfo of the onology object of the profile.
     * For CGMES 2.4.15, there is no such version info.
     *
     * @return The version info of the profile, or null if no version info is defined.
     */
    String getOwlVersionInfo();

    /**
     * Wraps a graph as a ProfileOntology.
     * If the graph is already a ProfileOntology, it is returned as is.
     * Otherwise, a new ProfileOntologyImpl is created wrapping the graph.
     *
     * @param graph The graph to wrap.
     * @return A ProfileOntology wrapping the given graph.
     */
    static ProfileOntology wrap(Graph graph) throws IllegalArgumentException {
        if (graph instanceof ProfileOntology po) {
            return po;
        }
        var cimVersion = CIMGraph.getCIMXMLVersion(graph);
        return switch (cimVersion) {
            case CIM_16 -> {
                if(isHeaderProfile(graph)) {
                    // If the graph contains header profile, skip the version IRI and keyword check.
                    yield new ProfileOntologyCIM16(graph, true);
                }
                if(!ProfileOntologyCIM16.hasVersionIRIAndKeyword(graph)) {
                    throw new IllegalArgumentException("Graph does not contain the required '...Version.shortName' and '...Version.entsoeURI*' or '...Version.baseURI...' for a CGMES 2.4.15 profile.");
                }
                yield new ProfileOntologyCIM16(graph, false);
            }
            case CIM_17, CIM_18 -> {
                if(ProfileOntologyCIM17.hasOntology(graph)) {
                    if(!ProfileOntologyCIM17.hasVersionIRIAndKeyword(graph)) {
                        throw new IllegalArgumentException("Graphs ontology does not contain the required versionIRI and keyword for a CIM profile.");
                    }
                    // If the graph contains the ontology subject, it is assumed to be a CGMES 2.4.15 profile.
                    yield new ProfileOntologyCIM17(graph, false);
                }
                if(isHeaderProfile(graph)) {
                    // If the graph contains header profile --> it is still CIM16 style
                    yield new ProfileOntologyCIM17(graph, true);
                }
                throw new IllegalArgumentException("Graph does not contain the required ontology subject for a CIM profile.");
            }
            case NO_CIM -> throw new IllegalArgumentException("Graph does not appear to be a CIM graph. No proper 'cim' namespace defined.");
        };
    }

    static boolean isHeaderProfile(Graph graph) {
        return graph.stream(Node.ANY, RDF.type.asNode(), TYPE_CLASS_CATEGORY)
                .anyMatch(t
                        -> t.getSubject().isURI()
                        && t.getSubject().getURI().endsWith(PACKAGE_FILE_HEADER_PROFILE));
    }
}

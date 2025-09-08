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

import java.util.Set;

/**
 * A CIM profile ontology graph.
 * A profile describes a subset of the CIM schema for a specific use case, e.g. CGMES 2.4.15.
 * A profile is identified by its version IRI(s).
 * A profile can be a header profile or a full profile.
 * A header profile describes the RDF schema for a CIM model header or document header.
 * A full profile describes the RDF schema for a CIM model.
 * <p>
 * CGMES 2.4.15 profiles are identified by their version IRIs, which are defined in a
 * "{Profile}Version" class, e.g. "CGMES_2_4_15Version".
 * The version IRI(s) are defined as fixed properties of the "{Profile}Version" class,
 * e.g. "CGMES_2_4_15Version.baseURI.*" and "CGMES_2_4_15Version.entsoeURI*".
 * The "{Profile}Version" class also defines a "shortName" property, which is used as
 * dcat:keyword of the profile.
 * <p>
 * Header profiles do not contain version IRIs or keywords in CGMES 2.4.15 profiles.
 * But the new ontology document header typically contain "DH" as keyword.
 * To maintain compatibility, "DH" shall be used for old CGMES 2.4.15 file header profiles.
 * <p>
 * In CIM 17 and CIM 18 profiles, the version IRI(s) and keyword are defined in the ontology
 * object of the profile, using standard OWL properties owl:versionIRI and dcat:keyword.
 * <p>
 * A profile can be wrapped around any graph that contains the required information.
 * The static {@link #wrap(Graph)} method can be used to create a CimProfile from any graph.
 * It checks if the graph contains the required information and throws an IllegalArgumentException
 * if not.
 */
public interface CimProfile extends CimGraph {

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
     * Checks if this profile is equal to another profile.
     * Two profiles are equal if they have the same CIM version and the same set of version
     * IRIs, or if both are header profiles.
     * @param other The other profile to compare to.
     * @return true if the profiles are equal, false otherwise.
     */
    default boolean equals(CimProfile other) {
        if(other == null) {
            return false;
        }
        if(!this.getCIMVersion().equals(other.getCIMVersion())) {
            return false;
        }
        if(isHeaderProfile()) {
            return other.isHeaderProfile();
        }
        return this.getOwlVersionIRIs().equals(other.getOwlVersionIRIs());
    }

    /**
     * Calculates the hash code for this profile.
     * The hash code is based on the CIM version and the set of version IRIs, or if it is a header profile.
     * @return The hash code for this profile.
     */
    default int calculateHashCode() {
        // hash code from isHeader, cimVersion and version IRIs
        int result = Boolean.hashCode(isHeaderProfile());
        result = 31 * result + getCIMVersion().hashCode();
        if (!isHeaderProfile()) {
            result = 31 * result + getOwlVersionIRIs().hashCode();
        }
        return result;
    }

    /**
     * Wraps a graph as a CimProfile.
     * If the graph is already a CimProfile, it is returned as is.
     * Otherwise, a new ProfileOntologyImpl is created wrapping the graph.
     *
     * @param graph The graph to wrap.
     * @return A CimProfile wrapping the given graph.
     */
    static CimProfile wrap(Graph graph) throws IllegalArgumentException {
        if (graph instanceof CimProfile po) {
            return po;
        }
        var cimVersion = CimGraph.getCIMXMLVersion(graph);
        return switch (cimVersion) {
            case CIM_16 -> {
                if(CimProfile16.isHeaderProfile(graph)) {
                    // If the graph contains header profile, skip the version IRI and keyword check.
                    yield new CimProfile16(graph, true);
                }
                if(!CimProfile16.hasVersionIRIAndKeyword(graph)) {
                    throw new IllegalArgumentException("Graph does not contain the required '...Version.shortName' and '...Version.entsoeURI*' or '...Version.baseURI...' for a CGMES 2.4.15 profile.");
                }
                yield new CimProfile16(graph, false);
            }
            case CIM_17 -> {
                if(CimProfile17.isHeaderProfile(graph)) {
                    // If the graph contains header profile --> it is still CIM16 style
                    yield new CimProfile17(graph, true);
                }
                if(CimProfile17.hasOntology(graph)) {
                    if(!CimProfile17.hasVersionIRIAndKeyword(graph)) {
                        throw new IllegalArgumentException("Graphs ontology does not contain the required versionIRI and keyword for a CIM profile.");
                    }
                    // If the graph contains the ontology subject, it is assumed to be a CGMES 2.4.15 profile.
                    yield new CimProfile17(graph, false);
                }
                throw new IllegalArgumentException("Graph does not contain the required ontology subject for a CIM profile.");
            }
            case CIM_18 -> {
                if(CimProfile18.isHeaderProfile(graph)) {
                    // If the graph contains header profile --> it is still CIM16 style
                    yield new CimProfile18(graph, true);
                }
                if(CimProfile18.hasOntology(graph)) {
                    if(!CimProfile18.hasVersionIRIAndKeyword(graph)) {
                        throw new IllegalArgumentException("Graphs ontology does not contain the required versionIRI and keyword for a CIM profile.");
                    }
                    // If the graph contains the ontology subject, it is assumed to be a CGMES 2.4.15 profile.
                    yield new CimProfile18(graph, false);
                }
                throw new IllegalArgumentException("Graph does not contain the required ontology subject for a CIM profile.");
            }
            case NO_CIM -> throw new IllegalArgumentException("Graph does not appear to be a CIM graph. No proper 'cim' namespace defined.");
        };
    }


}

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
import org.apache.jena.sparql.graph.GraphWrapper;
import org.apache.jena.vocabulary.RDF;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A wrapper for a graph that contains a CIM profile ontology as defined using CIM 16
 */
public class CimProfile16 extends GraphWrapper implements CimProfile {

    final static String NS_RDFS = "http://www.w3.org/2000/01/rdf-schema#";

    /**
     * This is how the profile IRI ends in CGMES 2.4.15.
     * Example: "http://entsoe.eu/CIM/SchemaExtension/3/1#EquipmentVersion"
     */
    final static String PROFILE_VERSION_POSTFIX = "Version";

    final static Node PREDICATE_RDFS_DOMAIN = NodeFactory.createURI(NS_RDFS + "domain");
    final static Node PREDICATE_CIMS_IS_FIXED = NodeFactory.createURI(NS_CIMS + "isFixed");

    /**
     * Checks if the given graph contains the required information to be wrapped as a CimProfile.
     * It must contain exactly one fixed text for the property cim:shortName.
     * It may contain zero or more fixed texts for the properties cim:entsoeURI and cim:baseURI.
     * If the graph contains at least one fixed text for cim:entsoeURI or cim:baseURI,
     * it must also contain a fixed text for cim:shortName.
     * @param graph The graph to check.
     * @return true if the graph can be wrapped as a CimProfile, false otherwise.
     */
    public static boolean hasVersionIRIAndKeyword(Graph graph) {
        if(getProfilePropertyFixedTexts(graph, ".shortName").findAny().isEmpty()) {
            return false;   //no keyword defined
        }
        if(getProfilePropertyFixedTexts(graph, ".entsoeURI").findAny().isPresent()) {
            return true; //at least one version IRI defined
        }
        if(getProfilePropertyFixedTexts(graph, ".baseURI").findAny().isPresent()) {
            return true; //at least one version IRI defined
        }
        return false; //no version IRI defined
    }


    private static Stream<String> getProfilePropertyFixedTexts(Graph graph, String propertyNameStartWithIncludingDot) {
        return graph.stream(Node.ANY, PREDICATE_RDFS_DOMAIN, Node.ANY) //first look for the domain
                .filter(t
                        -> t.getObject().isURI()
                        && t.getObject().getURI().endsWith(PROFILE_VERSION_POSTFIX)
                        && t.getSubject().isURI()
                        && t.getSubject().getURI().startsWith(t.getObject().getURI())
                        && t.getSubject().getURI().regionMatches(t.getObject().getURI().length(),
                        propertyNameStartWithIncludingDot,0, propertyNameStartWithIncludingDot.length()))
                .flatMap(t -> graph
                        .stream(t.getSubject(), PREDICATE_CIMS_IS_FIXED, Node.ANY)
                        .filter(t2 -> t2.getObject().isLiteral())
                        .map(t2 -> t2.getObject().getLiteralLexicalForm()));
    }

    private final boolean isHeaderProfile;

    /**
     * Wraps the given graph as a CimProfile.
     * @param graph The graph to wrap.
     * @return The wrapped CimProfile.
     * @throws IllegalArgumentException if the graph does not contain the required information to be a CimProfile.
     */
    public CimProfile16(Graph graph, boolean isHeaderProfile) {
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
            // CGMES 2.4.15 file header profiles do not have a keyword.
            return "DH"; // Use "DH" for compatibility with old CGMES 2.4.15 file header profiles.
        }
        return getProfilePropertyFixedTexts(get(), ".shortName")
                .findFirst().orElse(null);
    }

    @Override
    public Set<Node> getOwlVersionIRIs() {
        return Stream.concat(
                getProfilePropertyFixedTexts(get(), ".entsoeURI"),
                getProfilePropertyFixedTexts(get(), ".baseURI"))
                .map(NodeFactory::createURI)
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public String getOwlVersionInfo() {
        return null;
    }

    @Override
    public final boolean equals(Object other) {
        if (!(other instanceof CimProfile16 that)) return false;

        return this.equals(that);
    }

    @Override
    public int hashCode() {
        return this.calculateHashCode();
    }

    /**
     * Determines if the given graph is a header profile.
     * In CIM 16, header profiles are identified by the presence of a
     * cim:Category with the value "PackageFileHeaderProfile".
     */
    public static boolean isHeaderProfile(Graph graph) {
        return graph.stream(Node.ANY, RDF.type.asNode(), TYPE_CLASS_CATEGORY)
                .anyMatch(t
                        -> t.getSubject().isURI()
                        && t.getSubject().getURI().endsWith(PACKAGE_FILE_HEADER_PROFILE));
    }
}

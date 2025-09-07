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

package org.apache.jena.cimxml.rdfs;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.cimxml.CimVersion;
import org.apache.jena.cimxml.graph.CimProfile;

import java.util.Map;
import java.util.Set;

public interface CimProfileRegistry {

    /**
     * A record to hold the rdfType(class), property, primitive type and reference type of a property.
     * Either primitiveType or referenceType may be null, but not both.
     * If primitiveType is not null, the property is a primitive property.
     * If referenceType is not null, the property is a reference property.
     */
    record PropertyInfo(Node rdfType, Node property, Node cimDatatype, RDFDatatype primitiveType, Node referenceType) {}

    /**
     * Registers an ontology graph for profiles in the registry.
     * During registration, the data types of all properties in the graph are extracted and stored in a map for fast lookup.
     * Throws an IllegalArgumentException if one of the profiles owlVersionIRIs is already registered
     * or in case of a header profile, if one has already been registered for the same CIM version.
     * @param cimProfile The profile ontology to register.
     */
    void register(CimProfile cimProfile);

    /**
     * Checks if the registry contains all profile IRIs in the given set.
     * @param owlVersionIRIs A set of profile IRIs as found in the model header.
     * @return true if all profile IRIs are registered, false otherwise.
     */
    boolean containsProfile(Set<Node> owlVersionIRIs);

    /**
     * Checks if the registry contains a header profile for the given CIM version.
     * @param version The CIM version to check.
     * @return true if a header profile for the given CIM version is registered, false otherwise.
     */
    boolean containsHeaderProfile(CimVersion version);

    /**
     * Get all registered ontologies in the registry.
     * @return A collection of all registered ontologies.
     */
    Set<CimProfile> getRegisteredProfiles();

    /**
     * Get all properties and their associated RDF datatypes for the given set of profile IRIs.
     * The set may contain profile IRIs for multiple ontologies.
     * Throws an IllegalArgumentException if one of the profile IRIs is not registered.
     * @param owlVersionIRIs A set of profile IRIs as found in the model header.
     * @return A map of properties and their associated RDF datatypes. The map is thread-safe for reading.
     *         Returns null if one of the profile IRIs is not registered.
     */
    Map<Node, PropertyInfo> getPropertiesAndDatatypes(Set<Node> owlVersionIRIs);

    /**
     * Get all properties and their associated RDF datatypes for the header profile of the given CIM version.
     * Throws an IllegalArgumentException if no header profile has been registered for the given CIM version.
     * @param version The CIM version for which the header profile should be used.
     * @return A map of properties and their associated RDF datatypes. The map is thread-safe for reading.
     *         Returns null if no header profile is registered for the given CIM version.
     */
    Map<Node, PropertyInfo> getHeaderPropertiesAndDatatypes(CimVersion version);

    /**
     * Get a mapping of primitive type names to RDF datatypes for all registered profiles.
     * This includes primitive types from all registered ontologies.
     * @return A map of primitive type names to RDF datatypes. The map is thread-safe for reading.
     */
    Map<String, RDFDatatype> getPrimitiveToRDFDatatypeMapping();

    /**
     * Registers a custom primitive type with the given CIM primitive type name and RDF datatype.
     * If the primitive type name is already registered, it will be overwritten with the new RDF datatype.
     * This method can be used to register custom primitive types that are not part of the standard CIM profiles.
     * The rdfDatatype must also be registered with Jena's TypeMapper.
     * @param cimPrimitiveTypeName The CIM primitive type name, e.g. "string", "int", "float", etc.
     * @param rdfDatatype The RDF datatype to associate with the given CIM primitive type name.
     */
    void registerPrimitiveType(String cimPrimitiveTypeName, RDFDatatype rdfDatatype);
}

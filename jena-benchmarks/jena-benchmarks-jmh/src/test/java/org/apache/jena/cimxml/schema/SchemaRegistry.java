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

package org.apache.jena.cimxml.schema;

import org.apache.commons.io.input.BufferedFileChannelInputStream;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.datatypes.xsd.impl.RDFLangString;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.GraphMem2Fast;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RDFParserBuilder;
import org.apache.jena.riot.lang.rdfxml.RRX;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.exec.QueryExec;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * A registry for RDF schemas.
 * <p>
 * The registry is used to register RDF schemas and to parse RDF/XML files using the registered schemas. *
 */
public class SchemaRegistry {

    /**
     * Registers a schema from a file.
     * The given uriOfFile is parsed as RDF/XML.
     * The typed properties of the schema are extracted and stored in the registry.
     * @param schemaUri The URI of the schema, this is used to identify the schema
     * @param uriOrFile The URI or file name of the schema, this is used to load the schema
     * @return
     */
    public SchemaRegistry register(Node schemaUri, String uriOrFile) {
        return register(schemaUri, uriOrFile, BaseURI.DEFAULT_BASE_URI);
    }

    public SchemaRecord getSchemaRecord(Node schemaUri) {
        return schemaMap.get(schemaUri);
    }

    public boolean contains(Node schemaUri) {
        return schemaMap.containsKey(schemaUri);
    }

    /**
     * Registers a schema from a file.
     * The given uriOfFile is parsed as RDF/XML.
     * The typed properties of the schema are extracted and stored in the registry.
     * @param schemaUri The URI of the schema, this is used to identify the schema
     * @param uriOrFile The URI or file name of the schema, this is used to load the schema
     * @param baseURIForParsing The base URI for parsing the schema
     * @return This SchemaRegistry
     */
    public SchemaRegistry register(Node schemaUri, String uriOrFile, String baseURIForParsing) {
        final Graph g = new GraphMem2Fast();
        try(final var is = new BufferedFileChannelInputStream.Builder()
                .setFile(uriOrFile)
                .setOpenOptions(StandardOpenOption.READ)
                .setBufferSize(64*4096)
                .get()) {
            RDFParser
                    .source(is)
                    .lang(Lang.RDFXML)
                    .checking(false)
                    .parse(g);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        register(schemaUri, g, baseURIForParsing);
        return this;
    }

    /**
     * Registers a schema from an input stream.
     * The given input stream is parsed as RDF/XML.
     * The typed properties of the schema are extracted and stored in the registry.
     * @param schemaUri The URI of the schema, this is used to identify the schema
     * @param inputStream The input stream of the schema, this is used to load the schema
     * @return This SchemaRegistry
     */
    public SchemaRegistry register(Node schemaUri, InputStream inputStream) {
        return register(schemaUri, inputStream, BaseURI.DEFAULT_BASE_URI);
    }

    /**
     * Registers a schema from an input stream.
     * The given input stream is parsed as RDF/XML.
     * The typed properties of the schema are extracted and stored in the registry.
     * @param schemaUri The URI of the schema, this is used to identify the schema
     * @param inputStream The input stream of the schema, this is used to load the schema
     * @param baseURIForParsing The base URI for parsing instance graphs
     * @return This SchemaRegistry
     */
    public SchemaRegistry register(Node schemaUri, InputStream inputStream, String baseURIForParsing) {
        final Graph g = new GraphMem2Fast();
        RDFParser
                .source(inputStream)
                .lang(Lang.RDFXML)
                .checking(false)
                .parse(g);
        register(schemaUri, g, baseURIForParsing);
        return this;
    }

    /**
     * Registers a schema from a graph.
     * The given graph is used to register the schema.
     * The typed properties of the schema are extracted and stored in the registry.
     * @param schemaUri The URI of the schema, this is used to identify the schema
     * @param schemaGraph The graph of the schema
     * @return This SchemaRegistry
     */
    public SchemaRegistry register(Node schemaUri,  Graph schemaGraph) {
        return register(schemaUri, schemaGraph, BaseURI.DEFAULT_BASE_URI);
    }

    /**
     * Registers a schema from a graph.
     * The given graph is used to register the schema.
     * The typed properties of the schema are extracted and stored in the registry.
     * @param schemaUri The URI of the schema, this is used to identify the schema
     * @param schemaGraph The graph of the schema
     * @param baseURIForParsing The base URI for parsing instance graphs
     * @return This SchemaRegistry
     */
    public SchemaRegistry register(Node schemaUri, Graph schemaGraph, String baseURIForParsing) {
        if(schemaMap.containsKey(schemaUri))
            return this;
        typedProperties.put(schemaUri, getTypedProperties(schemaGraph));
        schemaMap.put(schemaUri, new SchemaRecord(schemaUri, schemaGraph, baseURIForParsing));
        return this;
    }

    /**
     * Parses an RDF/XML file using the schema with the given URI.
     * The typed properties of the schema are used to parse typed literal values of the instance graph.
     * @param schemaUri The URI identifying the schema
     * @param filename The file name of the instance graph
     * @return
     */
    public Graph parseRDFXML(Node schemaUri, String filename) {
        final Graph g;
        try(final var is = new BufferedFileChannelInputStream.Builder()
                .setFile(filename)
                .setOpenOptions(StandardOpenOption.READ)
                .setBufferSize(64*4096)
                .get()) {
            g = parseRDFXML(schemaUri, is);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return g;
    }

    /**
     * Parses an RDF/XML file using the schema with the given URI.
     * The typed properties of the schema are used to parse typed literal values of the instance graph.
     * @param schemaUri The URI identifying the schema
     * @param inputStream The input stream of the instance graph
     * @return
     */
    public Graph parseRDFXML(Node schemaUri, InputStream inputStream) {
        return parseRDFXML(schemaUri, RDFParser.source(inputStream));
    }

    private Graph parseRDFXML(Node schemaUri, RDFParserBuilder parserBuilderWithSource) {
        final var schemaGraph = schemaMap.get(schemaUri);
        if(schemaGraph == null)
            throw new IllegalArgumentException("The schema with URI '" + schemaUri + "' is not registered.");
        final var sink = new GraphMem2Fast();
        parserBuilderWithSource
                .base(schemaGraph.baseUriForParsing())  // base URI for the model and thus for al mRID's in the model
                .lang(Lang.RDFXML)
                .checking(false)
                .parse(new StreamTypedTriples(sink, this.typedProperties.get(schemaUri)));
        return sink;
    }

    private final ConcurrentMap<Node, SchemaRecord> schemaMap = new java.util.concurrent.ConcurrentHashMap<>();
    private final ConcurrentMap<Node, Map<Node, ParsingDirective>> typedProperties = new java.util.concurrent.ConcurrentHashMap<>();
    private final static Query typedPropertiesQuery = QueryFactory.create("""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX cims: <http://iec.ch/TC57/1999/rdf-schema-extensions-19990926#>

            SELECT ?property ?primitiveType
            WHERE
            {
                ?property cims:dataType ?dataType.
                {
                    ?dataType cims:stereotype "CIMDatatype".
                    []  rdfs:domain ?dataType;
                        rdfs:label ?label;
                        #rdfs:label "value";
                        cims:dataType/cims:stereotype "Primitive";
                        cims:dataType/rdfs:label ?primitiveType.
                    FILTER (!bound(?label) ||  str(?label) = "value")
                    FILTER (str(?primitiveType) != "String")
                }
                UNION
                {
                    ?dataType   cims:stereotype "Primitive";
                                rdfs:label ?primitiveType.
                    FILTER (str(?primitiveType) != "String")
                }
            }
            """);

    public record ParsingDirective(boolean isIRI, RDFDatatype datatype) {};

    private static Map<Node, ParsingDirective> getTypedProperties(Graph g) {
        final var map = new HashMap<Node, ParsingDirective>();
        QueryExec.graph(g)
                .query(typedPropertiesQuery)
                .select()
                .forEachRemaining(vars -> {
                    final var property = vars.get("property");
                    final var primitiveType = vars.get("primitiveType").getLiteralLexicalForm();
                    if("IRI".equals(primitiveType)) {
                        map.put(property, new ParsingDirective(true, null));
                    } else {
                        map.put(property, new ParsingDirective(false, getDataType(primitiveType)));
                    }

                });
        return Collections.unmodifiableMap(map);
    }

    private static RDFDatatype getDataType(String primitiveType) {
        switch (primitiveType) {
            case "Base64Binary":
                return XSDDatatype.XSDbase64Binary;
            case "Boolean":
                return XSDDatatype.XSDboolean;
            case "Byte":
                return XSDDatatype.XSDbyte;
            case "Date":
                return XSDDatatype.XSDdate;
            case "DateTime":
                return XSDDatatype.XSDdateTime;
            case "DateTimeStamp":
                return XSDDatatype.XSDdateTimeStamp;
            case "Day":
                return XSDDatatype.XSDgDay;
            case "DayTimeDuration":
                return XSDDatatype.XSDdayTimeDuration;
            case "Decimal":
                return XSDDatatype.XSDdecimal;
            case "Double":
                return XSDDatatype.XSDdouble;
            case "Duration":
                return XSDDatatype.XSDduration;
            case "Float":
                return XSDDatatype.XSDfloat;
            case "HexBinary":
                return XSDDatatype.XSDhexBinary;
            case "Int":
                return XSDDatatype.XSDint;
            case "Integer":
                return XSDDatatype.XSDinteger;
            case "LangString":
                return RDFLangString.rdfLangString;
            case "Long":
                return XSDDatatype.XSDlong;
            case "Month":
                return XSDDatatype.XSDgMonth;
            case "MonthDay":
                return XSDDatatype.XSDgMonthDay;
            case "NegativeInteger":
                return XSDDatatype.XSDnegativeInteger;
            case "NonNegativeInteger":
                return XSDDatatype.XSDnonNegativeInteger;
            case "NonPositiveInteger":
                return XSDDatatype.XSDnonPositiveInteger;
            case "PositiveInteger":
                return XSDDatatype.XSDpositiveInteger;
            case "StringIRI":
                return XSDDatatype.XSDstring;
            case "Time":
                return XSDDatatype.XSDtime;
            case "UnsignedByte":
                return XSDDatatype.XSDunsignedByte;
            case "UnsignedInt":
                return XSDDatatype.XSDunsignedInt;
            case "UnsignedLong":
                return XSDDatatype.XSDunsignedLong;
            case "UnsignedShort":
                return XSDDatatype.XSDunsignedShort;
            case "URI":
                return XSDDatatype.XSDanyURI;
            case "Year":
                return XSDDatatype.XSDgYear;
            case "YearMonth":
                return XSDDatatype.XSDgYearMonth;
            case "YearMonthDuration":
                return XSDDatatype.XSDyearMonthDuration;
            default:
                throw new IllegalArgumentException("The type '" + primitiveType + "' is not yet supported.");
        }
    }

    private static class StreamTypedTriples implements StreamRDF {
        private final Map<Node, ParsingDirective> typedProperties;
        private final Graph sink;

        public StreamTypedTriples(Graph sink, Map<Node, ParsingDirective> typedProperties) {
            this.sink = sink;
            this.typedProperties = typedProperties;
        }

        @Override
        public void start() {

        }

        @Override
        public void triple(Triple triple) {
            if (triple.getObject().isLiteral()) {
                var parsingDirective = typedProperties.get(triple.getPredicate());
                if (parsingDirective != null) {
                    if(parsingDirective.isIRI) {
                        if(!triple.getObject().isURI()) {
                            triple = Triple.create(
                                    triple.getSubject(),
                                    triple.getPredicate(),
                                    NodeFactory.createURI(triple.getObject().getLiteralLexicalForm()));
                        }
                    } else {
                        triple = Triple.create(
                                triple.getSubject(),
                                triple.getPredicate(),
                                NodeFactory.createLiteral(triple.getObject().getLiteralLexicalForm(),
                                        parsingDirective.datatype));
                    }
                }
            }
            sink.add(triple);
        }

        @Override
        public void quad(Quad quad) {
            throw new NotImplementedException();
        }

        @Override
        public void base(String base) {

        }

        @Override
        public void prefix(String prefix, String iri) {

        }

        @Override
        public void finish() {

        }
    }

}

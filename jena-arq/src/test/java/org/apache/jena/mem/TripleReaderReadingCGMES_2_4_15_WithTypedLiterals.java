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

package org.apache.jena.mem;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.GraphMem2Fast;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.Query;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.core.DatasetGraphMapLink;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.exec.QueryExecDataset;
import org.apache.jena.sparql.graph.GraphFactory;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TripleReaderReadingCGMES_2_4_15_WithTypedLiterals {

    public record TriplesAndNamespaces(Collection<Triple> triples, PrefixMapping prefixMapping) {
        static TriplesAndNamespaces createEmpty() {
            return new TriplesAndNamespaces(new ArrayList<>(), PrefixMapping.Factory.create());
        }
    };

    private static final ConcurrentMap<String, Map<URI, RDFDatatype>> typedPropertiesBySchemaUri = new ConcurrentHashMap<>();

    private final static Query query = new ParameterizedSparqlString("""
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
            """).asQuery();

    private static String getRDFSchemaUri(String graphUri) {
        if (graphUri.endsWith("_EQ.xml")) {
            return "C:/rd/CGMES/ENTSOE_CGMES_v2.4.15_04Jul2016_RDFS/EquipmentProfileCoreRDFSAugmented-v2_4_15-4Jul2016.rdf";
        } else if (graphUri.endsWith("_SSH.xml")) {
            return "C:/rd/CGMES/ENTSOE_CGMES_v2.4.15_04Jul2016_RDFS/SteadyStateHypothesisProfileRDFSAugmented-v2_4_15-16Feb2016.rdf";
        } else if (graphUri.endsWith("_TP.xml")) {
            return "C:/rd/CGMES/ENTSOE_CGMES_v2.4.15_04Jul2016_RDFS/TopologyProfileRDFSAugmented-v2_4_15-16Feb2016.rdf";
        } else if (graphUri.endsWith("_SV.xml")) {
            return "C:/rd/CGMES/ENTSOE_CGMES_v2.4.15_04Jul2016_RDFS/StateVariablesProfileRDFSAugmented-v2_4_15-16Feb2016.rdf";
        }
        return null;
    }

    public static void read(String graphUri, Graph targetGraph) {
        read(graphUri, targetGraph, true, false);
    }

    public static void read(String graphUri, Graph targetGraph, boolean checking, boolean canonicalValues) {
        var rdfSchemaUri = getRDFSchemaUri(graphUri);
        if (rdfSchemaUri != null) {
            read(graphUri, rdfSchemaUri, Lang.RDFXML, targetGraph, checking, canonicalValues);
        } else {
            RDFDataMgr.read(targetGraph, graphUri);
        }
    }


    public static TriplesAndNamespaces read(String graphUri) {
        var rdfSchemaUri = getRDFSchemaUri(graphUri);
        if (rdfSchemaUri != null) {
            return read(graphUri, rdfSchemaUri, Lang.RDFXML);
        } else {
            final var triples = new ArrayList<Triple>();
            final var loadingGraph = new GraphMem2Fast() {
                @Override
                public void performAdd(Triple t) {
                    triples.add(t);
                }
            };
            RDFDataMgr.read(loadingGraph, graphUri);
            return new TriplesAndNamespaces(triples, loadingGraph.getPrefixMapping());
        }
    }

    public static TriplesAndNamespaces read(String graphUri, String rdfSchemaUri, Lang lang) {
        return read(graphUri, rdfSchemaUri, lang, true, false);
    }

    public static TriplesAndNamespaces read(String graphUri, String rdfSchemaUri, Lang lang, boolean checking, boolean canonicalValues) {
        final var triples = TriplesAndNamespaces.createEmpty();
        final var streamSink = new StreamTypedTriplesToList(triples, getOrInitTypedProperties(rdfSchemaUri));
        parseRDF(streamSink, graphUri, lang, checking, canonicalValues);
        return streamSink.getTriplesAndNamespaces();
    }

    public static void read(String graphUri, String rdfSchemaUri, Lang lang, Graph targetGraph) {
        read(graphUri, rdfSchemaUri, lang, targetGraph, true, false);
    }

    public static void read(String graphUri, String rdfSchemaUri, Lang lang, Graph targetGraph, boolean checking, boolean canonicalValues) {
        var streamSink = new StreamTypedTriples(targetGraph, getOrInitTypedProperties(rdfSchemaUri));
        parseRDF(streamSink, graphUri, lang, checking, canonicalValues);
    }

    private static Map<URI, RDFDatatype> getOrInitTypedProperties(String rdfSchemaUri) {
        var typedProperties = typedPropertiesBySchemaUri.get(rdfSchemaUri);
        if (typedProperties == null) {
            typedProperties = getTypedProperties(rdfSchemaUri);
            typedPropertiesBySchemaUri.put(rdfSchemaUri, typedProperties);
        }
        return typedProperties;
    }

    private static void parseRDF(StreamRDF streamSink, String graphUri, Lang lang, boolean checking, boolean canonicalValues) {
        RDFParser.create()
                .source(graphUri)
                .base("xx:")
                .forceLang(lang)
                .checking(checking)
                .canonicalValues(canonicalValues)
                .parse(streamSink);
    }

    private static Map<URI, RDFDatatype> getTypedProperties(String rdfSchemaUri) {
        var g = GraphFactory.createGraphMem();
        RDFDataMgr.read(g, rdfSchemaUri);
        var dataset = new DatasetGraphMapLink(g);
        var rowSet = QueryExecDataset.newBuilder().query(query).dataset(dataset).build().select();
        var map = new HashMap<URI, RDFDatatype>();
        rowSet.forEach(vars -> {
            map.put(URI.create(vars.get("property").getURI()),
                    getDataType(vars.get("primitiveType").getLiteralLexicalForm()));
            if(vars.get("property").getURI().startsWith("http://iec.ch/TC57/2013/CIM-schema-cim16#")) {
                // add also the CIM100 version of the property
                map.put(URI.create(vars.get("property").getURI().replace("http://iec.ch/TC57/2013/CIM-schema-cim16#", "http://iec.ch/TC57/CIM100#")),
                        getDataType(vars.get("primitiveType").getLiteralLexicalForm()));
            }
        });
        return map;
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
        private final Map<URI, RDFDatatype> typedProperties;
        private final Graph sink;

        public StreamTypedTriples(Graph sink, Map<URI, RDFDatatype> typedProperties) {
            this.sink = sink;
            this.typedProperties = typedProperties;
            this.sink.getPrefixMapping().setNsPrefix("xs", "http://www.w3.org/2001/XMLSchema#");
        }

        @Override
        public void start() {

        }

        @Override
        public void triple(Triple triple) {
            final Node subject;
            if(triple.getSubject().getURI().startsWith("xx:#_")) {
                if(triple.getSubject().getURI().length() == 37) {
                    StringBuilder sb = new StringBuilder(45);
                    sb.append("urn:uuid:");
                    sb.append(triple.getSubject().getURI().substring(5, 13));
                    sb.append("-");
                    sb.append(triple.getSubject().getURI().substring( 13, 17));
                    sb.append("-");
                    sb.append(triple.getSubject().getURI().substring( 17, 21));
                    sb.append("-");
                    sb.append(triple.getSubject().getURI().substring( 21, 25));
                    sb.append("-");
                    sb.append(triple.getSubject().getURI().substring( 25, 37));
                    subject = NodeFactory.createURI(sb.toString());
                } else {
                    System.err.println("Incorrect URI: " + triple.getSubject().getURI());
                    subject = NodeFactory.createURI("urn:uuid:" + triple.getSubject().getURI().substring(5));
                }
            } else {
                subject = triple.getSubject();
            }
            final Node object;
            if (triple.getObject().isLiteral()) {
                var dType = typedProperties.get(URI.create(triple.getPredicate().getURI()));
                object = dType != null
                        ? NodeFactory.createLiteral(triple.getObject().getLiteralLexicalForm(), dType)
                        : triple.getObject();
            } else {
                object = triple.getObject();
            }
            sink.add(Triple.create(subject, triple.getPredicate(), object));
        }

        @Override
        public void quad(Quad quad) {
            throw new NotImplementedException();
        }

        @Override
        public void base(String base) {
            sink.getPrefixMapping().setNsPrefix("", base);
        }

        @Override
        public void prefix(String prefix, String iri) {
            sink.getPrefixMapping().setNsPrefix(prefix, iri);
        }

        @Override
        public void finish() {

        }
    }

    private static class StreamTypedTriplesToList implements StreamRDF {
        private final Map<URI, RDFDatatype> typedProperties;
        private final TriplesAndNamespaces sink;

        public StreamTypedTriplesToList(TriplesAndNamespaces sink, Map<URI, RDFDatatype> typedProperties) {
            this.sink = sink;
            this.typedProperties = typedProperties;
            this.sink.prefixMapping.setNsPrefix("xs", "http://www.w3.org/2001/XMLSchema#");
        }

        public TriplesAndNamespaces getTriplesAndNamespaces() {
            return this.sink;
        }

        @Override
        public void start() {

        }

        @Override
        public void triple(Triple triple) {
            final Node subject;
            if(triple.getSubject().getURI().startsWith("xx:#_")) {
                if(triple.getSubject().getURI().length() == 37) {
                    StringBuilder sb = new StringBuilder(45);
                    sb.append("urn:uuid:");
                    sb.append(triple.getSubject().getURI().substring(5, 13));
                    sb.append("-");
                    sb.append(triple.getSubject().getURI().substring( 13, 17));
                    sb.append("-");
                    sb.append(triple.getSubject().getURI().substring( 17, 21));
                    sb.append("-");
                    sb.append(triple.getSubject().getURI().substring( 21, 25));
                    sb.append("-");
                    sb.append(triple.getSubject().getURI().substring( 25, 37));
                    subject = NodeFactory.createURI(sb.toString());
                } else {
                    System.err.println("Incorrect URI: " + triple.getSubject().getURI());
                    subject = NodeFactory.createURI("urn:uuid:" + triple.getSubject().getURI().substring(5));
                }
            } else {
                subject = triple.getSubject();
            }
            final Node object;
            if (triple.getObject().isLiteral()) {
                var dType = typedProperties.get(URI.create(triple.getPredicate().getURI()));
                object = dType != null
                    ? NodeFactory.createLiteral(triple.getObject().getLiteralLexicalForm(), dType)
                    : triple.getObject();
            } else {
                object = triple.getObject();
            }
            sink.triples.add(Triple.create(subject, triple.getPredicate(), object));
        }

        @Override
        public void quad(Quad quad) {
            throw new NotImplementedException();
        }

        @Override
        public void base(String base) {
            sink.prefixMapping.setNsPrefix("", base);
        }

        @Override
        public void prefix(String prefix, String iri) {
            sink.prefixMapping.setNsPrefix(prefix, iri);
        }

        @Override
        public void finish() {

        }
    }
}

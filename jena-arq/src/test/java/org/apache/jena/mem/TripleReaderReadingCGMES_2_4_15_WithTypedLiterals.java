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
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.Query;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.DatasetGraphMapLink;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.exec.QueryExecDataset;
import org.apache.jena.sparql.graph.GraphFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class TripleReaderReadingCGMES_2_4_15_WithTypedLiterals {

    private static final ConcurrentMap<String, Map<URI, RDFDatatype>> typedPropertiesBySchemaUri = new ConcurrentHashMap<>();

    private final static Query query = new ParameterizedSparqlString("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
            "PREFIX cims: <http://iec.ch/TC57/1999/rdf-schema-extensions-19990926#>\n" +
            "\n" +
            "SELECT ?property ?primitiveType\n" +
            "WHERE {\n" +
            "\t ?property cims:dataType ?dataType.\n" +
            "\t\t{\n" +
            "\t\t\t?dataType cims:stereotype \"CIMDatatype\".\n" +
            "\t\t\t[] rdfs:domain ?dataType;\n" +
            "\t\t\t   cims:dataType/cims:stereotype \"Primitive\";\n" +
            "\t\t\t   cims:dataType/rdfs:label ?primitiveType\n" +
            "\t\t\t   FILTER (str(?primitiveType) != \"String\")\n" +
            "\t\t}\n" +
            "\t\tUNION\n" +
            "\t\t{\n" +
            "\t\t\t?dataType cims:stereotype \"Primitive\";\n" +
            "\t\t\t\trdfs:label ?primitiveType.\n" +
            "\t\t\t FILTER (str(?primitiveType) != \"String\")\n" +
            "\t\t}\n" +
            "}").asQuery();

    private static String getRDFSchemaUri(String graphUri) {
        if (graphUri.endsWith("_EQ.xml")) {
            return "C:/rd/CGMES/ENTSOE_CGMES_v2.4.15_04Jul2016_RDFS/EquipmentProfileCoreRDFSAugmented-v2_4_15-4Jul2016.rdf";
        } else if (graphUri.endsWith("_SSH.xml")) {
            return "C:/rd/CGMES/ENTSOE_CGMES_v2.4.15_04Jul2016_RDFS/SteadyStateHypothesisProfileRDFSAugmented-v2_4_15-16Feb2016.rdf";
        }
        if (graphUri.endsWith("_TP.xml")) {
            return "C:/rd/CGMES/ENTSOE_CGMES_v2.4.15_04Jul2016_RDFS/TopologyProfileRDFSAugmented-v2_4_15-16Feb2016.rdf";
        } else if (graphUri.endsWith("_SV.xml")) {
            return "C:/rd/CGMES/ENTSOE_CGMES_v2.4.15_04Jul2016_RDFS/StateVariablesProfileRDFSAugmented-v2_4_15-16Feb2016.rdf";
        }
        return null;
    }

    public static void read(String graphUri, Graph targetGraph) {
        var rdfSchemaUri = getRDFSchemaUri(graphUri);
        if (rdfSchemaUri != null) {
            read(graphUri, rdfSchemaUri, Lang.RDFXML, targetGraph);
        } else {
            RDFDataMgr.read(targetGraph, graphUri);
        }
    }


    public static List<Triple> read(String graphUri) {
        var rdfSchemaUri = getRDFSchemaUri(graphUri);
        if (rdfSchemaUri != null) {
            return read(graphUri, rdfSchemaUri, Lang.RDFXML);
        } else {
            var triples = new ArrayList<Triple>();
            var loadingGraph = new GraphMem() {
                @Override
                public void performAdd(Triple t) {
                    triples.add(t);
                }
            };
            RDFDataMgr.read(loadingGraph, graphUri);
            return triples;
        }
    }

    public static List<Triple> read(String graphUri, String rdfSchemaUri, Lang lang) {
        var triples = new ArrayList<Triple>();
        var streamSink = new StreamTypedTriplesToList(triples, getOrInitTypedProperties(rdfSchemaUri));
        parseRDF(streamSink, graphUri, lang);
        return streamSink.getTriples();
    }

    public static void read(String graphUri, String rdfSchemaUri, Lang lang, Graph targetGraph) {
        var streamSink = new StreamTypedTriples(targetGraph, getOrInitTypedProperties(rdfSchemaUri));
        parseRDF(streamSink, graphUri, lang);
    }

    private static Map<URI, RDFDatatype> getOrInitTypedProperties(String rdfSchemaUri) {
        var typedProperties = typedPropertiesBySchemaUri.get(rdfSchemaUri);
        if (typedProperties == null) {
            typedProperties = getTypedProperties(rdfSchemaUri);
            typedPropertiesBySchemaUri.put(rdfSchemaUri, typedProperties);
        }
        return typedProperties;
    }

    private static void parseRDF(StreamRDF streamSink, String graphUri, Lang lang) {
        RDFParser.create()
                .source(graphUri)
                .base("x:")
                .lang(lang)
                .parse(streamSink);
    }

    private static Map<URI, RDFDatatype> getTypedProperties(String rdfSchemaUri) {
        var g = GraphFactory.createGraphMem();
        RDFDataMgr.read(g, rdfSchemaUri);
        var dataset = new DatasetGraphMapLink(g);
        var rowSet = QueryExecDataset.newBuilder().query(query).dataset(dataset).build().select();
        return rowSet.stream().collect(Collectors.toMap(
                vars -> URI.create(vars.get("property").getURI().replace("http://iec.ch/TC57/2013/CIM-schema-cim16#", "http://iec.ch/TC57/CIM100#")),
                vars -> getDataType(vars.get("primitiveType").getLiteralLexicalForm())));
    }

    private static RDFDatatype getDataType(String primitiveType) {
        switch (primitiveType) {
            case "Vase64Binary":
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
        }

        @Override
        public void start() {

        }

        @Override
        public void triple(Triple triple) {
            if (triple.getObject().isLiteral()) {
                var dType = typedProperties.get(URI.create(triple.getPredicate().getURI()));
                if (dType != null) {
                    sink.add(Triple.create(
                            triple.getSubject(),
                            triple.getPredicate(),
                            NodeFactory.createLiteral(triple.getObject().getLiteralLexicalForm(), dType)));
                } else {
                    sink.add(triple);
                }
            } else {
                sink.add(triple);
            }
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

    private static class StreamTypedTriplesToList implements StreamRDF {
        private final Map<URI, RDFDatatype> typedProperties;
        private final List<Triple> sink;

        public StreamTypedTriplesToList(List<Triple> sink, Map<URI, RDFDatatype> typedProperties) {
            this.sink = sink;
            this.typedProperties = typedProperties;
        }

        public List<Triple> getTriples() {
            return this.sink;
        }

        @Override
        public void start() {

        }

        @Override
        public void triple(Triple triple) {
            if (triple.getObject().isLiteral()) {
                var dType = typedProperties.get(URI.create(triple.getPredicate().getURI()));
                if (dType != null) {
                    sink.add(Triple.create(triple.getSubject(),
                            triple.getPredicate(),
                            NodeFactory.createLiteral(triple.getObject().getLiteralLexicalForm(), triple.getObject().getLiteralLanguage(), dType)));
                } else {
                    sink.add(triple);
                }
            } else {
                sink.add(triple);
            }
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

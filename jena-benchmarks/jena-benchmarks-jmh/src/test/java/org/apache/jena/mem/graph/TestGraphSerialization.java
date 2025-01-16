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

package org.apache.jena.mem.graph;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Graph;
import org.apache.jena.mem.TripleReaderReadingCGMES_2_4_15_WithTypedLiterals;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.mem.graph.helper.Serialization;
import org.apache.jena.mem2.GraphMem2Fast;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.*;
import org.apache.jena.shacl.ShaclValidator;
import org.apache.jena.shacl.Shapes;
import org.apache.jena.shacl.lib.ShLib;
import org.apache.jena.vocabulary.RDF;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;


@State(Scope.Benchmark)
public class TestGraphSerialization {

    @Param({
//            "../testing/cheeses-0.1.ttl",
//            "../testing/pizza.owl.rdf",
//            "C:/temp/res_test/xxx_CGMES_EQ.xml",
            "C:/temp/res_test/xxx_CGMES_SSH.xml",
//            "C:/temp/res_test/xxx_CGMES_TP.xml",
//            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml",
//            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml",
//            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml",
//            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml",
//            "../testing/BSBM/bsbm-1m.nt.gz",
//            "../testing/BSBM/bsbm-5m.nt.gz",
//            "../testing/BSBM/bsbm-25m.nt.gz",
    })
    public String param0_GraphUri;
    @Param({
//            "TURTLE_PRETTY",
//            "TURTLE_BLOCKS",
//            "TURTLE_FLAT",
//            "TURTLE_LONG",
//            "NTRIPLES_UTF8",
//            "NQUADS_UTF8",
//            "TRIG_PRETTY",
//            "TRIG_BLOCKS",
//            "TRIG_FLAT",
//            "TRIG_LONG",
//            "JSONLD11_PRETTY",
//            "JSONLD11_PLAIN",
//            "JSONLD11_FLAT",
            "RDFXML_PRETTY",
//            "RDFXML_PLAIN",
//            "RDFJSON",
//            "TRIX",
//            "RDF_PROTO",
//            "RDF_PROTO_VALUES",
            "RDF_THRIFT",
            "RDF_THRIFT_VALUES"
    })
    public String param1_RDFFormat;
    @Param({
//            Serialization.NO_COMPRESSOR,
            Serialization.LZ4_FASTEST,
            Serialization.GZIP
    })
    public String param2_Compressor;

    @Param({
            "true",
            "false"
    })
    public String param3_canonicalValues;

    private boolean canonicalValues;

    private Graph graphToSerialize;
    private Serialization.SerializedData serializedGraph;
    private RDFFormat rdfFormat;



    private static RDFFormat getRDFFormat(String rdfFormat) {
        switch (rdfFormat) {
            case "TURTLE_PRETTY":
                return RDFFormat.TURTLE_PRETTY;
            case "TURTLE_BLOCKS":
                return RDFFormat.TURTLE_BLOCKS;
            case "TURTLE_FLAT":
                return RDFFormat.TURTLE_FLAT;
            case "TURTLE_LONG":
                return RDFFormat.TURTLE_LONG;
            case "NTRIPLES_UTF8":
                return RDFFormat.NTRIPLES_UTF8;
            case "NQUADS_UTF8":
                return RDFFormat.NQUADS_UTF8;
            case "TRIG_PRETTY":
                return RDFFormat.TRIG_PRETTY;
            case "TRIG_BLOCKS":
                return RDFFormat.TRIG_BLOCKS;
            case "TRIG_FLAT":
                return RDFFormat.TRIG_FLAT;
            case "TRIG_LONG":
                return RDFFormat.TRIG_LONG;
            case "JSONLD11_PRETTY":
                return RDFFormat.JSONLD11_PRETTY;
            case "JSONLD11_PLAIN":
                return RDFFormat.JSONLD11_PLAIN;
            case "JSONLD11_FLAT":
                return RDFFormat.JSONLD11_FLAT;
            case "RDFXML_PRETTY":
                return RDFFormat.RDFXML_PRETTY;
            case "RDFXML_PLAIN":
                return RDFFormat.RDFXML_PLAIN;
            case "RDFJSON":
                return RDFFormat.RDFJSON;
            case "TRIX":
                return RDFFormat.TRIX;
            case "RDF_PROTO":
                return RDFFormat.RDF_PROTO;
            case "RDF_PROTO_VALUES":
                return RDFFormat.RDF_PROTO_VALUES;
            case "RDF_THRIFT":
                return RDFFormat.RDF_THRIFT;
            case "RDF_THRIFT_VALUES":
                return RDFFormat.RDF_THRIFT_VALUES;
            default:
                throw new IllegalArgumentException("Unknown rdfFormat: " + rdfFormat);
        }
    }

    @Test
    public void writeGraphWithTypedLiterals() {
        var triplesAndNamespaces = TripleReaderReadingCGMES_2_4_15_WithTypedLiterals.read("C:/temp/res_test/xxx_CGMES_SSH.xml");
        var g = new GraphMem2Fast();
        g.getPrefixMapping().setNsPrefixes(triplesAndNamespaces.prefixMapping());
        triplesAndNamespaces.triples().forEach(g::add);
        RDFWriter.source(g).format(RDFFormat.RDFXML).output("c:/temp/xxx_CGMES_SSH.rdf");
    }

    @Test
    public void readGraph() {
        var g = RDFDataMgr.loadGraph("C:\\temp\\xxx_CGMES_SSH1.rdf");
        int i=0;
    }

    @Test
    public void testValidateSHACL() {
        var dataGraph = RDFDataMgr.loadGraph("c:/temp/DataWithTypedLiterals.ttl");
        final var query = """
                PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\s
                PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX xsd:     <http://www.w3.org/2001/XMLSchema#>
                                
                PREFIX sh:      <http://www.w3.org/ns/shacl#>
                PREFIX shx:     <http://www.w3.org/ns/shacl-x#>
                PREFIX amp: <http://amprion.net/CIM/AMP-schema-cim1#>
                                
                amp:myShape\s
                  a sh:NodeShape ;
                  sh:targetClass amp:ACLineSegment ;\s
                  sh:property [
                    sh:path amp:Wire.x ;
                    sh:datatype xsd:float ; \s
                  ] ;
                  .
                """;
        var shapes = buildShapes(query);

        var validationReport = ShaclValidator.get().validate(shapes, dataGraph);

        ShLib.printReport(validationReport);
    }

    private Shapes buildShapes(String Query) {
        final var g = new GraphMem2Fast();
        RDFParser.create().source(new StringReader(Query)).lang(Lang.TTL).parse(g);
        return Shapes.parse(g);
    }


    //@Test
    public void loadSerializeAndDeserialize() {
        //final var rdfFormat = Lang.RDFXML;
        final var g = new GraphMem2Fast();
        TripleReaderReadingCGMES_2_4_15_WithTypedLiterals.read("C:/temp/res_test/xxx_CGMES_SSH.xml", g);
        for (var rdfFormat : List.of(RDFFormat.TURTLE_PRETTY, RDFFormat.TURTLE_BLOCKS, RDFFormat.TURTLE_FLAT, RDFFormat.TURTLE_LONG, RDFFormat.NTRIPLES_UTF8, RDFFormat.NQUADS_UTF8, RDFFormat.TRIG_PRETTY, RDFFormat.TRIG_BLOCKS, RDFFormat.TRIG_FLAT, RDFFormat.TRIG_LONG, RDFFormat.JSONLD11_PRETTY, RDFFormat.JSONLD11_PLAIN, RDFFormat.JSONLD11_FLAT, RDFFormat.RDFXML_PRETTY, RDFFormat.RDFXML_PLAIN, RDFFormat.RDFJSON, RDFFormat.TRIX, RDFFormat.RDF_PROTO, RDFFormat.RDF_PROTO_VALUES, RDFFormat.RDF_THRIFT, RDFFormat.RDF_THRIFT_VALUES)) {
            for (var compressor : List.of(Serialization.NO_COMPRESSOR, Serialization.LZ4_FASTEST, Serialization.GZIP)) {
                var compressedGraph = Serialization.serialize(g, rdfFormat, compressor);
                //print: "Size of output stream in format %rdfFormat% and with compressor %compressor% is xxx.xx MB.
                System.out.printf("Size of output stream in format %20s and with compressor %12s is %6.2f MB.\n", rdfFormat.toString(), compressor, compressedGraph.bytes().length / 1024.0 / 1024.0);
                var g2 = Serialization.deserialize(compressedGraph, false);
                final var tripleWithBooleanObject = g.stream().filter(t -> t.getObject().isLiteral() && t.getObject().getLiteralDatatype() == XSDDatatype.XSDboolean).findFirst().orElseThrow();
                final var tripleWithFloatObject = g.stream().filter(t -> t.getObject().isLiteral() && t.getObject().getLiteralDatatype() == XSDDatatype.XSDfloat).findFirst().orElseThrow();
                if (!g2.contains(tripleWithBooleanObject)) {
                    //print: "The deserialized graph with the format %rdfFormat% does not contain tripleWithBooleanObject."
                    System.out.printf("The deserialized graph with the format %20s does not contain tripleWithBooleanObject.\n", rdfFormat);

                }
                if (!g2.contains(tripleWithFloatObject)) {
                    //print: "The deserialized graph with the format %rdfFormat% does not contain tripleWithFloatObject."
                    System.out.printf("The deserialized graph with the format %20s does not contain tripleWithFloatObject.\n", rdfFormat);
                }
            }
        }
    }

//    @Test
    public void createGraphWithTypedLiteralsAndDetermineIfTypesAreLostInDifferentFormats() {
        var g = new GraphMem2Fast();
        writeDistinctLiteralSamples(g);
        //print: "Original graph with typed literals"
        System.out.println("Original graph with typed literals");
        queryAndPrintDisinctLiteralSample(g);

        {
            //print: "Serialize as RDF_THRIFT and deserialize again (canonicalValues=false)"
            System.out.println("Serialize as RDF_THRIFT and deserialize again (canonicalValues=false)");
            final var deserializedGraph = Serialization.deserialize(Serialization.serialize(g, RDFFormat.RDF_THRIFT, Serialization.LZ4_FASTEST), false);
            queryAndPrintDisinctLiteralSample(deserializedGraph);
        }
        {
            //print: "Serialize as RDF_THRIFT and deserialize again (canonicalValues=false)"
            System.out.println("Serialize as RDF_THRIFT and deserialize again (canonicalValues=true)");
            final var deserializedGraph = Serialization.deserialize(Serialization.serialize(g, RDFFormat.RDF_THRIFT, Serialization.LZ4_FASTEST), true);
            queryAndPrintDisinctLiteralSample(deserializedGraph);
        }
        {
            //print: "Serialize as RDF_THRIFT and deserialize again (canonicalValues=false)"
            System.out.println("Serialize as RDF_THRIFT_VALUES and deserialize again (canonicalValues=false)");
            final var deserializedGraph = Serialization.deserialize(Serialization.serialize(g, RDFFormat.RDF_THRIFT_VALUES, Serialization.LZ4_FASTEST), false);
            queryAndPrintDisinctLiteralSample(deserializedGraph);
        }
        {
            //print: "Serialize as RDF_THRIFT and deserialize again (canonicalValues=false)"
            System.out.println("Serialize as RDF_THRIFT_VALUES and deserialize again (canonicalValues=true)");
            final var deserializedGraph = Serialization.deserialize(Serialization.serialize(g, RDFFormat.RDF_THRIFT_VALUES, Serialization.LZ4_FASTEST), true);
            queryAndPrintDisinctLiteralSample(deserializedGraph);
        }
    }

    private static void queryAndPrintDisinctLiteralSample(final Graph graph) {
        final var model = ModelFactory.createModelForGraph(graph);
        var header = new Boolean[]{true};
        QueryExecutionFactory.create("""
                        PREFIX jena_ex: <http://jena.apache.org/examples/literal-term-equality#>
                        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        SELECT ?s ?booleanValue ?intValue ?floatValue ?doubleValue 
                        WHERE { ?s jena_ex:ExampleObject.booleanValue ?booleanValue;
                                   jena_ex:ExampleObject.intValue ?intValue;
                                   jena_ex:ExampleObject.floatValue ?floatValue;
                                   jena_ex:ExampleObject.doubleValue ?doubleValue.}
                        ORDER BY ?s
                        """, model)
                .execSelect()
                .forEachRemaining(row -> {
                    if (header[0]) {
                        header[0] = false;
                        //print header from vars
                        var varNames = new ArrayList<String>();
                        row.varNames().forEachRemaining(varNames::add);
                        System.out.printf("%-10s %-50s %-50s %-50s %-50s\n", varNames.get(0), varNames.get(1), varNames.get(2), varNames.get(3), varNames.get(4));
                    }
                    //print result as table
                    System.out.printf("%-10s %-50s %-50s %-50s %-50s\n", row.get("s"), row.get("booleanValue"), row.get("intValue"), row.get("floatValue"), row.get("doubleValue"));
                });
    }

//    @Test
    public void queryMixedTermComparisonsGraphMem2() {
        final var model = writeMixedLiteralSamples(new GraphMem2Fast());
        QueryExecutionFactory.create("""
                        PREFIX jena_ex: <http://jena.apache.org/examples/literal-term-equality#>
                        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        SELECT ?s ?value
                        WHERE { ?s jena_ex:ExampleObject.value ?value.}
                        ORDER BY ?s
                        """, model)
                .execSelect()
                .forEachRemaining(row -> {
                    //print result as table
                    System.out.printf("%-10s %-10s\n", row.get("s"), row.get("value"));
                });

        //Query pairs of subjects with the same value
        //print: "Query subject with the same value via term comparison"
        System.out.println("Query subject with the same value via term comparison");
        QueryExecutionFactory.create("""
                        PREFIX jena_ex: <http://jena.apache.org/examples/literal-term-equality#>
                        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        SELECT ?s1 ?s2 ?value
                        WHERE { ?s1 jena_ex:ExampleObject.value ?value.
                                ?s2 jena_ex:ExampleObject.value ?value.
                                FILTER(?s1 != ?s2)}
                        ORDER BY ?s1 ?s2
                        """, model)
                .execSelect()
                .forEachRemaining(row -> {
                    //print result as table
                    System.out.printf("%-10s %-10s %-10s\n", row.get("s1"), row.get("s2"), row.get("value"));
                });

        //print: "Query subject with the same value via FILTER"
        System.out.println("Query subject with the same value via FILTER");
        QueryExecutionFactory.create("""
                        PREFIX jena_ex: <http://jena.apache.org/examples/literal-term-equality#>
                        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        SELECT ?s1 ?s2 ?value1 ?value2
                        WHERE { ?s1 jena_ex:ExampleObject.value ?value1.
                                ?s2 jena_ex:ExampleObject.value ?value2.
                                FILTER(?s1 != ?s2 && ?value1 = ?value2)}
                        ORDER BY ?s1 ?s2
                        """, model)
                .execSelect()
                .forEachRemaining(row -> {
                    //print result as table
                    System.out.printf("%-10s %-10s %-10s %-10s\n", row.get("s1"), row.get("s2"), row.get("value1"), row.get("value2"));
                });
    }

//    @Test
    public void queryBooleanTermComparisonsGraphMem2() {
        final var model = writeDistinctLiteralSamples(new GraphMem2Fast());
        QueryExecutionFactory.create("""
                        PREFIX jena_ex: <http://jena.apache.org/examples/literal-term-equality#>
                        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        SELECT ?s ?booleanValue ?intValue ?floatValue ?doubleValue 
                        WHERE { ?s jena_ex:ExampleObject.booleanValue ?booleanValue;
                                   jena_ex:ExampleObject.intValue ?intValue;
                                   jena_ex:ExampleObject.floatValue ?floatValue;
                                   jena_ex:ExampleObject.doubleValue ?doubleValue.}
                        ORDER BY ?s
                        """, model)
                .execSelect()
                .forEachRemaining(row -> {
                    //print result as table
                    System.out.printf("%-10s %-10s %-10s %-10s %-10s\n", row.get("s"), row.get("booleanValue"), row.get("intValue"), row.get("floatValue"), row.get("doubleValue"));
                });

        //Query pairs of subjects with the same booleanValue
        //print: "Query subjects with booleanValue true and union boolean value false"
        System.out.println("Query subjects with booleanValue true and union boolean value false");
        QueryExecutionFactory.create("""
                        PREFIX jena_ex: <http://jena.apache.org/examples/literal-term-equality#>
                        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        SELECT ?s1 ?booleanValue
                        WHERE {
                            { BIND(true AS ?booleanValue)
                              ?s1 jena_ex:ExampleObject.booleanValue true.}
                            UNION
                            { BIND(false AS ?booleanValue)
                              ?s1 jena_ex:ExampleObject.booleanValue false.}
                        } 
                        """, model)
                .execSelect()
                .forEachRemaining(row -> {
                    //print result as table
                    System.out.printf("%-10s %-10s\n", row.get("s1"), row.get("booleanValue"));
                });

        //Query pairs of subjects with the same booleanValue
        //print: "Query subjects FILTERED by booleanValue true"
        System.out.println("Query subjects filtered by booleanValue true");
        QueryExecutionFactory.create("""
                        PREFIX jena_ex: <http://jena.apache.org/examples/literal-term-equality#>
                        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        SELECT ?s1 ?booleanValue
                        WHERE {
                              ?s1 jena_ex:ExampleObject.booleanValue ?booleanValue.
                               FILTER(?booleanValue = true)}
                        ORDER BY ?s1
                        """, model)
                .execSelect()
                .forEachRemaining(row -> {
                    //print result as table
                    System.out.printf("%-10s %-10s\n", row.get("s1"), row.get("booleanValue"));
                });

        //print: "Query pairs of subjects with the same booleanValue comparing the terms"
        System.out.println("Query pairs of subjects with the same booleanValue comparing the terms");
        QueryExecutionFactory.create("""
                        PREFIX jena_ex: <http://jena.apache.org/examples/literal-term-equality#>
                        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        SELECT ?s1 ?s2 ?booleanValue
                        WHERE { ?s1 jena_ex:ExampleObject.booleanValue ?booleanValue.
                                ?s2 jena_ex:ExampleObject.booleanValue ?booleanValue.
                                FILTER(?s1 != ?s2)}
                        ORDER BY ?s1 ?s2
                        """, model)
                .execSelect()
                .forEachRemaining(row -> {
                    //print result as table
                    System.out.printf("%-10s %-10s %-10s\n", row.get("s1"), row.get("s2"), row.get("booleanValue"));
                });

        //print: "Query pairs of subjects with the same booleanValue using FILTER"
        System.out.println("Query pairs of subjects with the same booleanValue using FILTER");
        QueryExecutionFactory.create("""
                        PREFIX jena_ex: <http://jena.apache.org/examples/literal-term-equality#>
                        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        SELECT ?s1 ?s2 ?booleanValue1 ?booleanValue2
                        WHERE { ?s1 jena_ex:ExampleObject.booleanValue ?booleanValue1.
                                ?s2 jena_ex:ExampleObject.booleanValue ?booleanValue2.
                                FILTER(?s1 != ?s2 && ?booleanValue1 = ?booleanValue2)}
                        ORDER BY ?s1 ?s2
                        """, model)
                .execSelect()
                .forEachRemaining(row -> {
                    //print result as table
                    System.out.printf("%-10s %-10s %-10s %-10s\n", row.get("s1"), row.get("s2"), row.get("booleanValue1"), row.get("booleanValue2"));
                });
    }

//    @Test
    public void queryIntegerTermComparisonsGraphMem2() {
        var model = writeDistinctLiteralSamples(new GraphMem2Fast());
        QueryExecutionFactory.create("""
                        PREFIX jena_ex: <http://jena.apache.org/examples/literal-term-equality#>
                        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        SELECT ?s ?booleanValue ?intValue ?floatValue ?doubleValue 
                        WHERE { ?s jena_ex:ExampleObject.booleanValue ?booleanValue;
                                   jena_ex:ExampleObject.intValue ?intValue;
                                   jena_ex:ExampleObject.floatValue ?floatValue;
                                   jena_ex:ExampleObject.doubleValue ?doubleValue.}
                        ORDER BY ?s
                        """, model)
                .execSelect()
                .forEachRemaining(row -> {
                    //print result as table
                    System.out.printf("%-10s %-10s %-10s %-10s %-10s\n", row.get("s"), row.get("booleanValue"), row.get("intValue"), row.get("floatValue"), row.get("doubleValue"));
                });

        //Query pairs of subjects with the same intValue
        //print: "Query subjects with intValue 1 and union intValue 0"
        System.out.println("Query subjects with intValue 1 and union intValue 0");
        QueryExecutionFactory.create("""
                        PREFIX jena_ex: <http://jena.apache.org/examples/literal-term-equality#>
                        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        SELECT ?s1 ?intValue
                        WHERE {
                            { BIND(1 AS ?intValue)
                              ?s1 jena_ex:ExampleObject.intValue 1.}
                            UNION
                            { BIND(0 AS ?intValue)
                              ?s1 jena_ex:ExampleObject.intValue 0.}
                        } 
                        """, model)
                .execSelect()
                .forEachRemaining(row -> {
                    //print result as table
                    System.out.printf("%-10s %-10s\n", row.get("s1"), row.get("intValue"));
                });

        //print: "Query pairs of subjects with the same intValue comparing the terms"
        System.out.println("Query pairs of subjects with the same intValue comparing the terms");
        QueryExecutionFactory.create("""
                        PREFIX jena_ex: <http://jena.apache.org/examples/literal-term-equality#>
                        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        SELECT ?s1 ?s2 ?intValue
                        WHERE { ?s1 jena_ex:ExampleObject.intValue ?intValue.
                                ?s2 jena_ex:ExampleObject.intValue ?intValue.
                                FILTER(!sameTerm(?s1, ?s2))}
                        ORDER BY ?s1 ?s2
                        """, model)
                .execSelect()
                .forEachRemaining(row -> {
                    //print result as table
                    System.out.printf("%-10s %-10s %-10s\n", row.get("s1"), row.get("s2"), row.get("intValue"));
                });

        //print: "Query pairs of subjects with the same intValue using FILTER"
        System.out.println("Query pairs of subjects with the same intValue using FILTER");
        QueryExecutionFactory.create("""
                        PREFIX jena_ex: <http://jena.apache.org/examples/literal-term-equality#>
                        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        SELECT ?s1 ?s2 ?intValue1 ?intValue2
                        WHERE { ?s1 jena_ex:ExampleObject.intValue ?intValue1.
                                ?s2 jena_ex:ExampleObject.intValue ?intValue2.
                                FILTER(?s1 != ?s2 && ?intValue1 = ?intValue2)}
                        ORDER BY ?s1 ?s2
                        """, model)
                .execSelect()
                .forEachRemaining(row -> {
                    //print result as table
                    System.out.printf("%-10s %-10s %-10s %-10s\n", row.get("s1"), row.get("s2"), row.get("intValue1"), row.get("intValue2"));
                });
    }

//    @Test
    public void queryFloatTermComparisonsGraphMem2() {
        var model = writeDistinctLiteralSamples(new GraphMem2Fast());
        QueryExecutionFactory.create("""
                        PREFIX jena_ex: <http://jena.apache.org/examples/literal-term-equality#>
                        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        SELECT ?s ?booleanValue ?intValue ?floatValue ?doubleValue 
                        WHERE { ?s jena_ex:ExampleObject.booleanValue ?booleanValue;
                                   jena_ex:ExampleObject.intValue ?intValue;
                                   jena_ex:ExampleObject.floatValue ?floatValue;
                                   jena_ex:ExampleObject.doubleValue ?doubleValue.}
                        ORDER BY ?s
                        """, model)
                .execSelect()
                .forEachRemaining(row -> {
                    //print result as table
                    System.out.printf("%-10s %-10s %-10s %-10s %-10s\n", row.get("s"), row.get("booleanValue"), row.get("intValue"), row.get("floatValue"), row.get("doubleValue"));
                });

        //Query pairs of subjects with the same floatValue
        //print: "Query subjects with floatValue 1 and union floatValue 0"
        // Attention: in SPARQL is no literal syntax for xsd:float, so the query is not correct. 1.0 translates to xsd:decimal
        System.out.println("Query subjects with intValue 1 and union intValue 0");
        QueryExecutionFactory.create("""
                        PREFIX jena_ex: <http://jena.apache.org/examples/literal-term-equality#>
                        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        SELECT ?s1 ?intValue
                        WHERE {
                            { BIND(1 AS ?intValue)
                              ?s1 jena_ex:ExampleObject.floatValue 1.0 . }
                            UNION
                            { BIND(0 AS ?intValue)
                              ?s1 jena_ex:ExampleObject.floatValue 0.0 .}
                        } 
                        """, model)
                .execSelect()
                .forEachRemaining(row -> {
                    //print result as table
                    System.out.printf("%-10s %-10s\n", row.get("s1"), row.get("intValue"));
                });

        //print: "Query pairs of subjects with the same floatValue comparing the terms"
        System.out.println("Query pairs of subjects with the same floatValue comparing the terms");
        QueryExecutionFactory.create("""
                        PREFIX jena_ex: <http://jena.apache.org/examples/literal-term-equality#>
                        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                        SELECT ?s1 ?s2 ?floatValue
                        WHERE { ?s1 jena_ex:ExampleObject.floatValue ?floatValue.
                                ?s2 jena_ex:ExampleObject.floatValue ?floatValue.
                                FILTER(!sameTerm(?s1, ?s2))}
                        ORDER BY ?s1 ?s2
                        """, model)
                .execSelect()
                .forEachRemaining(row -> {
                    //print result as table
                    System.out.printf("%-10s %-10s %-10s\n", row.get("s1"), row.get("s2"), row.get("floatValue"));
                });

        //print: "Query pairs of subjects with the same floatValue using FILTER"
        System.out.println("Query pairs of subjects with the same floatValue using FILTER");
        QueryExecutionFactory.create("""
                        PREFIX jena_ex: <http://jena.apache.org/examples/literal-term-equality#>
                        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                        SELECT ?s1 ?s2 ?floatValue1 ?floatValue2
                        WHERE { ?s1 jena_ex:ExampleObject.floatValue ?floatValue1.
                                ?s2 jena_ex:ExampleObject.floatValue ?floatValue2.
                                FILTER(?s1 != ?s2 && ?floatValue1 = ?floatValue2)}
                        ORDER BY ?s1 ?s2
                        """, model)
                .execSelect()
                .forEachRemaining(row -> {
                    //print result as table
                    System.out.printf("%-10s %-10s %-10s %-10s\n", row.get("s1"), row.get("s2"), row.get("floatValue1"), row.get("floatValue2"));
                });
    }

//    @Test
    public void queryDoubleTermComparisonsGraphMem2() {
        var model = writeDistinctLiteralSamples(new GraphMem2Fast());
        QueryExecutionFactory.create("""
                        PREFIX jena_ex: <http://jena.apache.org/examples/literal-term-equality#>
                        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        SELECT ?s ?booleanValue ?intValue ?floatValue ?doubleValue 
                        WHERE { ?s jena_ex:ExampleObject.booleanValue ?booleanValue;
                                   jena_ex:ExampleObject.intValue ?intValue;
                                   jena_ex:ExampleObject.floatValue ?floatValue;
                                   jena_ex:ExampleObject.doubleValue ?doubleValue.}
                        ORDER BY ?s
                        """, model)
                .execSelect()
                .forEachRemaining(row -> {
                    //print result as table
                    System.out.printf("%-10s %-10s %-10s %-10s %-10s\n", row.get("s"), row.get("booleanValue"), row.get("intValue"), row.get("floatValue"), row.get("doubleValue"));
                });

        //print: "Query pairs of subjects with the same doubleValue comparing the terms"
        System.out.println("Query pairs of subjects with the same doubleValue comparing the terms");
        QueryExecutionFactory.create("""
                        PREFIX jena_ex: <http://jena.apache.org/examples/literal-term-equality#>
                        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                        SELECT ?s1 ?s2 ?doubleValue
                        WHERE { ?s1 jena_ex:ExampleObject.doubleValue ?doubleValue.
                                ?s2 jena_ex:ExampleObject.doubleValue ?doubleValue.
                                FILTER(!sameTerm(?s1, ?s2))}
                        ORDER BY ?s1 ?s2
                        """, model)
                .execSelect()
                .forEachRemaining(row -> {
                    //print result as table
                    System.out.printf("%-10s %-10s %-10s\n", row.get("s1"), row.get("s2"), row.get("doubleValue"));
                });

        //print: "Query pairs of subjects with the same doubleValue using FILTER"
        System.out.println("Query pairs of subjects with the same doubleValue using FILTER");
        QueryExecutionFactory.create("""
                        PREFIX jena_ex: <http://jena.apache.org/examples/literal-term-equality#>
                        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                        SELECT ?s1 ?s2 ?doubleValue1 ?doubleValue2
                        WHERE { ?s1 jena_ex:ExampleObject.floatValue ?doubleValue1.
                                ?s2 jena_ex:ExampleObject.floatValue ?doubleValue2.
                                FILTER(?s1 != ?s2 && ?doubleValue1 = ?doubleValue2)}
                        ORDER BY ?s1 ?s2
                        """, model)
                .execSelect()
                .forEachRemaining(row -> {
                    //print result as table
                    System.out.printf("%-10s %-10s %-10s %-10s\n", row.get("s1"), row.get("s2"), row.get("doubleValue1"), row.get("doubleValue2"));
                });
    }

    private final static String NS = "http://jena.apache.org/examples/literal-term-equality#";

    private Model writeDistinctLiteralSamples(Graph g) {
        final var model = ModelFactory.createModelForGraph(g);
        final var propertyBoolean = model.createProperty(NS + "ExampleObject.booleanValue");
        final var propertyInt = model.createProperty(NS + "ExampleObject.intValue");
        final var propertyFloat = model.createProperty(NS + "ExampleObject.floatValue");
        final var propertyDouble = model.createProperty(NS + "ExampleObject.doubleValue");

        model.createResource("A")
                .addProperty(RDF.type, model.createResource(NS + "ExampleObject"))
                .addProperty(propertyBoolean, "true", XSDDatatype.XSDboolean)
                .addProperty(propertyInt, "1", XSDDatatype.XSDinteger)
                .addProperty(propertyFloat, "1.0", XSDDatatype.XSDfloat)
                .addProperty(propertyDouble, "1.0", XSDDatatype.XSDdouble);
        model.createResource("B")
                .addProperty(RDF.type, model.createResource(NS + "ExampleObject"))
                .addProperty(propertyBoolean, "true", XSDDatatype.XSDboolean)
                .addProperty(propertyInt, "1", XSDDatatype.XSDinteger)
                .addProperty(propertyFloat, "1.0", XSDDatatype.XSDfloat)
                .addProperty(propertyDouble, "1.0", XSDDatatype.XSDdouble);
        model.createResource("C")
                .addProperty(RDF.type, model.createResource(NS + "ExampleObject"))
                .addProperty(propertyBoolean, "1", XSDDatatype.XSDboolean)
                .addProperty(propertyInt, "001", XSDDatatype.XSDinteger)
                .addProperty(propertyFloat, "1", XSDDatatype.XSDfloat)
                .addProperty(propertyDouble, "1.00", XSDDatatype.XSDdouble);
        model.createResource("D")
                .addProperty(RDF.type, model.createResource(NS + "ExampleObject"))
                .addProperty(propertyBoolean, "false", XSDDatatype.XSDboolean)
                .addProperty(propertyInt, "0", XSDDatatype.XSDinteger)
                .addProperty(propertyFloat, "0.0", XSDDatatype.XSDfloat)
                .addProperty(propertyDouble, "0.0", XSDDatatype.XSDdouble);
        return model;
    }

    private Model writeMixedLiteralSamples(Graph g) {
        final var model = ModelFactory.createModelForGraph(g);
        final var value = model.createProperty(NS + "ExampleObject.value");

        model.createResource("A")
                .addProperty(RDF.type, model.createResource(NS + "ExampleObject"))
                .addProperty(value, "true", XSDDatatype.XSDboolean);
        model.createResource("B")
                .addProperty(RDF.type, model.createResource(NS + "ExampleObject"))
                .addProperty(value, "1", XSDDatatype.XSDinteger);
        model.createResource("C")
                .addProperty(RDF.type, model.createResource(NS + "ExampleObject"))
                .addProperty(value, "1.0", XSDDatatype.XSDfloat);
        model.createResource("D")
                .addProperty(RDF.type, model.createResource(NS + "ExampleObject"))
                .addProperty(value, "1.0", XSDDatatype.XSDdouble);
        model.createResource("E")
                .addProperty(RDF.type, model.createResource(NS + "ExampleObject"))
                .addProperty(value, "true", XSDDatatype.XSDboolean);
        model.createResource("F")
                .addProperty(RDF.type, model.createResource(NS + "ExampleObject"))
                .addProperty(value, "1", XSDDatatype.XSDinteger);
        model.createResource("G")
                .addProperty(RDF.type, model.createResource(NS + "ExampleObject"))
                .addProperty(value, "1.0", XSDDatatype.XSDfloat);
        model.createResource("H")
                .addProperty(RDF.type, model.createResource(NS + "ExampleObject"))
                .addProperty(value, "1.0", XSDDatatype.XSDdouble);
        model.createResource("I")
                .addProperty(RDF.type, model.createResource(NS + "ExampleObject"))
                .addProperty(value, "1", XSDDatatype.XSDboolean);
        model.createResource("J")
                .addProperty(RDF.type, model.createResource(NS + "ExampleObject"))
                .addProperty(value, "001", XSDDatatype.XSDinteger);
        model.createResource("K")
                .addProperty(RDF.type, model.createResource(NS + "ExampleObject"))
                .addProperty(value, "1", XSDDatatype.XSDfloat);
        model.createResource("L")
                .addProperty(RDF.type, model.createResource(NS + "ExampleObject"))
                .addProperty(value, "1.00", XSDDatatype.XSDdouble);
        model.createResource("M")
                .addProperty(RDF.type, model.createResource(NS + "ExampleObject"))
                .addProperty(value, "false", XSDDatatype.XSDboolean);
        model.createResource("N")
                .addProperty(RDF.type, model.createResource(NS + "ExampleObject"))
                .addProperty(value, "0", XSDDatatype.XSDinteger);
        model.createResource("O")
                .addProperty(RDF.type, model.createResource(NS + "ExampleObject"))
                .addProperty(value, "0.0", XSDDatatype.XSDfloat);
        model.createResource("P")
                .addProperty(RDF.type, model.createResource(NS + "ExampleObject"))
                .addProperty(value, "0.0", XSDDatatype.XSDdouble);
        return model;
    }

    @Benchmark
    public Serialization.SerializedData serialize() {
        final var compressedData = Serialization.serialize(graphToSerialize, rdfFormat, param2_Compressor);
        //print: "Size of output stream in rdfFormat %rdfFormat% is xxx.xx MB.
        System.out.printf("Size of output stream in rdfFormat %s is %.2f MB.\n", rdfFormat.toString(), compressedData.bytes().length / 1024.0 / 1024.0);
        return compressedData;
    }

    @Benchmark
    public Graph deserialize() {
        return Serialization.deserialize(serializedGraph, canonicalValues);
    }

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        this.canonicalValues = Boolean.parseBoolean(param3_canonicalValues);
        this.graphToSerialize = new GraphMem2Fast();
        TripleReaderReadingCGMES_2_4_15_WithTypedLiterals.read(param0_GraphUri, this.graphToSerialize, true, false);
        this.rdfFormat = getRDFFormat(param1_RDFFormat);
        this.serializedGraph = Serialization.serialize(graphToSerialize, rdfFormat, param2_Compressor);
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JMHDefaultOptions.getDefaults(this.getClass())
                .warmupIterations(5)
                .measurementIterations(25)
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }
}

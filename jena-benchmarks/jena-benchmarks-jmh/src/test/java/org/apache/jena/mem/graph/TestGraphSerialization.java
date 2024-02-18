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

import org.apache.jena.graph.Graph;
import org.apache.jena.mem.graph.helper.GraphSerialization;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.mem.graph.helper.TripleReaderReadingCGMES_2_4_15_WithTypedLiterals;
import org.apache.jena.mem2.GraphMem2Fast;
import org.apache.jena.riot.RDFFormat;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.io.File;
import java.util.List;

@State(Scope.Benchmark)
public class TestGraphSerialization {

    @Param({
//            "cheeses-0.1.ttl",
//            "pizza.owl.rdf",
            "xxx_CGMES_EQ.xml",
            "xxx_CGMES_SSH.xml",
            "xxx_CGMES_TP.xml",
            "RealGrid_EQ.xml",
            "RealGrid_SSH.xml",
            "RealGrid_TP.xml",
            "RealGrid_SV.xml",
            "bsbm-1m.nt.gz",
//            "bsbm-5m.nt.gz",
//            "bsbm-25m.nt.gz",
    })
    public String param0_GraphUri;

    private static String getFilePath(String fileName) {
        switch (fileName) {
            case "cheeses-0.1.ttl":
                return "../testing/cheeses-0.1.ttl";
            case "pizza.owl.rdf":
                return "../testing/pizza.owl.rdf";
            case "xxx_CGMES_EQ.xml":
                return "C:/temp/res_test/xxx_CGMES_EQ.xml";
            case "xxx_CGMES_SSH.xml":
                return "C:/temp/res_test/xxx_CGMES_SSH.xml";
            case "xxx_CGMES_TP.xml":
                return "C:/temp/res_test/xxx_CGMES_TP.xml";
            case "RealGrid_EQ.xml":
                return "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml";
            case "RealGrid_SSH.xml":
                return "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml";
            case "RealGrid_TP.xml":
                return "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml";
            case "RealGrid_SV.xml":
                return "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml";
            case "bsbm-1m.nt.gz":
                return "../testing/BSBM/bsbm-1m.nt.gz";
            case "bsbm-5m.nt.gz":
                return "../testing/BSBM/bsbm-5m.nt.gz";
            case "bsbm-25m.nt.gz":
                return "../testing/BSBM/bsbm-25m.nt.gz";
            default:
                throw new IllegalArgumentException("Unknown fileName: " + fileName);
        }
    }

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
//            "JSONLD11_PRETTY", --> seems to be broken
//            "JSONLD11_PLAIN",
//            "JSONLD11_FLAT",
//            "RDFXML_PRETTY",
//            "RDFXML_PLAIN",
//            "RDFJSON",
//            "TRIX",
//            "RDF_PROTO",
//            "RDF_PROTO2",
//            "RDF_PROTO_VALUES",
            "RDF_THRIFT",
//            "RDF_THRIFT_VALUES",
            "RDF_THRIFT2",
            "RDF_THRIFT3",
    })
    public String param1_RDFFormat;
    @Param({
            GraphSerialization.NO_COMPRESSOR,
//            GraphSerialization.LZ4_FASTEST,
//            GraphSerialization.GZIP
    })
    public String param2_Compressor;


    private Graph graphToSerialize;
    private GraphSerialization.SerializedData serializedGraph;
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
            case "RDF_PROTO2":
                return RDFFormat.RDF_PROTO2;
            case "RDF_THRIFT":
                return RDFFormat.RDF_THRIFT;
            case "RDF_THRIFT_VALUES":
                return RDFFormat.RDF_THRIFT_VALUES;
            case "RDF_THRIFT2":
                return RDFFormat.RDF_THRIFT2;
            case "RDF_THRIFT3":
                return RDFFormat.RDF_THRIFT3;
            default:
                throw new IllegalArgumentException("Unknown resultSetLang: " + rdfFormat);
        }
    }

    @Test
    @Ignore
    public void loadSerializeAndDeserialize() {
        for(var file : List.of(
//                "../testing/cheeses-0.1.ttl",
//                "../testing/pizza.owl.rdf",
                "C:/temp/res_test/xxx_CGMES_EQ.xml",
                "C:/temp/res_test/xxx_CGMES_SSH.xml",
                "C:/temp/res_test/xxx_CGMES_TP.xml",
//                "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml",
//                "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml",
                "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml"
//                "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml",
//                "../testing/BSBM/bsbm-1m.nt.gz"
            )) {
            final var fileName = new File(file).getName();
            final var g = new GraphMem2Fast();
            TripleReaderReadingCGMES_2_4_15_WithTypedLiterals.read(file, g);
            for (var rdfFormat : List.of(
//                    RDFFormat.TURTLE_PRETTY, RDFFormat.TURTLE_BLOCKS, RDFFormat.TURTLE_FLAT, RDFFormat.TURTLE_LONG,
//                    RDFFormat.NTRIPLES_UTF8, RDFFormat.NQUADS_UTF8,
//                    RDFFormat.TRIG_PRETTY, RDFFormat.TRIG_BLOCKS, RDFFormat.TRIG_FLAT, RDFFormat.TRIG_LONG,
//                    /*RDFFormat.JSONLD11_PRETTY, --> seem to be broken*/ RDFFormat.JSONLD11_PLAIN, RDFFormat.JSONLD11_FLAT,
//                    RDFFormat.RDFXML_PRETTY, RDFFormat.RDFXML_PLAIN, RDFFormat.RDFJSON,
//                    RDFFormat.TRIX,
                    RDFFormat.RDF_PROTO, //RDFFormat.RDF_PROTO_VALUES,
//                    RDFFormat.RDF_PROTO2,
                    RDFFormat.RDF_THRIFT, //RDFFormat.RDF_THRIFT_VALUES,
                    RDFFormat.RDF_THRIFT2,
                    RDFFormat.RDF_THRIFT3)) {
                for (var compressor : List.of(GraphSerialization.NO_COMPRESSOR, GraphSerialization.LZ4_FASTEST, GraphSerialization.GZIP)) {
                    final var compressedGraph = GraphSerialization.serialize(g, rdfFormat, compressor);
                    for(var i=0; i<10; i++) {
                        final var deserializedGraph = GraphSerialization.deserialize(compressedGraph, false);
                        Assert.assertEquals(g.size(), deserializedGraph.size());
                    }
                    //print: "Size of output stream in format %resultSetLang% and with compressor %compressor% is xxx.xx MB.
                    System.out.printf("Size of %-20s in format %-20s and with compressor %-12s is %7.2f MB.\n", fileName, rdfFormat.toString(), compressor, compressedGraph.bytes().length / 1024.0 / 1024.0);
                }
            }
        }
    }

//    @Benchmark
//    public GraphSerialization.SerializedData serialize() {
//        return GraphSerialization.serialize(graphToSerialize, rdfFormat, param2_Compressor);
//    }
//
//    @Benchmark
//    public Graph deserialize() {
//        return GraphSerialization.deserialize(serializedGraph, false);
//    }

    @Benchmark
    public Graph serializeAndDeserializeGraph() {
        return GraphSerialization.deserialize(GraphSerialization.serialize(graphToSerialize, rdfFormat, param2_Compressor), false);
    }

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        this.graphToSerialize = new GraphMem2Fast();
        TripleReaderReadingCGMES_2_4_15_WithTypedLiterals.read(getFilePath(param0_GraphUri), this.graphToSerialize);
        this.rdfFormat = getRDFFormat(param1_RDFFormat);
        this.serializedGraph = GraphSerialization.serialize(graphToSerialize, rdfFormat, param2_Compressor);
        System.out.printf("\nSize of output stream in resultSetLang %s is %4.2f MB. Graph contains %d triples.\n", rdfFormat.toString(), serializedGraph.bytes().length / 1024.0 / 1024.0, graphToSerialize.size());
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JMHDefaultOptions.getDefaults(this.getClass())
                .warmupIterations(8)
                .measurementIterations(30)
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }
}

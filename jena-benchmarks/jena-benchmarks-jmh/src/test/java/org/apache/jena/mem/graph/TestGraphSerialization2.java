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
import org.apache.jena.mem.graph.helper.*;
import org.apache.jena.riot.RDFFormat;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.util.function.Supplier;


@State(Scope.Benchmark)
public class TestGraphSerialization2 {

    @Param({
//            "../testing/cheeses-0.1.ttl",
//            "../testing/pizza.owl.rdf",
            "C:/temp/res_test/xxx_CGMES_EQ.xml",
            "C:/temp/res_test/xxx_CGMES_SSH.xml",
            "C:/temp/res_test/xxx_CGMES_TP.xml",
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
//            "RDF_THRIFT",
//            "RDF_THRIFT_VALUES"
    })
    public String param1_RDFFormat;
    @Param({
            Serialization.NO_COMPRESSOR,
//            Serialization.LZ4_FASTEST,
//            Serialization.GZIP
    })
    public String param2_Compressor;

    @Param({
//            "GraphMem (current)",
            "GraphMem2Fast (current)",
//            "GraphMem2Legacy (current)",
//            "GraphMem2Roaring (current)",
            "GraphMem (Jena 4.8.0)",
//            "GraphWrapperTransactional (current)",
//            "GraphTxn (current)",
    })
    public String param3_GraphImplementation;

    private Graph graphCurrent;
    private org.apache.shadedJena480.graph.Graph graph480;

    private Serialization.SerializedData serializedGraphCurrent;
    private Serialization480.SerializedData serializedGraph480;
    private RDFFormat rdfFormatCurrent;
    private org.apache.shadedJena480.riot.RDFFormat rdfFormat480;

    private Supplier<Graph> graphSupplierCurrent;
    private Supplier<org.apache.shadedJena480.graph.Graph> graphSupplier480;

    private Context trialContext;

    private static RDFFormat getRDFFormatCurrent(String rdfFormat) {
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

    private static org.apache.shadedJena480.riot.RDFFormat getRDFFormat480(String rdfFormat) {
        switch (rdfFormat) {
            case "TURTLE_PRETTY":
                return org.apache.shadedJena480.riot.RDFFormat.TURTLE_PRETTY;
            case "TURTLE_BLOCKS":
                return org.apache.shadedJena480.riot.RDFFormat.TURTLE_BLOCKS;
            case "TURTLE_FLAT":
                return org.apache.shadedJena480.riot.RDFFormat.TURTLE_FLAT;
            case "NTRIPLES_UTF8":
                return org.apache.shadedJena480.riot.RDFFormat.NTRIPLES_UTF8;
            case "NQUADS_UTF8":
                return org.apache.shadedJena480.riot.RDFFormat.NQUADS_UTF8;
            case "TRIG_PRETTY":
                return org.apache.shadedJena480.riot.RDFFormat.TRIG_PRETTY;
            case "TRIG_BLOCKS":
                return org.apache.shadedJena480.riot.RDFFormat.TRIG_BLOCKS;
            case "TRIG_FLAT":
                return org.apache.shadedJena480.riot.RDFFormat.TRIG_FLAT;
            case "JSONLD11_PRETTY":
                return org.apache.shadedJena480.riot.RDFFormat.JSONLD11_PRETTY;
            case "JSONLD11_PLAIN":
                return org.apache.shadedJena480.riot.RDFFormat.JSONLD11_PLAIN;
            case "JSONLD11_FLAT":
                return org.apache.shadedJena480.riot.RDFFormat.JSONLD11_FLAT;
            case "RDFXML_PRETTY":
                return org.apache.shadedJena480.riot.RDFFormat.RDFXML_PRETTY;
            case "RDFXML_PLAIN":
                return org.apache.shadedJena480.riot.RDFFormat.RDFXML_PLAIN;
            case "RDFJSON":
                return org.apache.shadedJena480.riot.RDFFormat.RDFJSON;
            case "TRIX":
                return org.apache.shadedJena480.riot.RDFFormat.TRIX;
            case "RDF_PROTO":
                return org.apache.shadedJena480.riot.RDFFormat.RDF_PROTO;
            case "RDF_PROTO_VALUES":
                return org.apache.shadedJena480.riot.RDFFormat.RDF_PROTO_VALUES;
            case "RDF_THRIFT":
                return org.apache.shadedJena480.riot.RDFFormat.RDF_THRIFT;
            case "RDF_THRIFT_VALUES":
                return org.apache.shadedJena480.riot.RDFFormat.RDF_THRIFT_VALUES;
            default:
                throw new IllegalArgumentException("Unknown rdfFormat: " + rdfFormat);
        }
    }

//    @Benchmark
//    public Object serialize() {
//        return serializer.get();
//    }

    private Serialization.SerializedData serializeCurrent() {
        return Serialization.serialize(graphCurrent, rdfFormatCurrent, param2_Compressor);
    }

    private Serialization480.SerializedData serialize480() {
        return Serialization480.serialize(graph480, rdfFormat480, param2_Compressor);
    }

    @Benchmark
    public Object deserialize() {
        return deserializer.get();
    }

    private Graph deserializeCurrent() {
        return Serialization.deserialize(serializedGraphCurrent, false, graphSupplierCurrent);
    }

    private org.apache.shadedJena480.graph.Graph deserialize480() {
        return Serialization480.deserialize(serializedGraph480, false, graphSupplier480);
    }

    private Supplier<Object> serializer;

    private Supplier<Object> deserializer;

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        this.trialContext = new Context(param3_GraphImplementation);
        switch (this.trialContext.getJenaVersion()) {
            case CURRENT:
                this.rdfFormatCurrent = getRDFFormatCurrent(param1_RDFFormat);
                this.graphSupplierCurrent = Releases.current.graphSupplier(trialContext.getGraphClass());


                var triples = Releases.current.readTriples(param0_GraphUri);
                this.graphCurrent = this.graphSupplierCurrent.get();
                triples.forEach(graphCurrent::add);

                this.serializedGraphCurrent = Serialization.serialize(graphCurrent, rdfFormatCurrent, param2_Compressor);
                System.out.printf("\nSize of output stream in rdfFormat %s is %.2f MB. Triple count: %d\n", rdfFormatCurrent.toString(), serializedGraphCurrent.bytes().length / 1024.0 / 1024.0, graphCurrent.size());

                this.serializer = this::serializeCurrent;
                this.deserializer = this::deserializeCurrent;
                break;
            case JENA_4_8_0:
                this.rdfFormat480 = getRDFFormat480(param1_RDFFormat);
                this.graphSupplier480 = Releases.v480.graphSupplier(trialContext.getGraphClass());

                var triples480 = Releases.v480.readTriples(param0_GraphUri);
                this.graph480 = this.graphSupplier480.get();
                triples480.forEach(graph480::add);

                this.serializedGraph480 = Serialization480.serialize(graph480, rdfFormat480, param2_Compressor);
                System.out.printf("\nSize of output stream in rdfFormat %s is %.2f MB. Triple count: %d\n", rdfFormat480.toString(), serializedGraph480.bytes().length / 1024.0 / 1024.0, graph480.size());

                this.serializer = this::serialize480;
                this.deserializer = this::deserialize480;
                break;
            default:
                throw new IllegalArgumentException("Unknown Jena version: " + this.trialContext.getJenaVersion());
        }
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JMHDefaultOptions.getDefaults(this.getClass())
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }
}

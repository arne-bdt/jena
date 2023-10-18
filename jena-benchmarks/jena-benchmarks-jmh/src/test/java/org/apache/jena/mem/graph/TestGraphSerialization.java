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

import net.jpountz.lz4.LZ4Factory;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Graph;
import org.apache.jena.mem.TripleReaderReadingCGMES_2_4_15_WithTypedLiterals;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.mem2.GraphMem2Fast;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RDFWriter;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


@State(Scope.Benchmark)
public class TestGraphSerialization {

    private static final String NO_COMPRESSOR = "none";
    private static final String LZ4_FASTEST = "LZ4Fastest";

    private final static String GZIP = "GZIP";

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
            "TURTLE_PRETTY",
            "TURTLE_BLOCKS",
            "TURTLE_FLAT",
            "TURTLE_LONG",
            "NTRIPLES_UTF8",
            "NQUADS_UTF8",
            "TRIG_PRETTY",
            "TRIG_BLOCKS",
            "TRIG_FLAT",
            "TRIG_LONG",
            "JSONLD11_PRETTY",
            "JSONLD11_PLAIN",
            "JSONLD11_FLAT",
            "RDFXML_PRETTY",
            "RDFXML_PLAIN",
            "RDFJSON",
            "TRIX",
            "RDF_PROTO",
            "RDF_PROTO_VALUES",
            "RDF_THRIFT",
            "RDF_THRIFT_VALUES"
    })
    public String param1_RDFFormat;
    @Param({
            NO_COMPRESSOR,
            LZ4_FASTEST,
            GZIP
    })
    public String param2_Compressor;
    private Graph graphToSerialize;
    private CompressedData compressedGraph;
    private RDFFormat rdfFormat;

    private static Graph deserialize(CompressedData compressedGraph) {
        var g1 = new GraphMem2Fast();

        switch (compressedGraph.compressorName) {
            case NO_COMPRESSOR -> {
                final var inputStream = new ByteArrayInputStream(compressedGraph.bytes);
                //RDFDataMgr.read(g1, inputStream, compressedGraph.rdfFormat.getLang());
                RDFParser
                        .source(inputStream)
                        .forceLang(compressedGraph.rdfFormat.getLang())
                        .checking(false)
                        .canonicalValues(false)
                        .parse(g1);
                try {
                    inputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            case LZ4_FASTEST -> {
                var uncompressedBytes = new byte[compressedGraph.uncompressedSize];
                LZ4Factory.fastestInstance().fastDecompressor().decompress(compressedGraph.bytes, uncompressedBytes);
                final var inputStream = new ByteArrayInputStream(uncompressedBytes);
                //RDFDataMgr.read(g1, inputStream, compressedGraph.rdfFormat.getLang());
                RDFParser
                        .source(inputStream)
                        .forceLang(compressedGraph.rdfFormat.getLang())
                        .checking(false)
                        .canonicalValues(false)
                        .parse(g1);
                try {
                    inputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            case GZIP -> {
                final var inputStream = new ByteArrayInputStream(compressedGraph.bytes);
                final GZIPInputStream inputStreamCompressed;
                try {
                    inputStreamCompressed = new java.util.zip.GZIPInputStream(inputStream);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                //RDFDataMgr.read(g1, inputStreamCompressed, compressedGraph.rdfFormat.getLang());
                RDFParser
                        .source(inputStreamCompressed)
                        .forceLang(compressedGraph.rdfFormat.getLang())
                        .checking(false)
                        .canonicalValues(false)
                        .parse(g1);
                try {
                    inputStreamCompressed.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                try {
                    inputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            default -> throw new IllegalArgumentException("Unknown compressor: " + compressedGraph.compressorName);
        }
        return g1;
    }

    private static CompressedData serialize(Graph graph, RDFFormat rdfFormat, String compressorName) {
        switch (compressorName) {
            case NO_COMPRESSOR -> {
                final var outputStream = new ByteArrayOutputStream();
                RDFWriter.source(graph).format(rdfFormat).output(outputStream);
                try {
                    outputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return new CompressedData(outputStream.toByteArray(), outputStream.size(), compressorName, rdfFormat);

            }
            case LZ4_FASTEST -> {
                final var outputStream = new ByteArrayOutputStream();
                RDFWriter.source(graph).format(rdfFormat).output(outputStream);
                try {
                    outputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                final var outputStreamCompressed = LZ4Factory.fastestInstance().fastCompressor().compress(outputStream.toByteArray());
                return new CompressedData(outputStreamCompressed, outputStream.size(), compressorName, rdfFormat);

            }
            case GZIP -> {
                final var outputStream = new ByteArrayOutputStream();
                final GZIPOutputStream outputStreamCompressed;
                try {
                    outputStreamCompressed = new GZIPOutputStream(outputStream);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                RDFWriter.source(graph).format(rdfFormat).output(outputStreamCompressed);
                try {
                    outputStreamCompressed.close();
                    outputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return new CompressedData(outputStream.toByteArray(), -1, compressorName, rdfFormat);
            }
            default -> throw new IllegalArgumentException("Unknown compressor: " + compressorName);
        }
    }

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

    //@Test
    public void loadSerializeAndDeserialize() {
        //final var rdfFormat = Lang.RDFXML;
        final var g = new GraphMem2Fast();
        TripleReaderReadingCGMES_2_4_15_WithTypedLiterals.read("C:/temp/res_test/xxx_CGMES_SSH.xml", g);
        for (var rdfFormat : List.of(RDFFormat.TURTLE_PRETTY, RDFFormat.TURTLE_BLOCKS, RDFFormat.TURTLE_FLAT, RDFFormat.TURTLE_LONG, RDFFormat.NTRIPLES_UTF8, RDFFormat.NQUADS_UTF8, RDFFormat.TRIG_PRETTY, RDFFormat.TRIG_BLOCKS, RDFFormat.TRIG_FLAT, RDFFormat.TRIG_LONG, RDFFormat.JSONLD11_PRETTY, RDFFormat.JSONLD11_PLAIN, RDFFormat.JSONLD11_FLAT, RDFFormat.RDFXML_PRETTY, RDFFormat.RDFXML_PLAIN, RDFFormat.RDFJSON, RDFFormat.TRIX, RDFFormat.RDF_PROTO, RDFFormat.RDF_PROTO_VALUES, RDFFormat.RDF_THRIFT, RDFFormat.RDF_THRIFT_VALUES)) {
            for (var compressor : List.of(NO_COMPRESSOR, LZ4_FASTEST, GZIP)) {
                var compressedGraph = serialize(g, rdfFormat, compressor);
                //print: "Size of output stream in format %rdfFormat% and with compressor %compressor% is xxx.xx MB.
                System.out.printf("Size of output stream in format %20s and with compressor %12s is %6.2f MB.\n", rdfFormat.toString(), compressor, compressedGraph.bytes.length / 1024.0 / 1024.0);
                var g2 = deserialize(compressedGraph);
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

    @Benchmark
    public CompressedData serialize() {
        final var compressedData = serialize(graphToSerialize, rdfFormat, param2_Compressor);
        //print: "Size of output stream in rdfFormat %rdfFormat% is xxx.xx MB.
        System.out.printf("Size of output stream in rdfFormat %s is %.2f MB.\n", rdfFormat.toString(), compressedData.bytes.length / 1024.0 / 1024.0);
        return compressedData;
    }

    @Benchmark
    public Graph deserialize() {
        return deserialize(compressedGraph);
    }

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        this.graphToSerialize = new GraphMem2Fast();
        TripleReaderReadingCGMES_2_4_15_WithTypedLiterals.read(param0_GraphUri, this.graphToSerialize);
        this.rdfFormat = getRDFFormat(param1_RDFFormat);
        this.compressedGraph = serialize(graphToSerialize, rdfFormat, param2_Compressor);
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JMHDefaultOptions.getDefaults(this.getClass())
                .warmupIterations(5)
                .measurementIterations(8)
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }

    private record CompressedData(byte[] bytes, int uncompressedSize, String compressorName, RDFFormat rdfFormat) {
    }

}

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
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Graph;
import org.apache.jena.mem.TripleReaderReadingCGMES_2_4_15_WithTypedLiterals;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.mem2.GraphMem2Fast;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertTrue;


@State(Scope.Benchmark)
public class TestGraphSerialization {

    private static final String NO_COMPRESSOR = "";
    private static final String LZ4_FASTEST = "LZ4Fastest";
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
            "RDFXML",
            "TURTLE",
            "NTRIPLES",
//            "JSONLD",
//            "JSONLD11",
//            "RDFJSON",
            "TRIG",
//            "NQUADS",
            "RDFPROTO",
            "RDFTHRIFT",
//            "TRIX"
    })
    public String param1_Lang;
    @Param({
            NO_COMPRESSOR,
            LZ4_FASTEST,
            CompressorStreamFactory.GZIP,
//            CompressorStreamFactory.LZ4_BLOCK,
//            CompressorStreamFactory.LZ4_FRAMED,
    })
    public String param2_Compressor;
    private Graph graphToSerialize;
    private CompressedData compressedGraph;
    private Lang lang;

    private static Graph deserialize(CompressedData compressedGraph) {
        var g1 = new GraphMem2Fast();

        if (NO_COMPRESSOR.equals(compressedGraph.compressorName)) {
            final var inputStream = new ByteArrayInputStream(compressedGraph.bytes);
            RDFDataMgr.read(g1, inputStream, compressedGraph.lang);
            try {
                inputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (LZ4_FASTEST.equals(compressedGraph.compressorName)) {
            var uncompressedBytes = new byte[compressedGraph.uncompressedSize];
            LZ4Factory.fastestInstance().fastDecompressor().decompress(compressedGraph.bytes, uncompressedBytes);
            final var inputStream = new ByteArrayInputStream(uncompressedBytes);
            RDFDataMgr.read(g1, inputStream, compressedGraph.lang);
            try {
                inputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            final var inputStream = new ByteArrayInputStream(compressedGraph.bytes);
            final CompressorInputStream inputStreamCompressed;
            try {
                inputStreamCompressed = new CompressorStreamFactory().createCompressorInputStream(compressedGraph.compressorName, inputStream);
            } catch (CompressorException e) {
                throw new RuntimeException(e);
            }
            RDFDataMgr.read(g1, inputStreamCompressed, compressedGraph.lang);
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
        return g1;
    }

    private static CompressedData serialize(Graph graph, Lang lang, String compressorName) {
        if (NO_COMPRESSOR.equals(compressorName)) {
            final var outputStream = new ByteArrayOutputStream();
            RDFDataMgr.write(outputStream, graph, lang);
            try {
                outputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return new CompressedData(outputStream.toByteArray(), outputStream.size(), compressorName, lang);

        } else if (LZ4_FASTEST.equals(compressorName)) {
            final var outputStream = new ByteArrayOutputStream();
            RDFDataMgr.write(outputStream, graph, lang);
            try {
                outputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            final var outputStreamCompressed = LZ4Factory.fastestInstance().fastCompressor().compress(outputStream.toByteArray());
            return new CompressedData(outputStreamCompressed, outputStream.size(), compressorName, lang);

        } else {
            final var outputStream = new ByteArrayOutputStream();
            final CompressorOutputStream outputStreamCompressed;
            try {
                outputStreamCompressed = new CompressorStreamFactory().createCompressorOutputStream(compressorName, outputStream);
            } catch (CompressorException e) {
                throw new RuntimeException(e);
            }
            RDFDataMgr.write(outputStreamCompressed, graph, lang);
            try {
                outputStreamCompressed.close();
                outputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return new CompressedData(outputStream.toByteArray(), -1, compressorName, lang);
        }
    }

    private static Lang getLang(String lang) {
        switch (lang) {
            case "RDFXML":
                return Lang.RDFXML;
            case "TURTLE":
                return Lang.TURTLE;
            case "NTRIPLES":
                return Lang.NTRIPLES;
            case "JSONLD":
                return Lang.JSONLD;
            case "JSONLD11":
                return Lang.JSONLD11;
            case "RDFJSON":
                return Lang.RDFJSON;
            case "TRIG":
                return Lang.TRIG;
            case "NQUADS":
                return Lang.NQUADS;
            case "RDFPROTO":
                return Lang.RDFPROTO;
            case "RDFTHRIFT":
                return Lang.RDFTHRIFT;
            case "TRIX":
                return Lang.TRIX;
            default:
                throw new IllegalArgumentException("Unknown lang: " + lang);
        }
    }

    //    @Test
    public void loadSerializeAndDeserialize() {
        //final var lang = Lang.RDFXML;
        final var g = new GraphMem2Fast();
        TripleReaderReadingCGMES_2_4_15_WithTypedLiterals.read("C:/temp/res_test/xxx_CGMES_SSH.xml", g);
        for (var lang : List.of(Lang.RDFXML, Lang.TURTLE, Lang.NTRIPLES, Lang.TRIG, Lang.RDFPROTO, Lang.RDFTHRIFT)) {
            for (var compressor : List.of("", "LZ4Fastest", CompressorStreamFactory.GZIP, CompressorStreamFactory.LZ4_BLOCK, CompressorStreamFactory.LZ4_FRAMED)) {
                var compressedGraph = serialize(g, lang, compressor);
                //print: "Size of output stream in lang %lang% and with compressor %compressor% is xxx.xx MB.
                System.out.printf("Size of output stream in lang %s and with compressor %s is %.2f MB.\n", lang.getName(), compressor, compressedGraph.bytes.length / 1024.0 / 1024.0);
                var g2 = deserialize(compressedGraph);
                final var tripleWithBooleanObject = g.stream().filter(t -> t.getObject().isLiteral() && t.getObject().getLiteralDatatype() == XSDDatatype.XSDboolean).findFirst().orElseThrow();
                final var tripleWithFloatObject = g.stream().filter(t -> t.getObject().isLiteral() && t.getObject().getLiteralDatatype() == XSDDatatype.XSDfloat).findFirst().orElseThrow();
                assertTrue(g2.contains(tripleWithBooleanObject));
                assertTrue(g2.contains(tripleWithFloatObject));
            }
        }
    }

    @Benchmark
    public CompressedData serialize() {
        final var compressedData = serialize(graphToSerialize, lang, param2_Compressor);
        //print: "Size of output stream in lang %lang% is xxx.xx MB.
        System.out.printf("Size of output stream in lang %s is %.2f MB.\n", lang.getName(), compressedData.bytes.length / 1024.0 / 1024.0);
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
        this.lang = getLang(param1_Lang);
        this.compressedGraph = serialize(graphToSerialize, lang, param2_Compressor);
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

    private record CompressedData(byte[] bytes, int uncompressedSize, String compressorName, Lang lang) {
    }

}

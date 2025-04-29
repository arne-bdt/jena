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

package org.apache.jena.mem.graph.helper;

import net.jpountz.lz4.LZ4Factory;
import org.apache.shadedJena530.mem2.GraphMem2Fast;
import org.apache.shadedJena530.graph.Graph;
import org.apache.shadedJena530.riot.RDFFormat;
import org.apache.shadedJena530.riot.RDFParser;
import org.apache.shadedJena530.riot.RDFWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.function.Supplier;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Serialization530 {

    public static final String NO_COMPRESSOR = "none";
    public static final String LZ4_FASTEST = "LZ4Fastest";
    public final static String GZIP = "GZIP";


    public record SerializedData(byte[] bytes, int uncompressedSize, String compressorName, RDFFormat rdfFormat) {
    }

    public static SerializedData serialize(Graph graph, RDFFormat rdfFormat, String compressorName) {
        switch (compressorName) {
            case NO_COMPRESSOR -> {
                final var outputStream = new ByteArrayOutputStream();
                RDFWriter.source(graph).format(rdfFormat).output(outputStream);
                try {
                    outputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return new SerializedData(outputStream.toByteArray(), outputStream.size(), compressorName, rdfFormat);

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
                return new SerializedData(outputStreamCompressed, outputStream.size(), compressorName, rdfFormat);

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
                return new SerializedData(outputStream.toByteArray(), -1, compressorName, rdfFormat);
            }
            default -> throw new IllegalArgumentException("Unknown compressor: " + compressorName);
        }
    }

    public static Graph deserialize(SerializedData compressedGraph, boolean canonicalValues) {
        return deserialize(compressedGraph, canonicalValues, GraphMem2Fast::new);
    }

    public static Graph deserialize(SerializedData compressedGraph, boolean canonicalValues, Supplier<Graph> graphSupplier) {
        var g1 = graphSupplier.get();

        switch (compressedGraph.compressorName) {
            case NO_COMPRESSOR -> {
                final var inputStream = new ByteArrayInputStream(compressedGraph.bytes);
                //RDFDataMgr.read(g1, inputStream, compressedGraph.rdfFormat.getLang());
                RDFParser
                        .source(inputStream)
                        .forceLang(compressedGraph.rdfFormat.getLang())
                        .checking(false)
                        .canonicalValues(canonicalValues)
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
                        .canonicalValues(canonicalValues)
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
                    inputStreamCompressed = new GZIPInputStream(inputStream);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                //RDFDataMgr.read(g1, inputStreamCompressed, compressedGraph.rdfFormat.getLang());
                RDFParser
                        .source(inputStreamCompressed)
                        .forceLang(compressedGraph.rdfFormat.getLang())
                        .checking(false)
                        .canonicalValues(canonicalValues)
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
}

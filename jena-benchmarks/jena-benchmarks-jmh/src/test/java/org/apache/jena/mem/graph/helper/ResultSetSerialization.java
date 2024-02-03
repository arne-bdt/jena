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
import org.apache.jena.query.ResultSet;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.resultset.ResultSetReaderRegistry;
import org.apache.jena.riot.resultset.ResultSetWriterRegistry;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class ResultSetSerialization {

    public static final String NO_COMPRESSOR = "none";
    public static final String LZ4_FASTEST = "LZ4Fastest";
    public final static String GZIP = "GZIP";


    public record SerializedData(byte[] bytes, int uncompressedSize, String compressorName, Lang resultSetLang) {
    }

    public static SerializedData serialize(ResultSet resultSet, Lang resultSetLang, String compressorName) {
        final var resultSetWriter = ResultSetWriterRegistry.getFactory(resultSetLang).create(resultSetLang);
        switch (compressorName) {
            case NO_COMPRESSOR -> {
                final var outputStream = new ByteArrayOutputStream();
                resultSetWriter.write(outputStream, resultSet, null);
                try {
                    outputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return new SerializedData(outputStream.toByteArray(), outputStream.size(), compressorName, resultSetLang);

            }
            case LZ4_FASTEST -> {
                final var outputStream = new ByteArrayOutputStream();
                resultSetWriter.write(outputStream, resultSet, null);
                try {
                    outputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                final var outputStreamCompressed = LZ4Factory.fastestInstance().fastCompressor().compress(outputStream.toByteArray());
                return new SerializedData(outputStreamCompressed, outputStream.size(), compressorName, resultSetLang);

            }
            case GZIP -> {
                final var outputStream = new ByteArrayOutputStream();
                final GZIPOutputStream outputStreamCompressed;
                try {
                    outputStreamCompressed = new GZIPOutputStream(outputStream);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                resultSetWriter.write(outputStreamCompressed, resultSet, null);
                try {
                    outputStreamCompressed.close();
                    outputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return new SerializedData(outputStream.toByteArray(), -1, compressorName, resultSetLang);
            }
            default -> throw new IllegalArgumentException("Unknown compressor: " + compressorName);
        }
    }

    public static ResultSet deserialize(SerializedData serializedResultSet) {
        final ResultSet resultSet;
        final var resultSetReader = ResultSetReaderRegistry
                .getFactory(serializedResultSet.resultSetLang)
                .create(serializedResultSet.resultSetLang);
        switch (serializedResultSet.compressorName) {
            case NO_COMPRESSOR -> {
                final var inputStream = new ByteArrayInputStream(serializedResultSet.bytes);
                resultSet = resultSetReader.read(inputStream, null);
                try {
                    inputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            case LZ4_FASTEST -> {
                var uncompressedBytes = new byte[serializedResultSet.uncompressedSize];
                LZ4Factory.fastestInstance().fastDecompressor().decompress(serializedResultSet.bytes, uncompressedBytes);
                final var inputStream = new ByteArrayInputStream(uncompressedBytes);
                resultSet =  resultSetReader.read(inputStream, null);
                try {
                    inputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            case GZIP -> {
                final var inputStream = new ByteArrayInputStream(serializedResultSet.bytes);
                final GZIPInputStream inputStreamCompressed;
                try {
                    inputStreamCompressed = new GZIPInputStream(inputStream);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                resultSet =  resultSetReader.read(inputStreamCompressed, null);
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
            default -> throw new IllegalArgumentException("Unknown compressor: " + serializedResultSet.compressorName);
        }
        return resultSet;
    }
}

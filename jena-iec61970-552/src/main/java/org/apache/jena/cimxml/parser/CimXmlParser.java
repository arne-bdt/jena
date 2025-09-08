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

package org.apache.jena.cimxml.parser;

import org.apache.commons.io.input.BufferedFileChannelInputStream;
import org.apache.jena.cimxml.graph.CimProfile;
import org.apache.jena.cimxml.parser.system.StreamCIMXMLToDatasetGraph;
import org.apache.jena.cimxml.rdfs.CimProfileRegistry;
import org.apache.jena.cimxml.rdfs.CimProfileRegistryStd;
import org.apache.jena.cimxml.sparql.core.CimDatasetGraph;
import org.apache.jena.riot.system.ErrorHandler;
import org.apache.jena.riot.system.ErrorHandlerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * IEC 61970-552 CIMXML parser for Apache Jena.
 *
 * <p>This parser provides specialized handling for Common Information Model (CIM) XML files
 * as defined by the IEC 61970-552 standard. It extends standard RDF/XML parsing with
 * CIM-specific features including:</p>
 *
 * <ul>
 *   <li>Automatic CIM version detection from namespace declarations</li>
 *   <li>UUID normalization (handling underscore prefixes and missing dashes)</li>
 *   <li>Profile-based datatype resolution</li>
 *   <li>Support for FullModel and DifferenceModel structures</li>
 *   <li>Processing instruction handling for IEC 61970-552 version</li>
 * </ul>
 *
 * <h2>Usage Example:</h2>
 * <pre>{@code
 * // Create parser with default error handling
 * CimXmlParser parser = new CimXmlParser();
 *
 * // Register CIM profiles for datatype resolution
 * parser.parseAndRegisterCimProfile(Path.of("Equipment.rdf"));
 *
 * // Parse a CIMXML model
 * CimDatasetGraph dataset = parser.parseCimModel(Path.of("model.xml"));
 *
 * // Access model components
 * if (dataset.isFullModel()) {
 *     Graph body = dataset.getBody();
 *     CimModelHeader header = dataset.getModelHeader();
 * }
 * }</pre>
 *
 * <h2>Thread Safety:</h2>
 * <p>This class is thread-safe for parsing operations. The internal profile registry
 * is synchronized and can be safely accessed from multiple threads.</p>
 *
 * @see CimDatasetGraph
 * @see CimProfileRegistry
 * @see <a href="https://webstore.iec.ch/publication/25939">IEC 61970-552 Standard</a>
 * @since Jena 5.6.0
 */
public class CimXmlParser {

    private final ReaderCIMXML_StAX_SR reader;
    private final CimProfileRegistry cimProfileRegistry;
    private final RdfXmlParser rdfXmlParser;

    /**
     * Gets the error handler used by this parser.
     * @return the error handler
     */
    public ErrorHandler getErrorHandler() {
        return reader.errorHandler;
    }

    /**
     * Gets the CIM profile registry used by this parser.
     * @return the CIM profile registry
     */
    public CimProfileRegistry getCimProfileRegistry() {
        return cimProfileRegistry;
    }

    /**
     * Creates a new CIM/XML parser with the standard error handler.
     */
    public CimXmlParser() {
        this(ErrorHandlerFactory.errorHandlerStd);
    }

    /**
     * Creates a new CIM/XML parser with the given error handler.
     * @param errorHandler the error handler
     */
    public CimXmlParser(final ErrorHandler errorHandler) {
        this.reader = new ReaderCIMXML_StAX_SR(errorHandler);
        this.rdfXmlParser = new RdfXmlParser(this.reader);
        this.cimProfileRegistry = new CimProfileRegistryStd();
    }

    /**
     * Parses the CIM profile from the given path and registers it in the internal CIM profile registry.
     * @param pathToCimProfile the path to the CIM profile
     * @return the parsed CIM profile
     * @throws IOException if an I/O error occurs
     */
    public CimProfile parseAndRegisterCimProfile(final Path pathToCimProfile) throws IOException {
        final var profile = rdfXmlParser.parseCimProfile(pathToCimProfile);
        cimProfileRegistry.register(profile);
        return profile;
    }

    /**
     * Parses the CIM/XML from the given reader and returns the resulting CIM dataset graph.
     * @param reader the reader containing the CIM/XML
     * @return the resulting CIM dataset graph
     */
    public CimDatasetGraph parseCimModel(final Reader reader) {
        final var streamRDFProfile = new StreamCIMXMLToDatasetGraph();
        this.reader.read(reader, cimProfileRegistry, streamRDFProfile);
        return streamRDFProfile.getCIMDatasetGraph();
    }

    /**
     * Parses the CIM/XML from the given input stream and returns the resulting CIM dataset graph.
     * @param inputStream the input stream containing the CIM/XML
     * @return the resulting CIM dataset graph
     */
    public CimDatasetGraph parseCimModel(final InputStream inputStream) {
        final var streamRDFProfile = new StreamCIMXMLToDatasetGraph();
        this.reader.read(inputStream, cimProfileRegistry, streamRDFProfile);
        return streamRDFProfile.getCIMDatasetGraph();
    }

    /**
     * Parses the CIM/XML file at the given path and returns the resulting CIM dataset graph.
     * @param pathToCimModel the path to the CIM/XML file
     * @return the resulting CIM dataset graph
     * @throws IOException if an I/O error occurs
     */
    public CimDatasetGraph parseCimModel(final Path pathToCimModel) throws IOException {
        final var fileSize = Files.size(pathToCimModel);
        final var streamRDFProfile = new StreamCIMXMLToDatasetGraph();
        try(final var is = new BufferedFileChannelInputStream.Builder()
                .setPath(pathToCimModel)
                .setOpenOptions(StandardOpenOption.READ)
                .setBufferSize((fileSize > RdfXmlParser.MAX_BUFFER_SIZE) ? RdfXmlParser.MAX_BUFFER_SIZE : (int) fileSize)
                .get()) {
            reader.read(is, cimProfileRegistry, streamRDFProfile);
        }
        return streamRDFProfile.getCIMDatasetGraph();
    }
}

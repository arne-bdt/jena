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
 * CIM/XML parser.
 * This implementation uses ReaderCIMXML_StAX_SR, which is based on the RDF/XML reader ReaderRDFXML_StAX_SR
 * in Apache Jena, originally.
 * It has been adapted to the CIM/XML needs.
 * <p>
 * This parser has an internal CIM profile registry, which is used when parsing CIM/XML models.
 * CIM profiles can be parsed and registered in this registry using {@link #parseAndRegisterCimProfile(Path)}.
 * The registered CIM profiles are then used to interpret the CIM/XML models and parse the data types correctly.
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

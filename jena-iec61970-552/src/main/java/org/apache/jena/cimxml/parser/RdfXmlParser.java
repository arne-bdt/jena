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
import org.apache.jena.graph.Graph;
import org.apache.jena.riot.system.ErrorHandler;
import org.apache.jena.riot.system.ErrorHandlerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * RDF/XML parser.
 * This implementation uses ReaderCIMXML_StAX_SR, which is based on the RDF/XML reader ReaderRDFXML_StAX_SR
 * in Apache Jena, originally.
 * It has been adapted to the CIM/XML needs.
 * <p>
 * This implementation uses StAX via {@link javax.xml.stream.XMLStreamReader}.
 *
 * @see <a href="https://webstore.iec.ch/en/publication/25939">https://webstore.iec.ch/en/publication/25939</a>
 */
public class RdfXmlParser {

    final static int MAX_BUFFER_SIZE = 64*4096;

    private final ReaderCIMXML_StAX_SR reader;

    /**
     * Gets the error handler used by this parser.
     * @return the error handler
     */
    public ErrorHandler getErrorHandler() {
        return reader.errorHandler;
    }

    /**
     * Creates a new RDF/XML parser with the standard error handler.
     */
    public RdfXmlParser() {
        this(ErrorHandlerFactory.errorHandlerStd);
    }

    /**
     * Creates a new RDF/XML parser with the given reader.
     * @param reader the reader to use
     */
    RdfXmlParser(final ReaderCIMXML_StAX_SR reader) {
        this.reader = reader;
    }

    /**
     * Creates a new RDF/XML parser with the given error handler.
     * @param errorHandler the error handler to use
     */
    public RdfXmlParser(final ErrorHandler errorHandler) {
        this(new ReaderCIMXML_StAX_SR(errorHandler));
    }

    /**
     * Parses the RDF/XML from the given reader and returns the resulting CIM profile.
     * @param reader the reader containing RDF/XML data
     * @return the resulting CIM profile
     * @throws IOException if an I/O error occurs
     */
    public CimProfile parseCimProfile(final Reader reader) throws IOException {
        return CimProfile.wrap(parseGraph(reader));
    }

    /**
     * Parses the RDF/XML from the given input stream and returns the resulting CIM profile.
     * @param inputStream the input stream containing RDF/XML data
     * @return the resulting CIM profile
     * @throws IOException if an I/O error occurs
     */
    public CimProfile parseCimProfile(final InputStream inputStream) throws IOException {
        return CimProfile.wrap(parseGraph(inputStream));
    }

    /**
     * Parses the RDF/XML file at the given path and returns the resulting CIM profile.
     * @param pathToCimProfile the path to the RDF/XML file
     * @return the resulting CIM profile
     * @throws IOException if an I/O error occurs
     */
    public CimProfile parseCimProfile(final Path pathToCimProfile) throws IOException {
        return CimProfile.wrap(parseGraph(pathToCimProfile));
    }

    /**
     * Parses the RDF/XML from the given reader and returns the resulting graph.
     * @param reader the reader containing RDF/XML data
     * @return the resulting graph
     */
    public Graph parseGraph(final Reader reader) {
        final var streamRDFProfile = new StreamCIMXMLToDatasetGraph();
        this.reader.read(reader, streamRDFProfile);
        return streamRDFProfile.getCIMDatasetGraph().getDefaultGraph();
    }

    /**
     * Parses the RDF/XML from the given input stream and returns the resulting graph.
     * @param inputStream the input stream containing RDF/XML data
     * @return the resulting graph
     */
    public Graph parseGraph(final InputStream inputStream) {
        final var streamRDFProfile = new StreamCIMXMLToDatasetGraph();
        this.reader.read(inputStream, streamRDFProfile);
        return streamRDFProfile.getCIMDatasetGraph().getDefaultGraph();
    }

    /**
     * Parses the RDF/XML file at the given path and returns the resulting graph.
     * @param rdfxmlFilePath the path to the RDF/XML file
     * @return the resulting graph
     * @throws IOException if an I/O error occurs
     */
    public Graph parseGraph(final Path rdfxmlFilePath) throws IOException {
        final var fileSize = Files.size(rdfxmlFilePath);
        final var streamRDFProfile = new StreamCIMXMLToDatasetGraph();
        try(final var is = new BufferedFileChannelInputStream.Builder()
                .setPath(rdfxmlFilePath)
                .setOpenOptions(StandardOpenOption.READ)
                .setBufferSize((fileSize > MAX_BUFFER_SIZE) ? MAX_BUFFER_SIZE : (int) fileSize)
                .get()) {
            reader.read(is, streamRDFProfile);
        }
        return streamRDFProfile.getCIMDatasetGraph().getDefaultGraph();
    }



}

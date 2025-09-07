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

public class CimXmlParser {

    private final ReaderCIMXML_StAX_SR reader;
    private final CimProfileRegistry cimProfileRegistry;
    private final RdfXmlParser rdfXmlParser;

    public ErrorHandler getErrorHandler() {
        return reader.errorHandler;
    }

    public CimProfileRegistry getCimProfileRegistry() {
        return cimProfileRegistry;
    }

    public CimXmlParser() {
        this(ErrorHandlerFactory.errorHandlerStd);
    }

    public CimXmlParser(final ErrorHandler errorHandler) {
        this.reader = new ReaderCIMXML_StAX_SR(errorHandler);
        this.rdfXmlParser = new RdfXmlParser(this.reader);
        this.cimProfileRegistry = new CimProfileRegistryStd();
    }

    public CimProfile parseAndRegisterCimProfile(final Path pathToCimProfile) throws IOException {
        final var profile = rdfXmlParser.parseCimProfile(pathToCimProfile);
        cimProfileRegistry.register(profile);
        return profile;
    }

    public CimDatasetGraph parseCimModel(final Reader reader) {
        final var streamRDFProfile = new StreamCIMXMLToDatasetGraph();
        this.reader.read(reader, cimProfileRegistry, streamRDFProfile);
        return streamRDFProfile.getCIMDatasetGraph();
    }

    public CimDatasetGraph parseCimModel(final InputStream inputStream) {
        final var streamRDFProfile = new StreamCIMXMLToDatasetGraph();
        this.reader.read(inputStream, cimProfileRegistry, streamRDFProfile);
        return streamRDFProfile.getCIMDatasetGraph();
    }

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

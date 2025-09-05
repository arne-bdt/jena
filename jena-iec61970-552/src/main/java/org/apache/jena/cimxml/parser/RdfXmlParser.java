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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class RdfXmlParser {

    final static int MAX_BUFFER_SIZE = 64*4096;

    private final ReaderCIMXML_StAX_SR reader;

    public ErrorHandler getErrorHandler() {
        return reader.errorHandler;
    }

    public RdfXmlParser() {
        this(ErrorHandlerFactory.errorHandlerStd);
    }

    RdfXmlParser(final ReaderCIMXML_StAX_SR reader) {
        this.reader = reader;
    }

    public RdfXmlParser(final ErrorHandler errorHandler) {
        this(new ReaderCIMXML_StAX_SR(errorHandler));
    }

    public CimProfile parseCimProfile(final Path pathToCimProfile) throws IOException {
        return CimProfile.wrap(parseGraph(pathToCimProfile));
    }

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

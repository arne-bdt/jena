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

package org.apache.jena.riot.lang.cimxml;

import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.irix.SystemIRIx;
import org.apache.jena.riot.lang.cimxml.query.StreamCIMXMLToDatasetGraph;
import org.apache.jena.sys.JenaSystem;
import org.junit.Test;

import java.io.StringReader;

import static org.junit.Assert.*;

public class TestParserCIMXMLConformity {


    /**
     * Test that the parser can parse a CIM XML document with a version declaration.
     * And that the version is correctly parsed.
     */
    @Test
    public void testParseCIMXMLVersion() throws Exception {
        final var rdfxml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <?iec61970-552 version="2.0"?>
            <rdf:RDF
            xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
            xmlns:cim="http://iec.ch/TC57/2004/CIM-schema-cim10#">
            </rdf:RDF>
            """;

        Lib.setenv(SystemIRIx.sysPropertyProvider, "IRI3986");
        JenaSystem.init();
        SystemIRIx.reset();
        final var parser = new ReaderCIMXML_StAX_SR();
        final var streamRDF = new StreamCIMXMLToDatasetGraph();

        parser.read(new StringReader(rdfxml), streamRDF);

        assertEquals("version=\"2.0\"", streamRDF.getVersionOfCIMXML());
    }

    /**
     * Test that the parser can parse a CIM XML document without a version declaration.
     */
    @Test
    public void testParseCIMXMLWithoutVersion() throws Exception {
        final var rdfxml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <rdf:RDF
            xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
            xmlns:cim="http://iec.ch/TC57/2004/CIM-schema-cim10#">
            </rdf:RDF>
            """;

        Lib.setenv(SystemIRIx.sysPropertyProvider, "IRI3986");
        JenaSystem.init();
        SystemIRIx.reset();
        final var parser = new ReaderCIMXML_StAX_SR();
        final var streamRDF = new StreamCIMXMLToDatasetGraph();

        parser.read(new StringReader(rdfxml), streamRDF);

        assertNull(streamRDF.getVersionOfCIMXML());
    }
}

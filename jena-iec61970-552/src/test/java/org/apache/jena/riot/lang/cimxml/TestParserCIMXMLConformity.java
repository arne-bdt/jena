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

import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.irix.SystemIRIx;
import org.apache.jena.riot.lang.cimxml.query.StreamCIMXMLToDatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sys.JenaSystem;
import org.junit.Test;

import javax.xml.XMLConstants;
import java.io.StringReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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



    public class StreamRDFGraph implements StreamCIMXML {
        private final Graph graph;
        private String versionOfCIMXML;

        public String getVersionOfCIMXML() {
            return versionOfCIMXML;
        }

        public StreamRDFGraph(Graph graph) {
            this.graph = graph;
        }

        @Override
        public void setVersionOfCIMXML(String versionOfCIMXML) {
            this.versionOfCIMXML = versionOfCIMXML;
        }

        @Override
        public void switchContext(CIMXMLDocumentContext cimDocumentContext) {
            StreamCIMXML.super.switchContext(cimDocumentContext);
        }

        @Override
        public void start() {

        }

        @Override
        public void triple(Triple triple) {
            this.graph.add(triple);
        }

        @Override
        public void quad(Quad quad) {
            throw new UnsupportedOperationException("Not supported.");
        }

        @Override
        public void base(String base) {
            this.graph.getPrefixMapping().setNsPrefix(XMLConstants.DEFAULT_NS_PREFIX, base);
        }

        @Override
        public void prefix(String prefix, String iri) {
            this.graph.getPrefixMapping().setNsPrefix(prefix, iri);
        }

        @Override
        public void finish() {

        }
    }
}

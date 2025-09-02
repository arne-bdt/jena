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

package org.apache.jena.riot.lang.cimxml.graph;

import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.graph.Node;
import org.apache.jena.irix.SystemIRIx;
import org.apache.jena.riot.lang.cimxml.ReaderCIMXML_StAX_SR;
import org.apache.jena.riot.lang.cimxml.query.StreamCIMXMLToDatasetGraph;
import org.apache.jena.sys.JenaSystem;
import org.junit.Test;

import java.io.StringReader;

import static org.junit.Assert.*;

public class TestModelHeader {


    /**
     * Test that the parser can parse a CIM XML document with a version declaration.
     * And that the version is correctly parsed.
     */
    @Test
    public void parseFullModelHeader() throws Exception {
        final var rdfxml = """
            <?xml version="1.0" encoding="utf-8"?>
            <rdf:RDF
                xmlns:md="http://iec.ch/TC57/61970-552/ModelDescription/1#"
                xmlns:cim="http://iec.ch/TC57/CIM100#"
                xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
             <md:FullModel rdf:about="urn:uuid:08984e27-811f-4042-9125-1531ae0de0f6">
               <md:Model.version>003</md:Model.version>
               <md:Model.Supersedes rdf:resource="urn:uuid:f086bea4-3428-4e49-8214-752fdeb1e2e4" />
               <md:Model.DependentOn rdf:resource="urn:uuid:fa274c8c-a346-4080-ba5a-8a4eaa9083f9" />
               <md:Model.profile>http://iec.ch/TC57/ns/CIM/CoreEquipment-EU/3.0</md:Model.profile>
               <md:Model.profile>http://iec.ch/TC57/ns/CIM/MyCIMProfile/3.0</md:Model.profile>
             </md:FullModel>
            </rdf:RDF>
            """;

        Lib.setenv(SystemIRIx.sysPropertyProvider, "IRI3986");
        JenaSystem.init();
        SystemIRIx.reset();
        final var parser = new ReaderCIMXML_StAX_SR();
        final var streamRDF = new StreamCIMXMLToDatasetGraph();

        parser.read(new StringReader(rdfxml), streamRDF);

        assertTrue(streamRDF.getCIMDatasetGraph().isFullModel());
        assertNotNull(streamRDF.getCIMDatasetGraph().getModelHeader());
        var modelHeader = streamRDF.getCIMDatasetGraph().getModelHeader();

        assertEquals("urn:uuid:08984e27-811f-4042-9125-1531ae0de0f6", modelHeader.getModel().toString());

        assertEquals(1, modelHeader.getSupersedes().size());
        assertEquals("urn:uuid:f086bea4-3428-4e49-8214-752fdeb1e2e4",
                modelHeader.getSupersedes().stream().findAny().orElseThrow().getURI());

        assertEquals(1, modelHeader.getDependentOn().size());
        assertEquals("urn:uuid:fa274c8c-a346-4080-ba5a-8a4eaa9083f9",
                modelHeader.getDependentOn().stream().findAny().orElseThrow().getURI());

        assertEquals(2, modelHeader.getProfiles().size());
        assertTrue(modelHeader.getProfiles().stream().map(Node::getLiteralLexicalForm).toList()
                .contains("http://iec.ch/TC57/ns/CIM/CoreEquipment-EU/3.0"));
        assertTrue(modelHeader.getProfiles().stream().map(Node::getLiteralLexicalForm).toList()
                .contains("http://iec.ch/TC57/ns/CIM/MyCIMProfile/3.0"));

    }

    /**
     * Test that the parser can parse a CIM XML document with a version declaration.
     * And that the version is correctly parsed.
     */
    @Test
    public void parseDifferenceModelHeader() throws Exception {
        final var rdfxml = """
            <?xml version="1.0" encoding="utf-8"?>
            <rdf:RDF
                xmlns:dm="http://iec.ch/TC57/61970-552/DifferenceModel/1#"
                xmlns:md="http://iec.ch/TC57/61970-552/ModelDescription/1#"
                xmlns:cim="http://iec.ch/TC57/CIM100#"
                xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
             <dm:DifferenceModel rdf:about="urn:uuid:08984e27-811f-4042-9125-1531ae0de0f6">
               <md:Model.version>003</md:Model.version>
               <md:Model.Supersedes rdf:resource="urn:uuid:f086bea4-3428-4e49-8214-752fdeb1e2e4" />
               <md:Model.DependentOn rdf:resource="urn:uuid:fa274c8c-a346-4080-ba5a-8a4eaa9083f9" />
               <md:Model.profile>http://iec.ch/TC57/ns/CIM/CoreEquipment-EU/3.0</md:Model.profile>
               <md:Model.profile>http://iec.ch/TC57/ns/CIM/MyCIMProfile/3.0</md:Model.profile>
             </dm:DifferenceModel>
            </rdf:RDF>
            """;

        Lib.setenv(SystemIRIx.sysPropertyProvider, "IRI3986");
        JenaSystem.init();
        SystemIRIx.reset();
        final var parser = new ReaderCIMXML_StAX_SR();
        final var streamRDF = new StreamCIMXMLToDatasetGraph();

        parser.read(new StringReader(rdfxml), streamRDF);

        assertTrue(streamRDF.getCIMDatasetGraph().isDifferenceModel());
        assertNotNull(streamRDF.getCIMDatasetGraph().getModelHeader());
        var modelHeader = streamRDF.getCIMDatasetGraph().getModelHeader();

        assertEquals("urn:uuid:08984e27-811f-4042-9125-1531ae0de0f6", modelHeader.getModel().toString());

        assertEquals(1, modelHeader.getSupersedes().size());
        assertEquals("urn:uuid:f086bea4-3428-4e49-8214-752fdeb1e2e4",
                modelHeader.getSupersedes().stream().findAny().orElseThrow().getURI());

        assertEquals(1, modelHeader.getDependentOn().size());
        assertEquals("urn:uuid:fa274c8c-a346-4080-ba5a-8a4eaa9083f9",
                modelHeader.getDependentOn().stream().findAny().orElseThrow().getURI());

        assertEquals(2, modelHeader.getProfiles().size());
        assertTrue(modelHeader.getProfiles().stream().map(Node::getLiteralLexicalForm).toList()
                .contains("http://iec.ch/TC57/ns/CIM/CoreEquipment-EU/3.0"));
        assertTrue(modelHeader.getProfiles().stream().map(Node::getLiteralLexicalForm).toList()
                .contains("http://iec.ch/TC57/ns/CIM/MyCIMProfile/3.0"));


    }

}

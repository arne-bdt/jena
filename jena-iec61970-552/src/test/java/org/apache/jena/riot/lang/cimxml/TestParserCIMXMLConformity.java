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
import org.apache.jena.graph.NodeFactory;
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
    public void parseIEC61970_552Version() throws Exception {
        final var rdfxml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <?iec61970-552 version="2.0"?>
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
            </rdf:RDF>
            """;

        Lib.setenv(SystemIRIx.sysPropertyProvider, "IRI3986");
        JenaSystem.init();
        SystemIRIx.reset();
        final var parser = new ReaderCIMXML_StAX_SR();
        final var streamRDF = new StreamCIMXMLToDatasetGraph();

        parser.read(new StringReader(rdfxml), streamRDF);

        assertEquals("version=\"2.0\"", streamRDF.getVersionOfIEC61970_552());
    }

    /**
     * Test that the parser can parse a CIM XML document without a version declaration.
     */
    @Test
    public void parseWithoutIEC61970_552Version() throws Exception {
        final var rdfxml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
            </rdf:RDF>
            """;

        Lib.setenv(SystemIRIx.sysPropertyProvider, "IRI3986");
        JenaSystem.init();
        SystemIRIx.reset();
        final var parser = new ReaderCIMXML_StAX_SR();
        final var streamRDF = new StreamCIMXMLToDatasetGraph();

        parser.read(new StringReader(rdfxml), streamRDF);

        assertNull(streamRDF.getVersionOfIEC61970_552());
    }

    @Test
    public void parseCIMVersion17() throws Exception {
        final var rdfxml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <rdf:RDF 
                xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                xmlns:cim="http://iec.ch/TC57/CIM100#">
            </rdf:RDF>
            """;

        Lib.setenv(SystemIRIx.sysPropertyProvider, "IRI3986");
        JenaSystem.init();
        SystemIRIx.reset();
        final var parser = new ReaderCIMXML_StAX_SR();
        final var streamRDF = new StreamCIMXMLToDatasetGraph();

        parser.read(new StringReader(rdfxml), streamRDF);

        assertEquals(StreamCIMXML.CIMXMLVersion.CIM_17, streamRDF.getVersionOfCIMXML());
    }

    @Test
    public void parseCIMVersion18() throws Exception {
        final var rdfxml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <rdf:RDF
                xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                xmlns:cim="https://cim.ucaiug.io/ns#">
            </rdf:RDF>
            """;

        Lib.setenv(SystemIRIx.sysPropertyProvider, "IRI3986");
        JenaSystem.init();
        SystemIRIx.reset();
        final var parser = new ReaderCIMXML_StAX_SR();
        final var streamRDF = new StreamCIMXMLToDatasetGraph();

        parser.read(new StringReader(rdfxml), streamRDF);

        assertEquals(StreamCIMXML.CIMXMLVersion.CIM_18, streamRDF.getVersionOfCIMXML());
    }

    @Test
    public void parseFullModelAndContentInDifferentGraphs() {
        final var rdfxml = """
            <?xml version="1.0" encoding="utf-8"?>
            <rdf:RDF xmlns:cim="http://iec.ch/TC57/CIM100#" xmlns:md="http://iec.ch/TC57/61970-552/ModelDescription/1#" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:eu="http://iec.ch/TC57/CIM100-European#">
             <md:FullModel rdf:about="urn:uuid:08984e27-811f-4042-9125-1531ae0de0f6">
               <md:Model.profile>http://iec.ch/TC57/ns/CIM/CoreEquipment-EU/3.0</md:Model.profile>
             </md:FullModel>
             <cim:MyEquipment rdf:ID="_f67fc354-9e39-4191-a456-67537399bc48">
               <cim:IdentifiedObject.name>My Custom Equipment</cim:IdentifiedObject.name>
             </cim:MyEquipment>
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
        assertEquals(1, modelHeader.getProfiles().toList().size());
        assertTrue(modelHeader.getProfiles().map(node -> node.getLiteralLexicalForm()).toList()
                .contains("http://iec.ch/TC57/ns/CIM/CoreEquipment-EU/3.0"));

        var graph = streamRDF.getCIMDatasetGraph().getDefaultGraph();
        assertTrue(graph.contains(
                NodeFactory.createURI("urn:uuid:f67fc354-9e39-4191-a456-67537399bc48"),
                NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
                NodeFactory.createURI("http://iec.ch/TC57/CIM100#MyEquipment")
        ));
        assertTrue(graph.contains(
                NodeFactory.createURI("urn:uuid:f67fc354-9e39-4191-a456-67537399bc48"),
                NodeFactory.createURI("http://iec.ch/TC57/CIM100#IdentifiedObject.name"),
                NodeFactory.createLiteralString("My Custom Equipment")
        ));
    }

    @Test
    public void replaceUnderscoresInRdfAboutAndRdfId() {
        final var rdfxml = """
            <?xml version="1.0" encoding="utf-8"?>
            <rdf:RDF xmlns:cim="http://iec.ch/TC57/CIM100#" xmlns:md="http://iec.ch/TC57/61970-552/ModelDescription/1#" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:eu="http://iec.ch/TC57/CIM100-European#">
              <cim:MyEquipment rdf:ID="_f67fc354-9e39-4191-a456-67537399bc48">
                <cim:IdentifiedObject.name>My Custom Equipment</cim:IdentifiedObject.name>
              </cim:MyEquipment>
              <cim:MyEquipment rdf:about="#_f67fc354-9e39-4191-a456-67537399bc48">
                <cim:MyEquipment.MyReference rdf:resource="#_d597b77b-c8c4-4d88-883e-f516eedb913b" />
              </cim:MyEquipment>
            </rdf:RDF>
            """;

        Lib.setenv(SystemIRIx.sysPropertyProvider, "IRI3986");
        JenaSystem.init();
        SystemIRIx.reset();
        final var parser = new ReaderCIMXML_StAX_SR();
        final var streamRDF = new StreamCIMXMLToDatasetGraph();

        parser.read(new StringReader(rdfxml), streamRDF);

        var graph = streamRDF.getCIMDatasetGraph().getDefaultGraph();
        assertTrue(graph.contains(
                NodeFactory.createURI("urn:uuid:f67fc354-9e39-4191-a456-67537399bc48"),
                NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
                NodeFactory.createURI("http://iec.ch/TC57/CIM100#MyEquipment")
        ));
        assertTrue(graph.contains(
                NodeFactory.createURI("urn:uuid:f67fc354-9e39-4191-a456-67537399bc48"),
                NodeFactory.createURI("http://iec.ch/TC57/CIM100#IdentifiedObject.name"),
                NodeFactory.createLiteralString("My Custom Equipment")
        ));
        assertTrue(graph.contains(
                NodeFactory.createURI("urn:uuid:f67fc354-9e39-4191-a456-67537399bc48"),
                NodeFactory.createURI("http://iec.ch/TC57/CIM100#MyEquipment.MyReference"),
                NodeFactory.createURI("urn:uuid:d597b77b-c8c4-4d88-883e-f516eedb913b")
        ));
    }

    @Test
    public void replaceUnderscoresInRdfAboutAndRdfIdFixingMissingDashesInUuids() {
        final var rdfxml = """
            <?xml version="1.0" encoding="utf-8"?>
            <rdf:RDF xmlns:cim="http://iec.ch/TC57/CIM100#" xmlns:md="http://iec.ch/TC57/61970-552/ModelDescription/1#" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:eu="http://iec.ch/TC57/CIM100-European#">
              <cim:MyEquipment rdf:ID="_f67fc3549e394191a45667537399bc48">
                <cim:IdentifiedObject.name>My Custom Equipment</cim:IdentifiedObject.name>
              </cim:MyEquipment>
              <cim:MyEquipment rdf:about="#_f67fc3549e394191a45667537399bc48">
                <cim:MyEquipment.MyReference rdf:resource="#_d597b77bc8c44d88883ef516eedb913b" />
              </cim:MyEquipment>
            </rdf:RDF>
            """;

        Lib.setenv(SystemIRIx.sysPropertyProvider, "IRI3986");
        JenaSystem.init();
        SystemIRIx.reset();
        final var parser = new ReaderCIMXML_StAX_SR();
        final var streamRDF = new StreamCIMXMLToDatasetGraph();

        parser.read(new StringReader(rdfxml), streamRDF);

        var graph = streamRDF.getCIMDatasetGraph().getDefaultGraph();
        assertTrue(graph.contains(
                NodeFactory.createURI("urn:uuid:f67fc354-9e39-4191-a456-67537399bc48"),
                NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
                NodeFactory.createURI("http://iec.ch/TC57/CIM100#MyEquipment")
        ));
        assertTrue(graph.contains(
                NodeFactory.createURI("urn:uuid:f67fc354-9e39-4191-a456-67537399bc48"),
                NodeFactory.createURI("http://iec.ch/TC57/CIM100#IdentifiedObject.name"),
                NodeFactory.createLiteralString("My Custom Equipment")
        ));
        assertTrue(graph.contains(
                NodeFactory.createURI("urn:uuid:f67fc354-9e39-4191-a456-67537399bc48"),
                NodeFactory.createURI("http://iec.ch/TC57/CIM100#MyEquipment.MyReference"),
                NodeFactory.createURI("urn:uuid:d597b77b-c8c4-4d88-883e-f516eedb913b")
        ));

    }

}

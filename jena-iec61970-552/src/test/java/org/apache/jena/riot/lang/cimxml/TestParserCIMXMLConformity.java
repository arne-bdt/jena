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
import org.apache.jena.cimxml.CIMVersion;
import org.apache.jena.cimxml.parser.ReaderCIMXML_StAX_SR;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.irix.SystemIRIx;
import org.apache.jena.cimxml.parser.system.StreamCIMXMLToDatasetGraph;
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

        assertEquals(CIMVersion.CIM_17, streamRDF.getVersionOfCIMXML());
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

        assertEquals(CIMVersion.CIM_18, streamRDF.getVersionOfCIMXML());
    }

    @Test
    public void parseFullModelHeaderAndContentInDifferentGraphs() {
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
        assertEquals(1, modelHeader.getProfiles().size());
        assertTrue(modelHeader.getProfiles().stream().map(node -> node.getLiteralLexicalForm()).toList()
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
    public void parseDifferenceModelHeaderAndContentInDifferentGraphs() throws Exception {
        final var rdfxml = """
            <?xml version="1.0" encoding="utf-8"?>
            <rdf:RDF
                xmlns:dm="http://iec.ch/TC57/61970-552/DifferenceModel/1#"
                xmlns:md="http://iec.ch/TC57/61970-552/ModelDescription/1#"
                xmlns:cim="http://iec.ch/TC57/CIM100#"
                xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
             <dm:DifferenceModel rdf:about="urn:uuid:08984e27-811f-4042-9125-1531ae0de0f6">

                <dm:preconditions rdf:parseType="Statements">

                    <!-- expect the following element to be present in the model
                         before and after applying the differences -->
                    <rdf:Description rdf:about="#_135c601e-bad4-4872-ba8f-b15baf91bd2f">
                        <cim:IdentifiedObject.name>Name of my element</cim:IdentifiedObject.name>
                    </rdf:Description>

                </dm:preconditions>

                <dm:forwardDifferences rdf:parseType="Statements">

                    <!-- add the following property to the model (delete + add = update) -->
                    <rdf:Description rdf:about="#_135c601e-bad4-4872-ba8f-b15baf91bd2f">
                        <cim:MyElement.MyProperty>B</cim:MyElement.MyProperty>
                    </rdf:Description>

                    <!-- add the following new resource to the model -->
                    <cim:MyElement rdf:about="#_2d1e4820-8858-49de-b441-5a03e7c40035">
                        <cim:IdentifiedObject.name>Name of new element to add</cim:IdentifiedObject.name>
                        <cim:MyElement.MyProperty>property of new element</cim:MyElement.MyProperty>
                    </cim:MyElement>

                </dm:forwardDifferences>

                <dm:reverseDifferences rdf:parseType="Statements">

                    <!-- remove the following property from the model (delete + add = update) -->
                    <rdf:Description rdf:about="#_135c601e-bad4-4872-ba8f-b15baf91bd2f">
                        <cim:MyElement.MyProperty>A</cim:MyElement.MyProperty>
                    </rdf:Description>

                    <!-- remove the following resource from the model -->
                    <cim:MyElement rdf:about="#_c9fe6664-fcf0-44e6-9d20-656538b68d1c">
                        <cim:IdentifiedObject.name>Name of new element to remove entirely</cim:IdentifiedObject.name>
                        <cim:MyElement.MyProperty>property of new element to remove</cim:MyElement.MyProperty>
                    </cim:MyElement>

                </dm:reverseDifferences>

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

        var preconditions = streamRDF.getCIMDatasetGraph().getPreconditions();
        assertNotNull(preconditions);
        assertEquals(1, preconditions.size());
        assertTrue(preconditions.contains(
                NodeFactory.createURI("urn:uuid:135c601e-bad4-4872-ba8f-b15baf91bd2f"),
                NodeFactory.createURI("http://iec.ch/TC57/CIM100#IdentifiedObject.name"),
                NodeFactory.createLiteralString("Name of my element")
        ));

        var forwardDifferences = streamRDF.getCIMDatasetGraph().getForwardDifferences();
        assertNotNull(forwardDifferences);
        assertEquals(4, forwardDifferences.size());
        assertTrue(forwardDifferences.contains(
                NodeFactory.createURI("urn:uuid:135c601e-bad4-4872-ba8f-b15baf91bd2f"),
                NodeFactory.createURI("http://iec.ch/TC57/CIM100#MyElement.MyProperty"),
                NodeFactory.createLiteralString("B")
        ));
        assertTrue(forwardDifferences.contains(
                NodeFactory.createURI("urn:uuid:2d1e4820-8858-49de-b441-5a03e7c40035"),
                NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
                NodeFactory.createURI("http://iec.ch/TC57/CIM100#MyElement")
        ));
        assertTrue(forwardDifferences.contains(
                NodeFactory.createURI("urn:uuid:2d1e4820-8858-49de-b441-5a03e7c40035"),
                NodeFactory.createURI("http://iec.ch/TC57/CIM100#IdentifiedObject.name"),
                NodeFactory.createLiteralString("Name of new element to add")
        ));
        assertTrue(forwardDifferences.contains(
                NodeFactory.createURI("urn:uuid:2d1e4820-8858-49de-b441-5a03e7c40035"),
                NodeFactory.createURI("http://iec.ch/TC57/CIM100#MyElement.MyProperty"),
                NodeFactory.createLiteralString("property of new element")
        ));

        var reverseDifferences = streamRDF.getCIMDatasetGraph().getReverseDifferences();
        assertNotNull(reverseDifferences);
        assertEquals(4, reverseDifferences.size());
        assertTrue(reverseDifferences.contains(
                NodeFactory.createURI("urn:uuid:135c601e-bad4-4872-ba8f-b15baf91bd2f"),
                NodeFactory.createURI("http://iec.ch/TC57/CIM100#MyElement.MyProperty"),
                NodeFactory.createLiteralString("A")
        ));
        assertTrue(reverseDifferences.contains(
                NodeFactory.createURI("urn:uuid:c9fe6664-fcf0-44e6-9d20-656538b68d1c"),
                NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
                NodeFactory.createURI("http://iec.ch/TC57/CIM100#MyElement")
        ));
        assertTrue(reverseDifferences.contains(
                NodeFactory.createURI("urn:uuid:c9fe6664-fcf0-44e6-9d20-656538b68d1c"),
                NodeFactory.createURI("http://iec.ch/TC57/CIM100#IdentifiedObject.name"),
                NodeFactory.createLiteralString("Name of new element to remove entirely")
        ));
        assertTrue(reverseDifferences.contains(
                NodeFactory.createURI("urn:uuid:c9fe6664-fcf0-44e6-9d20-656538b68d1c"),
                NodeFactory.createURI("http://iec.ch/TC57/CIM100#MyElement.MyProperty"),
                NodeFactory.createLiteralString("property of new element to remove")
        ));
    }

    @Test
    public void writePrefixesToAllGraphInDifferenceModel() throws Exception {
        final var rdfxml = """
            <?xml version="1.0" encoding="utf-8"?>
            <rdf:RDF
                xmlns:dm="http://iec.ch/TC57/61970-552/DifferenceModel/1#"
                xmlns:md="http://iec.ch/TC57/61970-552/ModelDescription/1#"
                xmlns:cim="http://iec.ch/TC57/CIM100#"
                xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
             <dm:DifferenceModel rdf:about="urn:uuid:08984e27-811f-4042-9125-1531ae0de0f6">
                
                <dm:preconditions rdf:parseType="Statements">
                </dm:preconditions>

                <dm:forwardDifferences rdf:parseType="Statements">
                </dm:forwardDifferences>

                <dm:reverseDifferences rdf:parseType="Statements">
                </dm:reverseDifferences>

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

        assertEquals(4, modelHeader.getPrefixMapping().numPrefixes());
        assertEquals("urn:uuid:08984e27-811f-4042-9125-1531ae0de0f6", modelHeader.getModel().toString());

        var preconditions = streamRDF.getCIMDatasetGraph().getPreconditions();
        assertNotNull(preconditions);
        assertTrue(preconditions.getPrefixMapping().samePrefixMappingAs(modelHeader.getPrefixMapping()));
        assertEquals(0, preconditions.size());

        var forwardDifferences = streamRDF.getCIMDatasetGraph().getForwardDifferences();
        assertNotNull(forwardDifferences);
        assertTrue(forwardDifferences.getPrefixMapping().samePrefixMappingAs(modelHeader.getPrefixMapping()));
        assertEquals(0, forwardDifferences.size());

        var reverseDifferences = streamRDF.getCIMDatasetGraph().getReverseDifferences();
        assertNotNull(reverseDifferences);
        assertTrue(reverseDifferences.getPrefixMapping().samePrefixMappingAs(modelHeader.getPrefixMapping()));
        assertEquals(0, reverseDifferences.size());
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

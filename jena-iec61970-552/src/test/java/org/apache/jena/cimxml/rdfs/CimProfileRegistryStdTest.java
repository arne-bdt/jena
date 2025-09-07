package org.apache.jena.cimxml.rdfs;

import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.cimxml.graph.CimProfile;
import org.apache.jena.cimxml.parser.ReaderCIMXML_StAX_SR;
import org.apache.jena.cimxml.parser.system.StreamCIMXMLToDatasetGraph;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.irix.SystemIRIx;
import org.apache.jena.sys.JenaSystem;
import org.junit.Test;

import java.io.StringReader;
import java.util.Set;

import static org.junit.Assert.*;

public class CimProfileRegistryStdTest {

    @Test
    public void parseProfileWithOneClassAndTwoSimpleProperties() throws Exception {
        final var rdfxml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <rdf:RDF
               xmlns:cim="http://iec.ch/TC57/CIM100#"
               xmlns:cims="http://iec.ch/TC57/1999/rdf-schema-extensions-19990926#"
               xmlns:dcat="http://www.w3.org/ns/dcat#"
               xmlns:owl="http://www.w3.org/2002/07/owl#"
               xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
               xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
               xml:base ="http://iec.ch/TC57/CIM100">
                <!-- ······························································································· -->
                <rdf:Description rdf:about="http://iec.ch/TC57/ns/CIM/CoreEquipment-EU#Ontology">
                    <dcat:keyword>MYCUST</dcat:keyword>
                    <owl:versionIRI rdf:resource="http://example.org/MyCustom/1/1"/>
                    <owl:versionInfo xml:lang ="en">1.1.0</owl:versionInfo>
                   <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#Ontology"/>
                </rdf:Description >
                <!-- ······························································································· -->
                <rdf:Description rdf:about="#ClassA">
                    <rdf:type rdf:resource="http://www.w3.org/2000/01/rdf-schema#Class"/>
                    <rdfs:subClassOf rdf:resource="#IdentifiedObject"/>
                    <cims:stereotype rdf:resource="http://iec.ch/TC57/NonStandard/UML#concrete"/>
                </rdf:Description>
                <!-- ······························································································· -->
                <rdf:Description rdf:about="#ClassA.floatProperty">
                    <rdf:type rdf:resource="http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"/>
                    <cims:stereotype rdf:resource="http://iec.ch/TC57/NonStandard/UML#attribute"/>
                    <rdfs:domain rdf:resource="#ClassA"/>
                    <cims:dataType rdf:resource="#Float"/>
                 </rdf:Description>
                <!-- ······························································································· -->
                <rdf:Description rdf:about="#ClassA.textProperty">
                    <rdf:type rdf:resource="http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"/>
                    <cims:stereotype rdf:resource="http://iec.ch/TC57/NonStandard/UML#attribute"/>
                    <rdfs:domain rdf:resource="#ClassA"/>
                    <cims:dataType rdf:resource="#String"/>
                </rdf:Description>
                <!-- ······························································································· -->
                <rdf:Description rdf:about="#Float">
                    <rdfs:label xml:lang="en">Float</rdfs:label>
                    <cims:stereotype>Primitive</cims:stereotype>
                    <rdf:type rdf:resource="http://www.w3.org/2000/01/rdf-schema#Class"/>
                </rdf:Description>
                <!-- ······························································································· -->
                <rdf:Description rdf:about="#String">
                    <rdfs:label xml:lang="en">String</rdfs:label>
                    <cims:stereotype>Primitive</cims:stereotype>
                    <rdf:type rdf:resource="http://www.w3.org/2000/01/rdf-schema#Class"/>
                </rdf:Description>
            </rdf:RDF>
            """;

        Lib.setenv(SystemIRIx.sysPropertyProvider, "IRI3986");
        JenaSystem.init();
        SystemIRIx.reset();

        final var parser = new ReaderCIMXML_StAX_SR();
        final var streamRDF = new StreamCIMXMLToDatasetGraph();

        parser.read(new StringReader(rdfxml), streamRDF);

        var graph = streamRDF.getCIMDatasetGraph().getDefaultGraph();

        var profile = CimProfile.wrap(graph);

        var registry = new CimProfileRegistryStd();
        registry.register(profile);

        var owlVersionIRIs = Set.of(NodeFactory.createURI("http://example.org/MyCustom/1/1"));

        assertTrue(registry.containsProfile(owlVersionIRIs));

        assertTrue(registry.getRegisteredProfiles().contains(profile));
        assertEquals(1, registry.getRegisteredProfiles().size());

        var properties = registry.getPropertiesAndDatatypes(owlVersionIRIs);
        assertNotNull(properties);
        assertEquals(2, properties.size());

        var floatProperty = NodeFactory.createURI("http://iec.ch/TC57/CIM100#ClassA.floatProperty");
        assertTrue(properties.containsKey(floatProperty));
        var propertyInfo = properties.get(floatProperty);
        assertEquals(NodeFactory.createURI("http://iec.ch/TC57/CIM100#ClassA"), propertyInfo.rdfType());
        assertEquals(floatProperty, propertyInfo.property());
        assertEquals(XSDDatatype.XSDfloat, propertyInfo.primitiveType());
        assertNull(propertyInfo.referenceType());

        var textProperty = NodeFactory.createURI("http://iec.ch/TC57/CIM100#ClassA.textProperty");
        assertTrue(properties.containsKey(textProperty));
        propertyInfo = properties.get(textProperty);
        assertEquals(NodeFactory.createURI("http://iec.ch/TC57/CIM100#ClassA"), propertyInfo.rdfType());
        assertEquals(textProperty, propertyInfo.property());
        assertEquals(XSDDatatype.XSDstring, propertyInfo.primitiveType());
        assertNull(propertyInfo.referenceType());
    }

}
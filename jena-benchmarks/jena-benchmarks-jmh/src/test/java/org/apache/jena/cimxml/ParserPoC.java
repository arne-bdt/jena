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

package org.apache.jena.cimxml;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.GraphMem2Fast;
import org.apache.jena.mem2.GraphMem2Roaring;
import org.apache.jena.mem2.IndexingStrategy;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sys.JenaSystem;
import org.junit.Test;

import javax.xml.XMLConstants;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ParserPoC {

    /* Things to support:
     * <dm:forwardDifferences parseType=\"Statements\"
     * rdf:RDF | rdf:ID | rdf:about | rdf:parseType | rdf:resource | rdf:nodeID | rdf:datatype
     *  rdf:langString
     * | rdf:Description |
     * rdf:parseType="Resource" | rdf:parseType="Statement"
     *
     * Things, that are not supported:
     * <ul>
     *  <li>Namespace declarations are only supported in the rdf:RDF tag.
     *  <li>rdf:parseType="Collection" is not supported.
     *  <li>Reifying statements using rdf:ID is not supported.
     *  <li>rdf:parseType="Literal" is not supported.
     *  <li>rdf:li is not supported.
     * </ul>
     */

    //private final String file = "C:\\temp\\CGMES_v2.4.15_TestConfigurations_v4.0.3\\MicroGrid\\BaseCase_BC\\CGMES_v2.4.15_MicroGridTestConfiguration_BC_Assembled_CA_v2\\MicroGridTestConfiguration_BC_NL_GL_V2.xml";
    private final String file = "C:\\temp\\v59_3\\AMP_Export_s82_v58_H69.xml";



    public class StreamRDFGraph implements StreamRDF {
        private final Graph graph;

        public StreamRDFGraph(Graph graph) {
            this.graph = graph;
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

    @Test
    public void testTextParser() throws Exception {
        JenaSystem.init();
        final var xmlString = """
                <?xml version="1.0" encoding="utf-8"?>
                <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                            xmlns:dc="http://purl.org/dc/elements/1.1/">
                
                  <rdf:Description rdf:about="http://www.w3.org/TR/rdf-syntax-grammar">
                    <dc:title>RDF 1.1 XML Syntax</dc:title>
                    <dc:title xml:lang="en">RDF 1.1 XML Syntax</dc:title>
                    <dc:title xml:lang="en-US">RDF 1.1 XML Syntax</dc:title>
                  </rdf:Description>
                
                  <rdf:Description rdf:about="http://example.org/buecher/baum" xml:lang="de">
                    <dc:title>Der Baum</dc:title>
                    <dc:description>Das Buch ist außergewöhnlich</dc:description>
                    <dc:title xml:lang="en">The Tree</dc:title>
                  </rdf:Description>
                
                </rdf:RDF>
                """;
        final var is = new ByteArrayInputStream(xmlString.getBytes(StandardCharsets.UTF_8));
        final var graph = new GraphMem2Roaring(IndexingStrategy.LAZY);
        final var expectedGraph = new GraphMem2Roaring(IndexingStrategy.LAZY);
        final var parser = new CIMParser(is, new StreamRDFGraph(graph));

        final var stopWatch = StopWatch.createStarted();
        parser.parse();
        stopWatch.stop();
        // print number of triples parsed and the time taken
        System.out.println("Parsed triples: " + graph.size() + " in " + stopWatch);

        stopWatch.reset();
        stopWatch.start();
        RDFParser.create()
                .source(new ByteArrayInputStream(xmlString.getBytes(StandardCharsets.UTF_8)))
                .lang(org.apache.jena.riot.Lang.RDFXML)
                .parse(new StreamRDFGraph(expectedGraph));
        stopWatch.stop();

        // print number of triples parsed and the time taken
        System.out.println("Parsed expected triples: " + expectedGraph.size() + " in " + stopWatch);

        assertGraphsEqual(expectedGraph, graph);
    }

    @Test
    public void testCimXml() throws Exception {
        JenaSystem.init();
        final var xmlString = """
               <?xml version="1.0" encoding="utf-8"?>
               <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:cim="http://iec.ch/TC57/2013/CIM-schema-cim16#" xmlns:entsoe="http://entsoe.eu/CIM/SchemaExtension/3/1#" xmlns:md="http://iec.ch/TC57/61970-552/ModelDescription/1#">
                 <md:FullModel rdf:about="urn:uuid:4528b685-6ddc-471a-b8a2-8e77e6ec15f8">
                   <md:Model.created>2014-10-24T11:56:40</md:Model.created>
                   <md:Model.scenarioTime>2014-06-01T10:30:00</md:Model.scenarioTime>
                   <md:Model.version>2</md:Model.version>
                   <md:Model.DependentOn rdf:resource="urn:uuid:77b55f87-fc1e-4046-9599-6c6b4f991a86"/>
                   <md:Model.DependentOn rdf:resource="urn:uuid:2399cbd0-9a39-11e0-aa80-0800200c9a66"/>
                   <md:Model.description>CGMES Conformity Assessment: 'MicroGridTestConfiguration....BC (Assembled)Test Configuration. The model is owned by ENTSO-E and is provided by ENTSO-E “as it is”. To the fullest extent permitted by law, ENTSO-E shall not be liable for any damages of any kind arising out of the use of the model (including any of its subsequent modifications). ENTSO-E neither warrants, nor represents that the use of the model will not infringe the rights of third parties. Any use of the model shall  include a reference to ENTSO-E. ENTSO-E web site is the only official source of information related to the model.</md:Model.description>
                   <md:Model.modelingAuthoritySet>http://tennet.nl/CGMES/2.4.15</md:Model.modelingAuthoritySet>
                   <md:Model.profile>http://entsoe.eu/CIM/GeographicalLocation/2/1</md:Model.profile>
                 </md:FullModel>
                 <cim:CoordinateSystem rdf:ID="_50a38719-492c-4622-bba3-e99f0847be1c">
                   <cim:IdentifiedObject.name>WGS84</cim:IdentifiedObject.name>
                   <cim:CoordinateSystem.crsUrn>urn:ogc:def:crs:EPSG::4326</cim:CoordinateSystem.crsUrn>
                 </cim:CoordinateSystem>
                 <cim:Location rdf:ID="_37c3f6d0-1deb-48a8-92dd-18c80617073f">
                   <cim:Location.PowerSystemResources rdf:resource="#_c49942d6-8b01-4b01-b5e8-f1180f84906c"/>
                   <cim:Location.CoordinateSystem rdf:resource="#_50a38719-492c-4622-bba3-e99f0847be1c"/>
                 </cim:Location>
                 <cim:PositionPoint rdf:ID="_ed286b99-f37c-4677-8555-f8489d953cfa">
                   <cim:PositionPoint.sequenceNumber>1</cim:PositionPoint.sequenceNumber>
                   <cim:PositionPoint.Location rdf:resource="#_37c3f6d0-1deb-48a8-92dd-18c80617073f"/>
                   <cim:PositionPoint.xPosition>4.846580</cim:PositionPoint.xPosition>
                   <cim:PositionPoint.yPosition>52.404700</cim:PositionPoint.yPosition>
                 </cim:PositionPoint>
                </rdf:RDF>
                """;
        final var is = new ByteArrayInputStream(xmlString.getBytes(StandardCharsets.UTF_8));
        final var graph = new GraphMem2Roaring(IndexingStrategy.LAZY);
        final var expectedGraph = new GraphMem2Roaring(IndexingStrategy.LAZY);
        final var parser = new CIMParser(is, new StreamRDFGraph(graph));

        final var stopWatch = StopWatch.createStarted();
        parser.setBaseNamespace("urn:uuid:");
        parser.doNotHandleCimUuidsWithMissingPrefix();
        parser.treatRdfIdStandardConformant();
        parser.parse();
        stopWatch.stop();
        // print number of triples parsed and the time taken
        System.out.println("Parsed triples: " + graph.size() + " in " + stopWatch);

        stopWatch.reset();
        stopWatch.start();
        RDFParser.create()
                .source(new ByteArrayInputStream(xmlString.getBytes(StandardCharsets.UTF_8)))
                .base("urn:uuid:")
                .lang(org.apache.jena.riot.Lang.RDFXML)
                .checking(false)
                .parse(new StreamRDFGraph(expectedGraph));
        stopWatch.stop();

        // print number of triples parsed and the time taken
        System.out.println("Parsed expected triples: " + expectedGraph.size() + " in " + stopWatch);

        assertGraphsEqual(expectedGraph, graph);
    }

    @Test
    public void testRdfId() throws Exception {
        JenaSystem.init();
        final var xmlString = """
               <?xml version="1.0"?>
               <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                           xmlns:ex="http://example.org/stuff/1.0/"
                           xml:base="http://example.org/here/">
    
                 <rdf:Description rdf:ID="snack">
                   <ex:prop rdf:resource="fruit/apple"/>
                 </rdf:Description>
    
               </rdf:RDF>
                """;
        final var is = new ByteArrayInputStream(xmlString.getBytes(StandardCharsets.UTF_8));
        final var graph = new GraphMem2Roaring(IndexingStrategy.LAZY);
        final var expectedGraph = new GraphMem2Roaring(IndexingStrategy.LAZY);
        final var parser = new CIMParser(is, new StreamRDFGraph(graph));

        final var stopWatch = StopWatch.createStarted();
        parser.setBaseNamespace("urn:uuid");
        parser.doNotHandleCimUuidsWithMissingPrefix();
        parser.treatRdfIdStandardConformant();
        parser.parse();
        stopWatch.stop();
        // print number of triples parsed and the time taken
        System.out.println("Parsed triples: " + graph.size() + " in " + stopWatch);

        stopWatch.reset();
        stopWatch.start();
        RDFParser.create()
                .source(new ByteArrayInputStream(xmlString.getBytes(StandardCharsets.UTF_8)))
                .base("urn:uuid")
                .lang(org.apache.jena.riot.Lang.RDFXML)
                .checking(false)
                .parse(new StreamRDFGraph(expectedGraph));
        stopWatch.stop();

        // print number of triples parsed and the time taken
        System.out.println("Parsed expected triples: " + expectedGraph.size() + " in " + stopWatch);

        assertGraphsEqual(expectedGraph, graph);
    }

    @Test
    public void profileFileParser() throws Exception {
        JenaSystem.init();
        final var filePath = java.nio.file.Paths.get(file);
        final var graph = new GraphMem2Roaring(IndexingStrategy.LAZY);
        final var parser = new CIMParser(filePath, new StreamRDFGraph(graph));
        final var stopWatch = StopWatch.createStarted();
        parser.parse();
        stopWatch.stop();
        // print number of triples parsed and the time taken
        System.out.println("Parsed triples: " + graph.size() + " in " + stopWatch);
    }

    @Test
    public void profileFileDefaultParser() throws Exception {
        JenaSystem.init();
        final var filePath = java.nio.file.Paths.get(file);
        final var graph = new GraphMem2Roaring(IndexingStrategy.LAZY);
        final var stopWatch = StopWatch.createStarted();
        RDFParser.create()
                .source(filePath)
                .lang(org.apache.jena.riot.Lang.RDFXML)
                .parse(new StreamRDFGraph(graph));
        stopWatch.stop();
        // print number of triples parsed and the time taken
        System.out.println("Parsed triples: " + graph.size() + " in " + stopWatch);
    }

    @Test
    public void testFileParser() throws Exception {
        JenaSystem.init();
        final var filePath = java.nio.file.Paths.get(file);
        final var graph = new GraphMem2Roaring(IndexingStrategy.LAZY);
        final var expectedGraph = new GraphMem2Roaring(IndexingStrategy.LAZY);
        final var parser = new CIMParser(filePath, new StreamRDFGraph(graph));

        final var stopWatch = StopWatch.createStarted();
        parser.parse();
        stopWatch.stop();
        // print number of triples parsed and the time taken
        System.out.println("Parsed triples: " + graph.size() + " in " + stopWatch);

        stopWatch.reset();
        stopWatch.start();
        RDFParser.create()
                .source(filePath)
                .lang(org.apache.jena.riot.Lang.RDFXML)
                .parse(new StreamRDFGraph(expectedGraph));
        stopWatch.stop();

        // print number of triples parsed and the time taken
        System.out.println("Parsed expected triples: " + expectedGraph.size() + " in " + stopWatch);

        assertGraphsEqual(expectedGraph, graph);
    }

    public void assertGraphsEqual(Graph expected, Graph actual) {
        // check graph sizes
        assertEquals("Graphs are not equal: different sizes.",
                expected.size(), actual.size());
        // check that all triples in expected graph are in actual graph
        expected.find().forEachRemaining(expectedTriple -> {
            if(!actual.contains(expectedTriple)) {
                int i= 0;
            }
            assertTrue("Graphs are not equal: missing triple " + expectedTriple,
                    actual.contains(expectedTriple));
        });

        // check namespace mappings size
        assertEquals("Graphs are not equal: different number of namespaces.",
                expected.getPrefixMapping().numPrefixes(), actual.getPrefixMapping().numPrefixes());

        // check that all namespaces in expected graph are in actual graph
        expected.getPrefixMapping().getNsPrefixMap().forEach((prefix, uri) -> {
            assertTrue("Graphs are not equal: missing namespace " + prefix + " -> " + uri,
                    actual.getPrefixMapping().getNsPrefixMap().containsKey(prefix));
            assertEquals("Graphs are not equal: different URI for namespace " + prefix,
                    uri, actual.getPrefixMapping().getNsPrefixMap().get(prefix));
        });
    }



    @Test
    public void testQuery() throws Exception {
        final var stopWatch = StopWatch.createStarted();
        final var filePath = java.nio.file.Paths.get(file);
        final var subject = NodeFactory.createURI("cim:Location");
        final var predicate = NodeFactory.createURI("cim:Location.name");
        final var object = NodeFactory.createLiteralString("Test Location");
        final var graph = new GraphMem2Fast();
        graph.getPrefixMapping().setNsPrefix("cim", "http://iec.ch/TC57/2013/CIM-schema-cim16#");
        graph.add(subject, predicate, object);

        Query query = QueryFactory.create("""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX cims: <http://iec.ch/TC57/1999/rdf-schema-extensions-19990926#>
        PREFIX cim: <http://iec.ch/TC57/2013/CIM-schema-cim16#>
        SELECT ?s ?p ?o WHERE { ?s ?p ?o }
        """);
        QueryExec.graph(graph)
                .query(query)
                .select()
                .forEachRemaining(vars -> {
                    System.out.println(vars.toString());
                });


        // Create a CIMParserPoC instance
        //CIMXMLParser parser = new CIMXMLParser(filePath, 64 * 4096); // 256 KB
        //CIMXMLParser.ParseResult result = parser.parse(); ByteBuffer.wrap("sadasd".getBytes(charset));

        // Access parsed data
        //Map<String, String> namespaces = result.namespaces;
        //List<CIMXMLParser.Element> elements = result.rootElements;

        // Print summary
        //result.printSummary();
        stopWatch.stop();
        System.out.println(stopWatch);
    }
}

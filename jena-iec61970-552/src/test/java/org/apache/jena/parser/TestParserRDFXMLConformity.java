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

package org.apache.jena.parser;

import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.cimxml.parser.ReaderCIMXML_StAX_SR;
import org.apache.jena.graph.Graph;
import org.apache.jena.irix.SystemIRIx;
import org.apache.jena.mem2.GraphMem2Roaring;
import org.apache.jena.mem2.IndexingStrategy;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.cimxml.parser.system.StreamCIMXMLToDatasetGraph;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sys.JenaSystem;
import org.junit.Test;

import java.io.StringReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestParserRDFXMLConformity {


    /**
     * Test case for the <a href="https://www.w3.org/TR/rdf-syntax-grammar/">W3C RDF Syntax Grammar</a> example 02.
     * <p>
     * Download links:
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example02.rdf">example02.rdf</a>
     */
    @Test
    public void w3cRdfSyntaxGrammarExample02() throws Exception {
        final var rdfxml = """
            <?xml version="1.0"?>
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                     xmlns:dc="http://purl.org/dc/elements/1.1/"
                     xmlns:ex="http://example.org/stuff/1.0/">
            <!-- START -->
            <rdf:Description rdf:about="http://www.w3.org/TR/rdf-syntax-grammar">
              <ex:editor>
                <rdf:Description>
                  <ex:homePage>
                    <rdf:Description rdf:about="http://purl.org/net/dajobe/">
                    </rdf:Description>
                  </ex:homePage>
                </rdf:Description>
              </ex:editor>
            </rdf:Description>
            <!-- END -->
            </rdf:RDF>
            """;
        parseAndCompare(rdfxml);
    }

    /**
     * Test case for the <a href="https://www.w3.org/TR/rdf-syntax-grammar/">W3C RDF Syntax Grammar</a> example 03.
     * <p>
     * Download links:
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example03.rdf">example03.rdf</a>
     */
    @Test
    public void w3cRdfSyntaxGrammarExample03() throws Exception {
        final var rdfxml = """
            <?xml version="1.0"?>
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                     xmlns:dc="http://purl.org/dc/elements/1.1/"
                     xmlns:ex="http://example.org/stuff/1.0/">
            <!-- START -->
            <rdf:Description rdf:about="http://www.w3.org/TR/rdf-syntax-grammar">
              <ex:editor>
                <rdf:Description>
                  <ex:homePage>
                    <rdf:Description rdf:about="http://purl.org/net/dajobe/">
                    </rdf:Description>
                  </ex:homePage>
                </rdf:Description>
              </ex:editor>
            </rdf:Description>
    
            <rdf:Description rdf:about="http://www.w3.org/TR/rdf-syntax-grammar">
              <ex:editor>
                <rdf:Description>
                  <ex:fullName>Dave Beckett</ex:fullName>
                </rdf:Description>
              </ex:editor>
            </rdf:Description>
    
            <rdf:Description rdf:about="http://www.w3.org/TR/rdf-syntax-grammar">
              <dc:title>RDF/XML Syntax Specification (Revised)</dc:title>
            </rdf:Description>
            <!-- END -->
            </rdf:RDF>
            """;
        parseAndCompare(rdfxml);
    }

    /**
     * Test case for the <a href="https://www.w3.org/TR/rdf-syntax-grammar/">W3C RDF Syntax Grammar</a> example 04.
     * <p>
     * Download links:
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example04.rdf">example04.rdf</a>
     */
    @Test
    public void w3cRdfSyntaxGrammarExample04() throws Exception {
        final var rdfxml = """
            <?xml version="1.0"?>
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                     xmlns:dc="http://purl.org/dc/elements/1.1/"
                     xmlns:ex="http://example.org/stuff/1.0/">
            <!-- START -->
            <rdf:Description rdf:about="http://www.w3.org/TR/rdf-syntax-grammar">
              <ex:editor>
                <rdf:Description>
                  <ex:homePage>
                    <rdf:Description rdf:about="http://purl.org/net/dajobe/">
                    </rdf:Description>
                  </ex:homePage>
                  <ex:fullName>Dave Beckett</ex:fullName>
                </rdf:Description>
              </ex:editor>
              <dc:title>RDF/XML Syntax Specification (Revised)</dc:title>
            </rdf:Description>
            <!-- END -->
            </rdf:RDF>
            """;
        parseAndCompare(rdfxml);
    }

    /**
     * Test case for the <a href="https://www.w3.org/TR/rdf-syntax-grammar/">W3C RDF Syntax Grammar</a> example 05.
     * <p>
     * Download links:
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example05.rdf">example05.rdf</a>
     */
    @Test
    public void w3cRdfSyntaxGrammarExample05() throws Exception {
        final var rdfxml = """
            <?xml version="1.0"?>
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                     xmlns:dc="http://purl.org/dc/elements/1.1/"
                     xmlns:ex="http://example.org/stuff/1.0/">
            <!-- START -->
            <rdf:Description rdf:about="http://www.w3.org/TR/rdf-syntax-grammar">
              <ex:editor>
                <rdf:Description>
                  <ex:homePage rdf:resource="http://purl.org/net/dajobe/"/>
                  <ex:fullName>Dave Beckett</ex:fullName>
                </rdf:Description>
              </ex:editor>
              <dc:title>RDF/XML Syntax Specification (Revised)</dc:title>
            </rdf:Description>
            <!-- END -->
            </rdf:RDF>
            """;
        parseAndCompare(rdfxml);
    }

    /**
     * Test case for the <a href="https://www.w3.org/TR/rdf-syntax-grammar/">W3C RDF Syntax Grammar</a> example 06.
     * <p>
     * Download links:
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example06.rdf">example06.rdf</a>
     */
    @Test
    public void w3cRdfSyntaxGrammarExample06() throws Exception {
        final var rdfxml = """
            <?xml version="1.0"?>
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                     xmlns:dc="http://purl.org/dc/elements/1.1/"
                     xmlns:ex="http://example.org/stuff/1.0/">
            <!-- START -->
            <rdf:Description rdf:about="http://www.w3.org/TR/rdf-syntax-grammar"
                             dc:title="RDF/XML Syntax Specification (Revised)">
              <ex:editor>
                <rdf:Description ex:fullName="Dave Beckett">
                  <ex:homePage rdf:resource="http://purl.org/net/dajobe/"/>
                </rdf:Description>
              </ex:editor>
            </rdf:Description>
            <!-- END -->
            </rdf:RDF>
            """;
        parseAndCompare(rdfxml);
    }

    /**
     * Test case for the <a href="https://www.w3.org/TR/rdf-syntax-grammar/">W3C RDF Syntax Grammar</a> example 07.
     * <p>
     * Download links:
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example07.rdf">example07.rdf</a>
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example07.nt">example07.nt</a>
     */
    @Test
    public void w3cRdfSyntaxGrammarExample07() throws Exception {
        final var rdfxml = """
            <?xml version="1.0"?>
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                     xmlns:dc="http://purl.org/dc/elements/1.1/"
                     xmlns:ex="http://example.org/stuff/1.0/">
              <rdf:Description rdf:about="http://www.w3.org/TR/rdf-syntax-grammar"
                       dc:title="RDF/XML Syntax Specification (Revised)">
                <ex:editor>
                  <rdf:Description ex:fullName="Dave Beckett">
                <ex:homePage rdf:resource="http://purl.org/net/dajobe/" />
                  </rdf:Description>
                </ex:editor>
              </rdf:Description>
            </rdf:RDF>
            """;
        final var sameAsNTriples = """
            <http://www.w3.org/TR/rdf-syntax-grammar> <http://purl.org/dc/elements/1.1/title> "RDF/XML Syntax Specification (Revised)" .
            _:genid1 <http://example.org/stuff/1.0/fullName> "Dave Beckett" .
            _:genid1 <http://example.org/stuff/1.0/homePage> <http://purl.org/net/dajobe/> .
            <http://www.w3.org/TR/rdf-syntax-grammar> <http://example.org/stuff/1.0/editor> _:genid1 .
            """;
        parseAndCompare(rdfxml, sameAsNTriples);
    }

    /**
     * Test case for the <a href="https://www.w3.org/TR/rdf-syntax-grammar/">W3C RDF Syntax Grammar</a> example 08.
     * <p>
     * Download links:
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example08.rdf">example08.rdf</a>
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example08.nt">example08.nt</a>
     */
    @Test
    public void w3cRdfSyntaxGrammarExample08() throws Exception {
        final var rdfxml = """
            <?xml version="1.0" encoding="utf-8"?>
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                     xmlns:dc="http://purl.org/dc/elements/1.1/">
              <rdf:Description rdf:about="http://www.w3.org/TR/rdf-syntax-grammar">
                <dc:title>RDF/XML Syntax Specification (Revised)</dc:title>
                <dc:title xml:lang="en">RDF/XML Syntax Specification (Revised)</dc:title>
                <dc:title xml:lang="en-US">RDF/XML Syntax Specification (Revised)</dc:title>
              </rdf:Description>
            
              <rdf:Description rdf:about="http://example.org/buecher/baum" xml:lang="de">
                <dc:title>Der Baum</dc:title>
                <dc:description>Das Buch ist außergewöhnlich</dc:description>
                <dc:title xml:lang="en">The Tree</dc:title>
              </rdf:Description>
            </rdf:RDF>
            """;
        final var sameAsNTriples = """
            <http://www.w3.org/TR/rdf-syntax-grammar> <http://purl.org/dc/elements/1.1/title> "RDF/XML Syntax Specification (Revised)" .
            <http://www.w3.org/TR/rdf-syntax-grammar> <http://purl.org/dc/elements/1.1/title> "RDF/XML Syntax Specification (Revised)"@en .
            <http://www.w3.org/TR/rdf-syntax-grammar> <http://purl.org/dc/elements/1.1/title> "RDF/XML Syntax Specification (Revised)"@en-us .
            <http://example.org/buecher/baum> <http://purl.org/dc/elements/1.1/title> "Der Baum"@de .
            <http://example.org/buecher/baum> <http://purl.org/dc/elements/1.1/description> "Das Buch ist au\\u00DFergew\\u00F6hnlich"@de .
            <http://example.org/buecher/baum> <http://purl.org/dc/elements/1.1/title> "The Tree"@en .
            """;
        parseAndCompare(rdfxml, sameAsNTriples);
    }

    /**
     * Test case for the <a href="https://www.w3.org/TR/rdf-syntax-grammar/">W3C RDF Syntax Grammar</a> example 09.
     * <p>
     * Download links:
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example09.rdf">example09.rdf</a>
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example09.nt">example09.nt</a>
     */
    @Test
    public void w3cRdfSyntaxGrammarExample09() throws Exception {
        final var rdfxml = """
            <?xml version="1.0"?>
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                     xmlns:ex="http://example.org/stuff/1.0/">
              <rdf:Description rdf:about="http://example.org/item01">
                <ex:prop rdf:parseType="Literal"
                         xmlns:a="http://example.org/a#"><a:Box required="true">
                     <a:widget size="10" />
                     <a:grommit id="23" /></a:Box>
                </ex:prop>
              </rdf:Description>
            </rdf:RDF>
            """;
        final var sameAsNTriples = """
            <http://example.org/item01> <http://example.org/stuff/1.0/prop> "<a:Box xmlns:a=\\"http://example.org/a#\\" required=\\"true\\">\\n         <a:widget size=\\"10\\"></a:widget>\\n         <a:grommit id=\\"23\\"></a:grommit></a:Box>\\n    "^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral> .
            """;
        parseAndCompare(rdfxml, sameAsNTriples);
    }

    /**
     * Test case for the <a href="https://www.w3.org/TR/rdf-syntax-grammar/">W3C RDF Syntax Grammar</a> example 10.
     * <p>
     * Download links:
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example10.rdf">example10.rdf</a>
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example10.nt">example10.nt</a>
     */
    @Test
    public void w3cRdfSyntaxGrammarExample10() throws Exception {
        final var rdfxml = """
            <?xml version="1.0"?>
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                     xmlns:ex="http://example.org/stuff/1.0/">
              <rdf:Description rdf:about="http://example.org/item01">
                <ex:size rdf:datatype="http://www.w3.org/2001/XMLSchema#int">123</ex:size>
              </rdf:Description>
            </rdf:RDF>
            """;
        final var sameAsNTriples = """
            <http://example.org/item01> <http://example.org/stuff/1.0/size> "123"^^<http://www.w3.org/2001/XMLSchema#int> .
            """;
        parseAndCompare(rdfxml, sameAsNTriples);
    }

    /**
     * Test case for the <a href="https://www.w3.org/TR/rdf-syntax-grammar/">W3C RDF Syntax Grammar</a> example 11.
     * <p>
     * Download links:
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example11.rdf">example11.rdf</a>
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example11.nt">example11.nt</a>
     */
    @Test
    public void w3cRdfSyntaxGrammarExample11() throws Exception {
        final var rdfxml = """
            <?xml version="1.0"?>
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                     xmlns:dc="http://purl.org/dc/elements/1.1/"
                     xmlns:ex="http://example.org/stuff/1.0/">
              <rdf:Description rdf:about="http://www.w3.org/TR/rdf-syntax-grammar"
                       dc:title="RDF/XML Syntax Specification (Revised)">
                <ex:editor rdf:nodeID="abc"/>
              </rdf:Description>
            
              <rdf:Description rdf:nodeID="abc"
                               ex:fullName="Dave Beckett">
                <ex:homePage rdf:resource="http://purl.org/net/dajobe/"/>
              </rdf:Description>
            </rdf:RDF>
            """;
        final var sameAsNTriples = """
            <http://www.w3.org/TR/rdf-syntax-grammar> <http://purl.org/dc/elements/1.1/title> "RDF/XML Syntax Specification (Revised)" .
            <http://www.w3.org/TR/rdf-syntax-grammar> <http://example.org/stuff/1.0/editor> _:abc .
            _:abc <http://example.org/stuff/1.0/fullName> "Dave Beckett" .
            _:abc <http://example.org/stuff/1.0/homePage> <http://purl.org/net/dajobe/> .
            """;
        parseAndCompare(rdfxml, sameAsNTriples);
    }

    /**
     * Test case for the <a href="https://www.w3.org/TR/rdf-syntax-grammar/">W3C RDF Syntax Grammar</a> example 12.
     * <p>
     * Download links:
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example12.rdf">example12.rdf</a>
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example12.nt">example12.nt</a>
     */
    @Test
    public void w3cRdfSyntaxGrammarExample12() throws Exception {
        final var rdfxml = """
            <?xml version="1.0"?>
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                     xmlns:dc="http://purl.org/dc/elements/1.1/"
                     xmlns:ex="http://example.org/stuff/1.0/">
              <rdf:Description rdf:about="http://www.w3.org/TR/rdf-syntax-grammar"
                       dc:title="RDF/XML Syntax Specification (Revised)">
                <ex:editor rdf:parseType="Resource">
                  <ex:fullName>Dave Beckett</ex:fullName>
                  <ex:homePage rdf:resource="http://purl.org/net/dajobe/"/>
                </ex:editor>
              </rdf:Description>
            </rdf:RDF>
            """;
        final var sameAsNTriples = """
            <http://www.w3.org/TR/rdf-syntax-grammar> <http://purl.org/dc/elements/1.1/title> "RDF/XML Syntax Specification (Revised)" .
            _:genid1 <http://example.org/stuff/1.0/fullName> "Dave Beckett" .
            _:genid1 <http://example.org/stuff/1.0/homePage> <http://purl.org/net/dajobe/> .
            <http://www.w3.org/TR/rdf-syntax-grammar> <http://example.org/stuff/1.0/editor> _:genid1 .
            """;
        parseAndCompare(rdfxml, sameAsNTriples);
    }

    /**
     * Test case for the <a href="https://www.w3.org/TR/rdf-syntax-grammar/">W3C RDF Syntax Grammar</a> example 13.
     * <p>
     * Download links:
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example13.rdf">example13.rdf</a>
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example13.nt">example13.nt</a>
     */
    @Test
    public void w3cRdfSyntaxGrammarExample13() throws Exception {
        final var rdfxml = """
            <?xml version="1.0"?>
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                     xmlns:dc="http://purl.org/dc/elements/1.1/"
                     xmlns:ex="http://example.org/stuff/1.0/">
              <rdf:Description rdf:about="http://www.w3.org/TR/rdf-syntax-grammar"
                       dc:title="RDF/XML Syntax Specification (Revised)">
                <ex:editor ex:fullName="Dave Beckett" />
                <!-- Note the ex:homePage property has been ignored for this example -->
              </rdf:Description>
            </rdf:RDF>
            """;
        final var sameAsNTriples = """
            <http://www.w3.org/TR/rdf-syntax-grammar> <http://purl.org/dc/elements/1.1/title> "RDF/XML Syntax Specification (Revised)" .
            _:genid1 <http://example.org/stuff/1.0/fullName> "Dave Beckett" .
            <http://www.w3.org/TR/rdf-syntax-grammar> <http://example.org/stuff/1.0/editor> _:genid1 .
            """;
        parseAndCompare(rdfxml, sameAsNTriples);
    }

    /**
     * Test case for the <a href="https://www.w3.org/TR/rdf-syntax-grammar/">W3C RDF Syntax Grammar</a> example 14.
     * <p>
     * Download links:
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example14.rdf">example14.rdf</a>
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example14.nt">example14.nt</a>
     */
    @Test
    public void w3cRdfSyntaxGrammarExample14() throws Exception {
        final var rdfxml = """
            <?xml version="1.0"?>
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                     xmlns:dc="http://purl.org/dc/elements/1.1/"
                     xmlns:ex="http://example.org/stuff/1.0/">
              <rdf:Description rdf:about="http://example.org/thing">
                <rdf:type rdf:resource="http://example.org/stuff/1.0/Document"/>
                <dc:title>A marvelous thing</dc:title>
              </rdf:Description>
            </rdf:RDF>
            """;
        final var sameAsNTriples = """
            <http://example.org/thing> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/stuff/1.0/Document> .
            <http://example.org/thing> <http://purl.org/dc/elements/1.1/title> "A marvelous thing" .
            """;
        parseAndCompare(rdfxml, sameAsNTriples);
    }

    /**
     * Test case for the <a href="https://www.w3.org/TR/rdf-syntax-grammar/">W3C RDF Syntax Grammar</a> example 15.
     * <p>
     * Download links:
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example15.rdf">example15.rdf</a>
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example15.nt">example15.nt</a>
     */
    @Test
    public void w3cRdfSyntaxGrammarExample15() throws Exception {
        final var rdfxml = """
            <?xml version="1.0"?>
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                     xmlns:dc="http://purl.org/dc/elements/1.1/"
                     xmlns:ex="http://example.org/stuff/1.0/">
              <ex:Document rdf:about="http://example.org/thing">
                <dc:title>A marvelous thing</dc:title>
              </ex:Document>
            </rdf:RDF>
            """;
        final var sameAsNTriples = """
            <http://example.org/thing> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/stuff/1.0/Document> .
            <http://example.org/thing> <http://purl.org/dc/elements/1.1/title> "A marvelous thing" .
            """;
        parseAndCompare(rdfxml, sameAsNTriples);
    }

    /**
     * Test case for the <a href="https://www.w3.org/TR/rdf-syntax-grammar/">W3C RDF Syntax Grammar</a> example 16.
     * <p>
     * Download links:
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example16.rdf">example16.rdf</a>
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example16.nt">example16.nt</a>
     */
    @Test
    public void w3cRdfSyntaxGrammarExample16() throws Exception {
        final var rdfxml = """
            <?xml version="1.0"?>
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                     xmlns:ex="http://example.org/stuff/1.0/"
                     xml:base="http://example.org/here/">
              <rdf:Description rdf:ID="snack">
                <ex:prop rdf:resource="fruit/apple"/>
              </rdf:Description>
            </rdf:RDF>
            """;
        final var sameAsNTriples = """
            <http://example.org/here/#snack> <http://example.org/stuff/1.0/prop> <http://example.org/here/fruit/apple> .
            """;
        parseAndCompare(rdfxml, sameAsNTriples);
    }

    /**
     * Test case for the <a href="https://www.w3.org/TR/rdf-syntax-grammar/">W3C RDF Syntax Grammar</a> example 17.
     * <p>
     * Download links:
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example17.rdf">example17.rdf</a>
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example17.nt">example17.nt</a>
     */
    @Test
    public void w3cRdfSyntaxGrammarExample17() throws Exception {
        final var rdfxml = """
            <?xml version="1.0"?>
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
              <rdf:Seq rdf:about="http://example.org/favourite-fruit">
                <rdf:_1 rdf:resource="http://example.org/banana"/>
                <rdf:_2 rdf:resource="http://example.org/apple"/>
                <rdf:_3 rdf:resource="http://example.org/pear"/>
              </rdf:Seq>
            </rdf:RDF>
            """;
        final var sameAsNTriples = """
            <http://example.org/favourite-fruit> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Seq> .
            <http://example.org/favourite-fruit> <http://www.w3.org/1999/02/22-rdf-syntax-ns#_1> <http://example.org/banana> .
            <http://example.org/favourite-fruit> <http://www.w3.org/1999/02/22-rdf-syntax-ns#_2> <http://example.org/apple> .
            <http://example.org/favourite-fruit> <http://www.w3.org/1999/02/22-rdf-syntax-ns#_3> <http://example.org/pear> .
            """;
        parseAndCompare(rdfxml, sameAsNTriples);
    }

    /**
     * Test case for the <a href="https://www.w3.org/TR/rdf-syntax-grammar/">W3C RDF Syntax Grammar</a> example 18.
     * <p>
     * Download links:
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example18.rdf">example18.rdf</a>
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example18.nt">example18.nt</a>
     */
    @Test
    public void w3cRdfSyntaxGrammarExample18() throws Exception {
        final var rdfxml = """
            <?xml version="1.0"?>
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
              <rdf:Seq rdf:about="http://example.org/favourite-fruit">
                <rdf:li rdf:resource="http://example.org/banana"/>
                <rdf:li rdf:resource="http://example.org/apple"/>
                <rdf:li rdf:resource="http://example.org/pear"/>
              </rdf:Seq>
            </rdf:RDF>
            """;
        final var sameAsNTriples = """
            <http://example.org/favourite-fruit> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Seq> .
            <http://example.org/favourite-fruit> <http://www.w3.org/1999/02/22-rdf-syntax-ns#_1> <http://example.org/banana> .
            <http://example.org/favourite-fruit> <http://www.w3.org/1999/02/22-rdf-syntax-ns#_2> <http://example.org/apple> .
            <http://example.org/favourite-fruit> <http://www.w3.org/1999/02/22-rdf-syntax-ns#_3> <http://example.org/pear> .
            """;
        parseAndCompare(rdfxml, sameAsNTriples);
    }

    /**
     * Test case for the <a href="https://www.w3.org/TR/rdf-syntax-grammar/">W3C RDF Syntax Grammar</a> example 19.
     * <p>
     * Download links:
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example19.rdf">example19.rdf</a>
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example19.nt">example19.nt</a>
     */
    @Test
    public void w3cRdfSyntaxGrammarExample19() throws Exception {
        final var rdfxml = """
            <?xml version="1.0"?>
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                     xmlns:ex="http://example.org/stuff/1.0/">
              <rdf:Description rdf:about="http://example.org/basket">
                <ex:hasFruit rdf:parseType="Collection">
                  <rdf:Description rdf:about="http://example.org/banana"/>
                  <rdf:Description rdf:about="http://example.org/apple"/>
                  <rdf:Description rdf:about="http://example.org/pear"/>
                </ex:hasFruit>
              </rdf:Description>
            </rdf:RDF>
            """;
        final var sameAsNTriples = """
            _:genid1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> <http://example.org/banana> .
            _:genid2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> <http://example.org/apple> .
            _:genid1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:genid2 .
            _:genid3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> <http://example.org/pear> .
            _:genid2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:genid3 .
            _:genid3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .
            <http://example.org/basket> <http://example.org/stuff/1.0/hasFruit> _:genid1 .
            """;
        parseAndCompare(rdfxml, sameAsNTriples);
    }

    /**
     * Test case for the <a href="https://www.w3.org/TR/rdf-syntax-grammar/">W3C RDF Syntax Grammar</a> example 20.
     * <p>
     * Download links:
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example20.rdf">example20.rdf</a>
     * <a href="https://www.w3.org/TR/2004/REC-rdf-syntax-grammar-20040210/example20.nt">example20.nt</a>
     */
    @Test
    public void w3cRdfSyntaxGrammarExample20() throws Exception {
        final var rdfxml = """
            <?xml version="1.0"?>
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                     xmlns:ex="http://example.org/stuff/1.0/"
                     xml:base="http://example.org/triples/">
              <rdf:Description rdf:about="http://example.org/">
                <ex:prop rdf:ID="triple1">blah</ex:prop>
              </rdf:Description>
            </rdf:RDF>
            """;
        final var sameAsNTriples = """
            <http://example.org/> <http://example.org/stuff/1.0/prop> "blah" .
            <http://example.org/triples/#triple1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Statement> .
            <http://example.org/triples/#triple1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#subject> <http://example.org/> .
            <http://example.org/triples/#triple1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate> <http://example.org/stuff/1.0/prop> .
            <http://example.org/triples/#triple1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#object> "blah" .
            """;
        parseAndCompare(rdfxml, sameAsNTriples);
    }

    public void assertGraphEquals(Graph expected, Graph actual) {
        // check that all triples in expected graph are in actual graph
        expected.find().forEachRemaining(expectedTriple -> {
            if(!actual.contains(expectedTriple)) {
                if(expectedTriple.getSubject().isBlank() || expectedTriple.getPredicate().isBlank() || expectedTriple.getObject().isBlank()) {
                    return; // Blank nodes are not compared by value, so we skip them
                }
                int i= 0;
            }
            assertTrue("Graphs are not equal: missing triple " + expectedTriple,
                    actual.contains(expectedTriple));
        });

        // check graph sizes
        assertEquals("Graphs are not equal: different sizes.",
                expected.size(), actual.size());

        assertTrue(expected.isIsomorphicWith(actual)); // isIsomorphicWith also checks blank nodes
    }

    public void assertPrefixMappingEquals(PrefixMapping expected, PrefixMap actual) {

        // check namespace mappings size
        assertEquals("PrefixMappings are not equal: different number of namespaces.",
                expected.numPrefixes(), actual.size());

        // check that all namespaces in expected graph are in actual graph
        expected.getNsPrefixMap().forEach((prefix, uri) -> {
            assertTrue("PrefixMappings are not equal: missing namespace " + prefix + " -> " + uri,
                    actual.containsPrefix(prefix));
            assertEquals("PrefixMappings are not equal: different URI for namespace " + prefix,
                    uri, actual.get(prefix));
        });
    }

    public void parseAndCompare(String rdfxml) throws Exception {
        parseAndCompare(rdfxml, null);
    }

    public void parseAndCompare(String rdfxml, String nTriples) throws Exception {
        Lib.setenv(SystemIRIx.sysPropertyProvider, "IRI3986");
        JenaSystem.init();
        SystemIRIx.reset();
        final var expectedGraph = new GraphMem2Roaring(IndexingStrategy.LAZY);
        final var parser = new ReaderCIMXML_StAX_SR();
        final var streamRDF = new StreamCIMXMLToDatasetGraph();

        parser.read(new StringReader(rdfxml), null, streamRDF);
        var graph = streamRDF.getCIMDatasetGraph().getDefaultGraph();

        RDFParser.create()
                .source(new StringReader(rdfxml))
                .lang(org.apache.jena.riot.Lang.RDFXML)
                .checking(false)
                .parse(expectedGraph);

        assertGraphEquals(expectedGraph, graph);
        assertPrefixMappingEquals(expectedGraph.getPrefixMapping(), streamRDF.getCIMDatasetGraph().prefixes());

        if(nTriples != null) {
            final var nTriplesGraph = new GraphMem2Roaring(IndexingStrategy.LAZY);
            RDFParser.create()
                    .source(new StringReader(nTriples))
                    .lang(org.apache.jena.riot.Lang.NTRIPLES)
                    .checking(false)
                    .parse(nTriplesGraph);

            assertGraphEquals(expectedGraph, nTriplesGraph);
        }
    }
}

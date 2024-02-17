/**
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

package org.apache.jena.riot.thrift3;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.impl.Util;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.riot.thrift3.wire.RDF_BNode;
import org.apache.jena.riot.thrift3.wire.RDF_IRI;
import org.apache.jena.riot.thrift3.wire.RDF_Literal;
import org.apache.jena.riot.thrift3.wire.RDF_Term;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TestThrift3Term {
    static PrefixMap prefixMap = PrefixMapFactory.create() ;
    static {
        prefixMap.add("rdf",    RDF.getURI()) ;
        prefixMap.add("xsd",    XSD.getURI()) ;
        prefixMap.add("rdfs",   RDFS.getURI()) ;
        prefixMap.add("ex",     "http://example.org/") ;
        prefixMap.add("",       "http://example/") ;
        prefixMap.add("ns",     "http://namespace/ns#") ;
    }

    // Terms
    @Test public void term_uri_01() {
        testTerm("<http://hostname/>", new StringDictionaryWriter(), new ArrayList<>()) ;
    }

    @Test public void term_uri_02()  {
        var readerDict = new ArrayList<String>();
        RDF_Term rt = testTerm("<http://example/>", new StringDictionaryWriter(), readerDict) ;
        assertTrue(rt.isSetPrefixName()) ;
        assertEquals("", readerDict.get(rt.getPrefixName().prefix)) ;
        assertEquals("", readerDict.get(rt.getPrefixName().localName)) ;
    }

    @Test public void term_uri_03()  {
        var readerDict = new ArrayList<String>();
        RDF_Term rt = testTerm("<http://namespace/ns#foobar>", new StringDictionaryWriter(), readerDict) ;
        assertTrue(rt.isSetPrefixName()) ;
        assertEquals("ns", readerDict.get(rt.getPrefixName().prefix)) ;
        assertEquals("foobar", readerDict.get(rt.getPrefixName().localName)) ;
    }

    @Test public void term_uri_04()  {
        var readerDict = new ArrayList<String>();
        RDF_Term rt = testTerm("rdf:type", new StringDictionaryWriter(), readerDict) ;
        assertTrue(rt.isSetPrefixName()) ;
        assertEquals("rdf", readerDict.get(rt.getPrefixName().prefix)) ;
        assertEquals("type", readerDict.get(rt.getPrefixName().localName)) ;
    }

    @Test public void term_literal_01() {
        var readerDict = new ArrayList<String>();
        RDF_Term rt = testTerm("'foo'", new StringDictionaryWriter(), readerDict) ;
        assertFalse(rt.getLiteral().isSetDatatype()) ;
        assertFalse(rt.getLiteral().isSetDtPrefix()) ;
        assertFalse(rt.getLiteral().isSetLangtag()) ;
    }

    @Test public void term_literal_02() {
        var readerDict = new ArrayList<String>();
        RDF_Term rt = testTerm("'foo'@en", new StringDictionaryWriter(), readerDict) ;
        assertFalse(rt.getLiteral().isSetDatatype()) ;
        assertFalse(rt.getLiteral().isSetDtPrefix()) ;
        assertTrue(rt.getLiteral().isSetLangtag()) ;
    }

    @Test public void term_literal_03() {
        var readerDict = new ArrayList<String>();
        RDF_Term rt = testTerm("123", new StringDictionaryWriter(), readerDict) ;
        assertFalse(rt.getLiteral().isSetDatatype()) ;
        assertTrue(rt.getLiteral().isSetDtPrefix()) ;
        assertEquals("xsd", readerDict.get(rt.getLiteral().getDtPrefix().getPrefix())) ;
        assertEquals("integer", readerDict.get(rt.getLiteral().getDtPrefix().getLocalName())) ;
    }

    @Test public void term_literal_04() {
        var readerDict = new ArrayList<String>();
        RDF_Term rt = testTerm("'foo'^^<http://dataype/>", new StringDictionaryWriter(), readerDict) ;
        assertTrue(rt.getLiteral().isSetDatatype()) ;
        assertFalse(rt.getLiteral().isSetDtPrefix()) ;
        assertEquals("http://dataype/", readerDict.get(rt.getLiteral().getDatatype())) ;
    }

    @Test public void term_literal_05() {
        var readerDict = new ArrayList<String>();
        RDF_Term rt = testTerm("'foo'^^<http://example/>", new StringDictionaryWriter(), readerDict) ;
        assertFalse(rt.getLiteral().isSetDatatype()) ;
        assertTrue(rt.getLiteral().isSetDtPrefix()) ;
        assertEquals("", readerDict.get(rt.getLiteral().getDtPrefix().getPrefix())) ;
        assertEquals("", readerDict.get(rt.getLiteral().getDtPrefix().getLocalName())) ;
    }

    @Test public void term_var_01() {
        testTerm("?var", new StringDictionaryWriter(), new ArrayList<>()) ;
    }

    @Test public void term_bnode_01() {
        var readerDict = new ArrayList<String>();
        Node n = SSE.parseNode("_:blanknode") ;
        RDF_Term rt = testTerm(n, new StringDictionaryWriter(), readerDict) ;
        assertEquals(n.getBlankNodeLabel(), readerDict.get(rt.getBnode().getLabel())) ;
    }

    @Test public void term_bnode_02() {
        var readerDict = new ArrayList<String>();
        String label = "abcdefghijklmn" ;
        Node n = NodeFactory.createBlankNode(label) ;
        RDF_Term rt = testTerm(n, new StringDictionaryWriter(), readerDict) ;
        assertTrue(rt.isSetBnode()) ;
        assertEquals(label, readerDict.get(rt.getBnode().getLabel())) ;
    }

    @Test public void term_any_1() {
        RDF_Term rt = testTerm(Node.ANY, new StringDictionaryWriter(), new ArrayList<>());
        assertTrue(rt.isSetAny()) ;
    }

    private RDF_Term testTerm(String str, StringDictionaryWriter writerDict, List<String> readerDict) {
        RDF_Term rt = testTerm(SSE.parseNode(str), prefixMap, writerDict, readerDict) ;
        return rt ;
    }

    private RDF_Term testTerm(Node node, StringDictionaryWriter writerDict, List<String> readerDict) {
        return testTerm(node, null, writerDict, readerDict) ;
    }

    private RDF_Term testTerm(Node node, PrefixMap pmap, StringDictionaryWriter writerDict, List<String> readerDict) {
        RDF_Term rt = Thrift3Convert.convert(node, pmap, writerDict) ;
        if(writerDict.hasStringsToFlush()) {
            readerDict.addAll(writerDict.flush());
        }
        assertTrue(rt.isSet()) ;

        if ( node == null) {
            assertTrue(rt.isSetUndefined());
        } else if ( node.isURI() ) {
            assertTrue(rt.isSetIri() || rt.isSetPrefixName() ) ;
            if ( rt.isSetIri() ) {
                RDF_IRI iri = rt.getIri() ;
                assertEquals(node.getURI(), readerDict.get(iri.getIri())) ;
            }
            if ( rt.isSetPrefixName() ) {
                assertTrue(rt.getPrefixName().isSetPrefix()) ;
                assertTrue(rt.getPrefixName().isSetLocalName()) ;
            }
        } else if ( node.isLiteral() ) {
            assertTrue(rt.isSetLiteral()) ;
            RDF_Literal lit = rt.getLiteral() ;
            assertTrue(lit.isSetLex()) ;
            assertEquals(node.getLiteralLexicalForm(), lit.getLex()) ;

            // RDF 1.1
            if ( Util.isSimpleString(node) ) {
                assertFalse(lit.isSetDatatype()) ;
                assertFalse(lit.isSetDtPrefix()) ;
                assertFalse(lit.isSetLangtag()) ;
            } else if ( Util.isLangString(node) ) {
                assertFalse(lit.isSetDatatype()) ;
                assertFalse(lit.isSetDtPrefix()) ;
                assertTrue(lit.isSetLangtag()) ;
            }
            else {
                // Regular typed literal.
                assertTrue(lit.isSetDatatype() || lit.isSetDtPrefix()) ;
                assertFalse(lit.isSetLangtag()) ;
            }
        } else if ( node.isBlank() ) {
            assertTrue(rt.isSetBnode()) ;
            RDF_BNode bnode = rt.getBnode() ;
            assertEquals(node.getBlankNodeLabel(), readerDict.get(bnode.getLabel())) ;
        } else if ( node.isVariable() ) {
            assertTrue(rt.isSetVariable()) ;
            assertEquals(node.getName(), readerDict.get(rt.getVariable().getName())) ;
        } else if ( Node.ANY.equals(node) ) {
            assertTrue(rt.isSetAny()) ;
        } else
            fail("Unknown node type") ;

        // And reverse
        Node n2 = Thrift3Convert.convert(rt,pmap, readerDict) ;
        assertEquals(node, n2) ;
        return rt ;
    }

    @Test public void rdfterm_01() {
            RDF_Term rt = T3RDF.tANY ;
            Node n = Thrift3Convert.convert(rt, new ArrayList<>()) ;
            assertEquals(Node.ANY, n) ;
       }

    @Test public void rdfterm_02() {
        RDF_Term rt = T3RDF.tUNDEF ;
        Node n = Thrift3Convert.convert(rt, new ArrayList<>()) ;
        assertNull(n) ;
    }

    @Test public void round_trip_01() {
        testTerm(null, null, new StringDictionaryWriter(), new ArrayList<>());
    }

    @Test public void round_trip_02() {
        testTerm(Node.ANY, null, new StringDictionaryWriter(), new ArrayList<>());
    }

    @Test public void round_trip_03() {
        testTerm(NodeFactory.createVariable("x"), null,
                new StringDictionaryWriter(), new ArrayList<>());
    }

    // Round trip node->bytes->node.
    @Test public void round_trip_bytes_01() {
        testTermBytes(NodeFactory.createURI("http://example/"));
    }

    @Test public void round_trip_bytes_02() {
        testTermBytes(NodeFactory.createLiteralString("value"));
    }

    @Test public void round_trip_bytes_03() {
        testTermBytes(NodeFactory.createBlankNode("0123456"));
    }

    private void testTermBytes(Node node) {
        final var writerDict = new StringDictionaryWriter();
        final var readerDict = new ArrayList<String>();
        RDF_Term rt = Thrift3Convert.convert(node, writerDict);
        if(writerDict.hasStringsToFlush()) {
            readerDict.addAll(writerDict.flush());
        }
        byte[] b = Thrift3Convert.termToBytes(rt);
        RDF_Term rt2 = Thrift3Convert.termFromBytes(b);
        Node node2 = Thrift3Convert.convert(rt2, readerDict);
        assertEquals(node, node2);
    }

}


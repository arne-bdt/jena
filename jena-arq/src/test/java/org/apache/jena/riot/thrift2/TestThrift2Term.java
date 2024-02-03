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

package org.apache.jena.riot.thrift2;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.impl.Util;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.riot.thrift2.wire.RDF_BNode;
import org.apache.jena.riot.thrift2.wire.RDF_IRI;
import org.apache.jena.riot.thrift2.wire.RDF_Literal;
import org.apache.jena.riot.thrift2.wire.RDF_Term;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestThrift2Term {
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
        testTerm("<http://hostname/>") ;
    }

    @Test public void term_uri_02()  {
        RDF_Term rt = testTerm("<http://example/>") ;
        assertTrue(rt.isSetPrefixName()) ;
        assertEquals(rt.getPrefixName().prefix, "") ;
        assertEquals(rt.getPrefixName().localName,  "") ;
    }

    @Test public void term_uri_03()  {
        RDF_Term rt = testTerm("<http://namespace/ns#foobar>") ;
        assertTrue(rt.isSetPrefixName()) ;
        assertEquals(rt.getPrefixName().prefix, "ns") ;
        assertEquals(rt.getPrefixName().localName,  "foobar") ;
    }

    @Test public void term_uri_04()  {
        RDF_Term rt = testTerm("rdf:type") ;
        assertTrue(rt.isSetPrefixName()) ;
        assertEquals(rt.getPrefixName().prefix, "rdf") ;
        assertEquals(rt.getPrefixName().localName,  "type") ;
    }

    @Test public void term_literal_01() {
        RDF_Term rt = testTerm("'foo'") ;
        assertFalse(rt.getLiteral().isSetDatatype()) ;
        assertFalse(rt.getLiteral().isSetDtPrefix()) ;
        assertFalse(rt.getLiteral().isSetLangtag()) ;
    }

    @Test public void term_literal_02() {
        RDF_Term rt = testTerm("'foo'@en") ;
        assertFalse(rt.getLiteral().isSetDatatype()) ;
        assertFalse(rt.getLiteral().isSetDtPrefix()) ;
        assertTrue(rt.getLiteral().isSetLangtag()) ;
    }

    @Test public void term_literal_03() {
        RDF_Term rt = testTerm("123") ;
        assertFalse(rt.getLiteral().isSetDatatype()) ;
        assertTrue(rt.getLiteral().isSetDtPrefix()) ;
        assertEquals(rt.getLiteral().getDtPrefix().getPrefix(), "xsd") ;
        assertEquals(rt.getLiteral().getDtPrefix().getLocalName(), "integer") ;
    }

    @Test public void term_literal_04() {
        RDF_Term rt = testTerm("'foo'^^<http://dataype/>") ;
        assertTrue(rt.getLiteral().isSetDatatype()) ;
        assertFalse(rt.getLiteral().isSetDtPrefix()) ;
        assertEquals(rt.getLiteral().getDatatype(), "http://dataype/") ;
    }

    @Test public void term_literal_05() {
        RDF_Term rt = testTerm("'foo'^^<http://example/>") ;
        assertFalse(rt.getLiteral().isSetDatatype()) ;
        assertTrue(rt.getLiteral().isSetDtPrefix()) ;
        assertEquals(rt.getLiteral().getDtPrefix().getPrefix(), "") ;
        assertEquals(rt.getLiteral().getDtPrefix().getLocalName(), "") ;
    }

    @Test public void term_var_01() {
        testTerm("?var") ;
    }

    @Test public void term_bnode_01() {
        Node n = SSE.parseNode("_:blanknode") ;
        RDF_Term rt = testTerm(n) ;
        assertEquals(rt.getBnode().getLabel(), n.getBlankNodeLabel()) ;
    }

    @Test public void term_bnode_02() {
        String label = "abcdefghijklmn" ;
        Node n = NodeFactory.createBlankNode("abcdefghijklmn") ;
        RDF_Term rt = testTerm(n) ;
        assertTrue(rt.isSetBnode()) ;
        assertEquals("abcdefghijklmn", rt.getBnode().getLabel()) ;
    }

    @Test public void term_any_1() {
        RDF_Term rt = testTerm(Node.ANY) ;
        assertTrue(rt.isSetAny()) ;
    }

    private RDF_Term testTerm(String str) {
        RDF_Term rt = testTerm(SSE.parseNode(str), prefixMap) ;
        return rt ;
    }

    private RDF_Term testTerm(Node node) {
        return testTerm(node, null) ;
    }

    private RDF_Term testTerm(Node node, PrefixMap pmap) {
        RDF_Term rt = Thrift2Convert.convert(node, pmap) ;
        assertTrue(rt.isSet()) ;

        if ( node == null) {
            assertTrue(rt.isSetUndefined());
        } else if ( node.isURI() ) {
            assertTrue(rt.isSetIri() || rt.isSetPrefixName() ) ;
            if ( rt.isSetIri() ) {
                RDF_IRI iri = rt.getIri() ;
                assertEquals(node.getURI(), iri.getIri()) ;
            }
            if ( rt.isSetPrefixName() ) {
                assertTrue(rt.getPrefixName().isSetPrefix()) ;
                assertTrue(rt.getPrefixName().isSetLocalName()) ;
            }
        } else if ( rt.isSetValDecimal() ||
                    rt.isSetValDouble() ||
                    rt.isSetValInteger() )
        {
            // Nothing specific to check.
            // And not reversible.
            return rt ;
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
            assertEquals(node.getBlankNodeLabel(), bnode.getLabel()) ;
        } else if ( node.isVariable() ) {
            assertTrue(rt.isSetVariable()) ;
            assertEquals(node.getName(), rt.getVariable().getName()) ;
        } else if ( Node.ANY.equals(node) ) {
            assertTrue(rt.isSetAny()) ;
        } else
            fail("Unknown node type") ;

        // And reverse
        Node n2 = Thrift2Convert.convert(rt,pmap) ;
        assertEquals(node, n2) ;
        return rt ;
    }

    @Test public void rdfterm_01() {
            RDF_Term rt = T2RDF.tANY ;
            Node n = Thrift2Convert.convert(rt) ;
            assertEquals(Node.ANY, n) ;
       }

    @Test public void rdfterm_02() {
        RDF_Term rt = T2RDF.tUNDEF ;
        Node n = Thrift2Convert.convert(rt) ;
        assertNull(n) ;
    }

    @Test public void round_trip_01() {
        testTerm(null, null);
    }

    @Test public void round_trip_02() {
        testTerm(Node.ANY, null);
    }

    @Test public void round_trip_03() {
        testTerm(NodeFactory.createVariable("x"), null);
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
        RDF_Term rt = Thrift2Convert.convert(node);
        byte[] b = Thrift2Convert.termToBytes(rt);
        RDF_Term rt2 = Thrift2Convert.termFromBytes(b);
        Node node2 = Thrift2Convert.convert(rt2);
        assertEquals(node, node2);
    }

}


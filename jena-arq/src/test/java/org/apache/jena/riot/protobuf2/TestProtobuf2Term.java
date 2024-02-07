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

package org.apache.jena.riot.protobuf2;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.impl.Util;
import org.apache.jena.riot.protobuf2.wire.PB2_RDF.*;
import org.apache.jena.riot.protobuf2.wire.PB2_RDF.RDF_Literal.LiteralKindCase;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class TestProtobuf2Term {
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
        assertTrue(rt.hasPrefixName()) ;
        assertEquals(rt.getPrefixName().getPrefix(), "") ;
        assertEquals(rt.getPrefixName().getLocalName(),  "") ;
    }

    @Test public void term_uri_03()  {
        RDF_Term rt = testTerm("<http://namespace/ns#foobar>") ;
        assertTrue(rt.hasPrefixName()) ;
        assertEquals(rt.getPrefixName().getPrefix(), "ns") ;
        assertEquals(rt.getPrefixName().getLocalName(),  "foobar") ;
    }

    @Test public void term_uri_04()  {
        RDF_Term rt = testTerm("rdf:type") ;
        assertTrue(rt.hasPrefixName()) ;
        assertEquals(rt.getPrefixName().getPrefix(), "rdf") ;
        assertEquals(rt.getPrefixName().getLocalName(),  "type") ;
    }

    @Test public void term_literal_01() {
        RDF_Term rt = testTerm("'foo'") ;
        assertTrue(rt.hasLiteral());
        RDF_Literal lit = rt.getLiteral();
        assertTrue(lit.getSimple());
    }

    @Test public void term_literal_02() {
        RDF_Term rt = testTerm("'foo'@en") ;
        assertFalse(rt.getLiteral().getSimple());

        assertTrue(rt.hasLiteral());
        RDF_Literal lit = rt.getLiteral();
        assertTrue(lit.getLiteralKindCase()==LiteralKindCase.LANGTAG);
        assertEquals("en", lit.getLangtag());
    }

    @Test public void term_literal_03() {
        RDF_Term rt = testTerm("123") ;
        assertTrue(rt.getLiteral().hasDtPrefix());
        assertTrue(rt.getLiteral().getLiteralKindCase()==LiteralKindCase.DTPREFIX);
        assertEquals(rt.getLiteral().getDtPrefix().getPrefix(), "xsd") ;
        assertEquals(rt.getLiteral().getDtPrefix().getLocalName(), "integer") ;
    }

    @Test public void term_literal_04() {
        RDF_Term rt = testTerm("'foo'^^<http://dataype/>") ;
        assertFalse(rt.getLiteral().getSimple());
        RDF_Literal lit = rt.getLiteral();
        assertEquals("http://dataype/", lit.getDatatype());
    }

    @Test public void term_literal_05() {
        RDF_Term rt = testTerm("'foo'^^<http://example/>") ;
        assertFalse(rt.getLiteral().getSimple());
        RDF_Literal lit = rt.getLiteral();
        assertEquals("", lit.getDtPrefix().getPrefix());
        assertEquals("", lit.getDtPrefix().getLocalName());
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
        assertTrue(rt.hasBnode()) ;
        assertEquals("abcdefghijklmn", rt.getBnode().getLabel()) ;
    }

    @Test public void term_any_1() {
        RDF_Term rt = testTerm(Node.ANY) ;
        assertTrue(rt.hasAny()) ;
    }

    private RDF_Term testTerm(String str) {
        RDF_Term rt = testTerm(SSE.parseNode(str), prefixMap) ;
        return rt ;
    }

    private RDF_Term testTerm(Node node) {
        return testTerm(node, null) ;
    }

    private static void assertNullPB(String obj) {
        assertEquals("", obj);
    }

    private static Set<XSDDatatype> integerSubTypes = new HashSet<>() ;
    static {
//        floatingpointSubTypes.add(XSDDatatype.XSDfloat) ;
//        floatingpointSubTypes.add(XSDDatatype.XSDdouble) ;
//      decimalSubTypes.add(XSDDatatype.XSDdecimal) ;
        integerSubTypes.add(XSDDatatype.XSDint) ;
        integerSubTypes.add(XSDDatatype.XSDlong) ;
        integerSubTypes.add(XSDDatatype.XSDshort) ;
        integerSubTypes.add(XSDDatatype.XSDbyte) ;
        integerSubTypes.add(XSDDatatype.XSDunsignedByte) ;
        integerSubTypes.add(XSDDatatype.XSDunsignedShort) ;
        integerSubTypes.add(XSDDatatype.XSDunsignedInt) ;
        integerSubTypes.add(XSDDatatype.XSDunsignedLong) ;
        integerSubTypes.add(XSDDatatype.XSDinteger) ;
        integerSubTypes.add(XSDDatatype.XSDnonPositiveInteger) ;
        integerSubTypes.add(XSDDatatype.XSDnonNegativeInteger) ;
        integerSubTypes.add(XSDDatatype.XSDpositiveInteger) ;
        integerSubTypes.add(XSDDatatype.XSDnegativeInteger) ;
    }

    // Encode a node, see if the RDF_Term is correct.
    private RDF_Term testTerm(Node node, PrefixMap pmap) {
        RDF_Term rt = Protobuf2Convert.convert(node, pmap) ;

        if ( node == null) {
            assertTrue(rt.hasUndefined());
            return rt;
        }

        switch (rt.getTermCase()) {
//            message RDF_Term {
//                oneof term {
//                  RDF_IRI        iri        = 1 ;
//                  RDF_BNode      bnode      = 2 ;
//                  RDF_Literal    literal    = 3 ;
//                  RDF_PrefixName prefixName = 4 ;
//                  RDF_VAR        variable   = 5 ;
//                  RDF_Triple     tripleTerm = 6 ;
//                  RDF_ANY        any        = 7 ;
//                  RDF_UNDEF      undefined  = 8 ;
//                  RDF_REPEAT     repeat     = 9 ;
//
//                  // Value forms of literals.
//                  int64          valInteger = 20 ;
//                  double         valDouble  = 21 ;
//                  RDF_Decimal    valDecimal = 22 ;
//                }
//              }
            case IRI : {
                RDF_IRI iri = rt.getIri() ;
                assertEquals(node.getURI(), iri.getIri()) ;
                break;
            }
            case BNODE : {
                RDF_BNode bnode = rt.getBnode() ;
                assertEquals(node.getBlankNodeLabel(), bnode.getLabel()) ;
                break;
            }
            case LITERAL : {
                RDF_Literal lit = rt.getLiteral() ;
                assertEquals(node.getLiteralLexicalForm(), lit.getLex()) ;

                // RDF 1.1
                if ( Util.isSimpleString(node) ) {
                    assertTrue(lit.getSimple());
                    // Protobuf default is ""
                    assertNullPB(lit.getDatatype()) ;
                    assertEquals(RDF_PrefixName.getDefaultInstance(), lit.getDtPrefix());
                    assertNullPB(lit.getLangtag()) ;
                } else if ( Util.isLangString(node) ) {
                    assertFalse(lit.getSimple());
                    assertNullPB(lit.getDatatype()) ;
                    assertEquals(RDF_PrefixName.getDefaultInstance(), lit.getDtPrefix());
                    assertNotSame("", lit.getLangtag()) ;
                }
                else {
                    assertFalse(lit.getSimple());
                    // Regular typed literal.
                    assertTrue(lit.getDatatype() != null || lit.getDtPrefix() != null );
                    assertNullPB(lit.getLangtag()) ;
                }
                break;
            }
            case PREFIXNAME : {
                assertNotNull(rt.getPrefixName().getPrefix()) ;
                assertNotNull(rt.getPrefixName().getLocalName()) ;
                String x = pmap.expand(rt.getPrefixName().getPrefix(), rt.getPrefixName().getLocalName());
                assertEquals(node.getURI(),x);
                break;
            }
            case VARIABLE :
                assertEquals(node.getName(), rt.getVariable().getName());
                break;
            case TRIPLETERM : {
                RDF_Triple encTriple = rt.getTripleTerm();
                Triple t = node.getTriple();
                RDF_Term rt_s = testTerm(t.getSubject(), pmap);
                RDF_Term rt_p = testTerm(t.getPredicate(), pmap);
                RDF_Term rt_o = testTerm(t.getObject(), pmap);
                assertEquals(encTriple.getS(), rt_s);
                assertEquals(encTriple.getP(), rt_p);
                assertEquals(encTriple.getO(), rt_o);
                break;
            }
            case ANY :
                assertEquals(Node.ANY, node);
            case REPEAT :
                break;
            case UNDEFINED :
                assertNull(node);
                return rt;
            case TERM_NOT_SET :
                break;
        }

        // And reverse
        Node n2 = Protobuf2Convert.convert(rt, pmap);
        assertEquals(node, n2) ;

        return rt;
    }
}


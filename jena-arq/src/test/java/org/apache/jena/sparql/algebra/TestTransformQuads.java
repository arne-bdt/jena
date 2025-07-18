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

package org.apache.jena.sparql.algebra;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.sse.SSE;

//Tests for conversion of algebra forms to quad form.
public class TestTransformQuads
{
    // Simple
    @Test public void quads01() { test ("{ GRAPH ?g { ?s ?p ?o } }",
                                        "(quadpattern (quad ?g ?s ?p ?o))"
                                        ); }
    // Not nested
    @Test public void quads02() { test ("{ GRAPH ?g { ?s ?p ?o } GRAPH ?g1 { ?s1 ?p1 ?o1 }  }",
                                        "(sequence" +
                                        "    (quadpattern (quad ?g ?s ?p ?o))",
                                        "    (quadpattern (quad ?g1 ?s1 ?p1 ?o1)))"
                                       ); }

    // Same ?g
    @Test public void quads03() { test ("{ GRAPH ?g { ?s ?p ?o } GRAPH ?g { ?s1 ?p1 ?o1 }  }",
                                        "(sequence" +
                                        "   (quadpattern (quad ?g ?s ?p ?o))" +
                                        "   (quadpattern (quad ?g ?s1 ?p1 ?o1)))"
                                        ); }
    // Nested
    @Test public void quads04() { test ("{ GRAPH ?g { ?s ?p ?o GRAPH ?g1 { ?s1 ?p1 ?o1 }  } }",
                                        "(sequence" +
                                        "   (quadpattern (quad ?g ?s ?p ?o))" +
                                        "   (quadpattern (quad ?g1 ?s1 ?p1 ?o1)))"
                                        ); }

    @Test public void quads05() { test ("{ GRAPH ?g { ?s ?p ?o GRAPH ?g { ?s1 ?p1 ?o1 }  } }",
                                        "(assign ((?g ?*g0))" +
                                        "   (sequence" +
                                        "     (quadpattern (quad ?*g0 ?s ?p ?o))" +
                                        "     (quadpattern (quad ?g ?s1 ?p1 ?o1))))"
                                        ); }
    // Filters
    @Test public void quadsFilter1() { test ("{ GRAPH ?g { ?s ?p ?o FILTER (str(?g) = 'graphURI') } }",
                                             "(assign ((?g ?*g0))" +
                                             "   (filter (= (str ?g) 'graphURI')" +
                                             "     (quadpattern (quad ?*g0 ?s ?p ?o))))"
                                        ); }

    @Test public void quadsFilter2() { test ("{ GRAPH ?g { ?s ?p ?o } FILTER (str(?g) = 'graphURI') }",
                                             "(filter (= (str ?g) 'graphURI')" +
                                             "   (quadpattern (quad ?g ?s ?p ?o)))"
                                        ); }

    // Nested and filter
    // ?g is unbound in the filter.
    @Test public void quadsFilter3() { test ("{ GRAPH ?g { ?s ?p ?o GRAPH ?g1 { ?s1 ?p1 ?o1 FILTER (str(?g) = 'graphURI') } } }",
                                                "(assign ((?g ?*g0))" +
                                                "    (sequence" +
                                                "        (quadpattern (quad ?*g0 ?s ?p ?o))" +
                                                "        (filter (= (str ?g) 'graphURI')" +
                                                "          (quadpattern (quad ?g1 ?s1 ?p1 ?o1)))))"
// If untransformed.
//                                        "   (join" +
//                                        "     (quadpattern (quad ?*g0 ?s ?p ?o))" +
//                                        "     (filter (= (str ?g) 'graphURI')" +
//                                        "       (quadpattern (quad ?g1 ?s1 ?p1 ?o1)))))"
                                        ); }

    @Test public void quadsFilter4() { test ("{ GRAPH ?g { ?s ?p ?o GRAPH ?g1 { ?s1 ?p1 ?o1 FILTER (str(?g1) = 'graphURI') } } }",
                                                "(sequence" +
                                                "   (quadpattern (quad ?g ?s ?p ?o))" +
                                                "   (assign ((?g1 ?*g0))" +
                                                "     (filter (= (str ?g1) 'graphURI')" +
                                                "       (quadpattern (quad ?*g0 ?s1 ?p1 ?o1)))))"
                                       ); }

    // Tricky pattern ... twice.
    @Test public void quadsFilter5() { test ( "{ GRAPH ?g { ?s ?p ?o FILTER (str(?g) = 'graphURI') } " +
                                                 "  GRAPH ?g { ?s ?p ?o FILTER (str(?g) = 'graphURI') } }",
                                                 "(sequence" +
                                                 "   (assign ((?g ?*g0))" +
                                                 "     (filter (= (str ?g) 'graphURI')" +
                                                 "       (quadpattern (quad ?*g0 ?s ?p ?o))))" +
                                                 "   (assign ((?g ?*g1))" +
                                                 "     (filter (= (str ?g) 'graphURI')" +
                                                 "       (quadpattern (quad ?*g1 ?s ?p ?o)))))"
                                         ); }

    // NOT EXISTS
    @Test public void quadsFilterNotExists1() { test ( "{ GRAPH ?g { ?s ?p ?o FILTER NOT EXISTS { GRAPH ?g1 { ?s1 ?p ?o1 } } } }",
                                                       "(filter (notexists",
                                                       "   (quadpattern (quad ?g1 ?s1 ?p ?o1)))",
                                                       "  (quadpattern (quad ?g ?s ?p ?o)))"
                                                         ); }

    // NOT EXISTS
    @Test public void quadsFilterNotExists2() { test ( "{ ?s ?p ?o FILTER NOT EXISTS { GRAPH ?g1 { ?s1 ?p ?o1 } } }",
                                                     "(filter (notexists",
                                                     "   (quadpattern (quad ?g1 ?s1 ?p ?o1)))",
                                                     "  (quadpattern (quad <urn:x-arq:DefaultGraphNode> ?s ?p ?o)))"
                                         ); }

    // NOT EXISTS in left join expression.
    @Test public void quadsFilterNotExists3() { test ( "{ ?s ?p ?o OPTIONAL { FILTER NOT EXISTS { ?x ?y ?z } } }",
                                                     false,
                                                     "(leftjoin",
                                                     "   (quadpattern (quad <urn:x-arq:DefaultGraphNode> ?s ?p ?o))",
                                                     "   (table unit)",
                                                     "   (notexists",
                                                     "     (quadpattern (quad <urn:x-arq:DefaultGraphNode> ?x ?y ?z))))"); }
    // JENA-535
    @Test public void quadsFilterNotExists4() { test ( "{ ?s ?p ?o OPTIONAL { FILTER NOT EXISTS { ?x ?y ?z } } }",
                                                 "(conditional",
                                                 "  (quadpattern (quad <urn:x-arq:DefaultGraphNode> ?s ?p ?o))",
                                                 "  (filter (notexists",
                                                 "             (quadpattern (quad <urn:x-arq:DefaultGraphNode> ?x ?y ?z)))",
                                                 "    (table unit)))"); }

    // NOT EXISTS in left join expression.
    @Test public void quadsFilterNotExists5() { test ( "{ ?s ?p ?o OPTIONAL { FILTER NOT EXISTS { GRAPH ?g { ?x ?y ?z } } } }",
                                                 false,
                                                 "(leftjoin",
                                                 "   (quadpattern (quad <urn:x-arq:DefaultGraphNode> ?s ?p ?o))",
                                                 "   (table unit)",
                                                 "   (notexists",
                                                 "     (quadpattern (?g ?x ?y ?z))))"); }


    @Test public void quadsSubquery1() { test ( "{ GRAPH ?g { { SELECT ?x WHERE { ?x ?p ?g } } } }",
                                                  "(project (?x)",
                                                  "  (quadpattern (quad ?g ?x ?/p ?/g)))"); }



    @Test public void quadsBind1() { test ( "{ GRAPH ?g { ?s ?p ?o . BIND(str(?g) as ?g1) } }",
                                            "(assign ((?g ?*g0))",
                                            "  (extend ((?g1 (str ?g) ))",
                                            "    (quadpattern (quad ?*g0 ?s ?p ?o))))"
                                            );

    }

    // NOT EXISTS in BIND
    @Test public void quadsBind2() { test ( "{ BIND ( true && NOT EXISTS { GRAPH ?g { ?x ?y ?z } } AS ?X ) }",
                                         "(extend ((?X (&& true (notexists",
                                         "                         (quadpattern (quad ?g ?x ?y ?z))))))",
                                         "    (table unit))"); }

    // Don't touch SERVICE
    @Test public void quadsService1() { test ("{ {?s ?p ?o } UNION { SERVICE <http://host/endpoint> { GRAPH ?gr { ?sr ?pr ?or }}} }",
                                            "(union",
                                            "  (quadpattern (quad <urn:x-arq:DefaultGraphNode> ?s ?p ?o))",
                                            "  (service <http://host/endpoint>",
                                            "    (graph ?gr",
                                            "      (bgp (triple ?sr ?pr ?or))))",
                                            ")"); }

    // Don't touch SERVICE
    @Test public void quadsService2() { test ("{ { GRAPH ?g { ?s ?p ?o } } UNION { SERVICE <http://host/endpoint> { GRAPH ?gr { ?sr ?pr ?or }}} }",
                                            "(union",
                                            "  (quadpattern (?g ?s ?p ?o))",
                                            "  (service <http://host/endpoint>",
                                            "    (graph ?gr",
                                            "      (bgp (triple ?sr ?pr ?or))))",
                                            ")"); }

    // Don't touch SERVICE
    @Test public void quads40() { test ("{ GRAPH ?g { SERVICE <http://host/endpoint> { ?s ?p ?o }}}",
                                        "(service <http://host/endpoint> (bgp (triple ?s ?p ?o)))");
                                }

    // Don't touch SERVICE
    @Test public void quads41() { test ("{ GRAPH ?g1 { SERVICE <http://host/endpoint> { ?s ?p ?o } ?s1 ?p1 ?o1 } }",
                                        "(sequence",
                                        "   (service <http://host/endpoint> (bgp (triple ?s ?p ?o)))",
                                        "   (quadpattern (?g1 ?s1 ?p1 ?o1))",
                                        ")");
                                }

    private static void test(String patternString, String... strExpected) {
        test(patternString, true, strExpected);
    }


    private static void test(String patternString, boolean optimize, String... strExpected)
    {
        Query q = QueryFactory.create("SELECT * WHERE "+patternString);
        Op op = Algebra.compile(q);
        if ( optimize )
            op = Algebra.optimize(op);
        op = Algebra.toQuadForm(op);
        Op op2 = SSE.parseOp(StrUtils.strjoinNL(strExpected));
        assertEquals(op2, op);
    }
}


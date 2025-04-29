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

package org.apache.jena.mem2;

import org.apache.jena.graph.GraphMemFactory;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.*;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.jena.update.UpdateAction;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class TestParseXMLLiteral {

    @Test
    public void testParseXMLLiteral() {
        String xml = "<rdf:RDF \n" +
                "    xml:base=\"http://iec.ch/TC57/2014/CIM-schema-cim16#\"\n" +
                "    xmlns:dm=\"http://iec.ch/2002/schema/CIM_difference_model#\" \n" +
                "    xmlns:md=\"http://iec.ch/TC57/61970-552/ModelDescription/1#\"\n" +
                "    xmlns:cim=\"http://iec.ch/TC57/2014/CIM-schema-cim16#\"\n" +
                "    xmlns:meta=\"http://iec.ch/TC57/2014/CIM-schema-cim16#\"\n" +
                "    xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
                "<dm:DifferenceModel rdf:about=\"#_248c809d-1d7b-397c-830f-6928007ae6d9\">                \n" +
                "<md:Model.version>1715589426</md:Model.version>\n" +
                "<md:Model.created>2024-05-13T08:37:06.830Z</md:Model.created>\n" +
                "<md:Model.scenarioTime>2024-05-13T08:37:06.830Z</md:Model.scenarioTime>\n" +
                "<md:Model.profile>http://profile/</md:Model.profile>\n" +
                "<md:Model.modelingAuthoritySet>unknown</md:Model.modelingAuthoritySet>\n" +
                "<meta:Model.modelVersionIri>http://ontology.adms.ru/UIP/md/2021-1</meta:Model.modelVersionIri>\n" +
                "<meta:Model.differenceFrom>2024-04-01T07:55:06.779475Z</meta:Model.differenceFrom>\n" +
                "<meta:Model.differenceTo>2027-10-01T08:37:06.779475Z</meta:Model.differenceTo>\n" +
                "<dm:forwardDifferences rdf:parseType=\"Literal\">\n" +
                "<cim:A rdf:about=\"#_individual-A-1\">\n" +
                "<cim:A-2-B rdf:resource=\"#_individual-B-1\"/>\n" +
                "</cim:A>\n" +
                "<cim:B rdf:about=\"#_individual-B-1\"/>\n" +
                "<cim:D rdf:about=\"#_individual-D-1\"/>\n" +
                "</dm:forwardDifferences>\n" +
                "<dm:reverseDifferences parseType=\"Literal\">\n" +
                "</dm:reverseDifferences>\n" +
                "</dm:DifferenceModel>\n" +
                "</rdf:RDF> ";


        Model res = ModelFactory.createDefaultModel();
        RDFParserBuilder
                .create()
                .fromString(xml)
                .forceLang(Lang.RDFXML)
                .build()
                .parse(res);

        RDFDataMgr.write(System.out, res, RDFFormat.JSONLD11_PRETTY);

        var subjDiffModel = NodeFactory.createURI("http://iec.ch/TC57/2014/CIM-schema-cim16#_248c809d-1d7b-397c-830f-6928007ae6d9");
        var predForwardDifferences = NodeFactory.createURI("http://iec.ch/2002/schema/CIM_difference_model#forwardDifferences");
        var objForwardDifferences = res.getGraph().find(subjDiffModel, predForwardDifferences, Node.ANY).next().getObject();

        var forwardRDF = "<rdf:RDF \n" +
                "    xml:base=\"http://iec.ch/TC57/2014/CIM-schema-cim16#\"\n" +
                "    xmlns:dm=\"http://iec.ch/2002/schema/CIM_difference_model#\" \n" +
                "    xmlns:md=\"http://iec.ch/TC57/61970-552/ModelDescription/1#\"\n" +
                "    xmlns:cim=\"http://iec.ch/TC57/2014/CIM-schema-cim16#\"\n" +
                "    xmlns:meta=\"http://iec.ch/TC57/2014/CIM-schema-cim16#\"\n" +
                "    xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
                objForwardDifferences.getLiteralLexicalForm() +
                "</rdf:RDF>";

        var forwardGraph = GraphFactory.createGraphMem();
        forwardGraph.getPrefixMapping().setNsPrefixes(res.getNsPrefixMap());
        RDFParserBuilder
                .create()
                .fromString(forwardRDF)
                .forceLang(Lang.RDFXML)
                .build()
                .parse(forwardGraph);


        RDFDataMgr.write(System.out, forwardGraph, RDFFormat.JSONLD11_PRETTY);
    }

    /**
     * Test SPARQL Update
     * Reproducing https://www.w3.org/TR/sparql11-update/#deleteInsert example 5:
     * <p>
     * # Graph: http://example/addresses
     * @prefix foaf:  <http://xmlns.com/foaf/0.1/> .
     *
     * <http://example/president25> foaf:givenName "Bill" .
     * <http://example/president25> foaf:familyName "McKinley" .
     * <http://example/president27> foaf:givenName "Bill" .
     * <http://example/president27> foaf:familyName "Taft" .
     * <http://example/president42> foaf:givenName "Bill" .
     * <http://example/president42> foaf:familyName "Clinton" .
     *
     * <p>
     *
     * # Update
     * PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
     *
     * WITH <http://example/addresses>
     * DELETE { ?person foaf:givenName 'Bill' }
     * INSERT { ?person foaf:givenName 'William' }
     * WHERE
     *   { ?person foaf:givenName 'Bill'
     *   }
     */

    @Test
    public void test_SPARQ_Update() {
        var ttl = """
                @prefix foaf:<http://xmlns.com/foaf/0.1/> .
                <http://example/president25> foaf:givenName "Bill" .
                <http://example/president25> foaf:familyName "McKinley" .
                <http://example/president27> foaf:givenName "Bill" .
                <http://example/president27> foaf:familyName "Taft" .
                <http://example/president42> foaf:givenName "Bill" .
                <http://example/president42> foaf:familyName "Clinton" .
                """;
        var g = GraphMemFactory.createGraphMem2();
        RDFParserBuilder
                .create()
                .fromString(ttl)
                .forceLang(Lang.TTL)
                .build()
                .parse(g);

        //print graph
        RDFDataMgr.write(System.out, g, RDFFormat.TURTLE_BLOCKS);

        var update = """
            PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
            
            #WITH <http://example/addresses>
            DELETE { ?person foaf:givenName 'Bill' }
            INSERT { ?person foaf:givenName 'William' }
            WHERE
              { ?person foaf:givenName 'Bill'
              }
          """;

        //var parser = UpdateParser.createParser(Syntax.syntaxSPARQL);
        UpdateRequest request = UpdateFactory.create() ;
        request.add(update);

        var operations = request.getOperations();

        // And perform the operations.
        UpdateAction.execute(request, g) ;

        //print after update
        System.out.println("After update:");
        //print graph
        RDFDataMgr.write(System.out, g, RDFFormat.TURTLE_BLOCKS);
    }
}

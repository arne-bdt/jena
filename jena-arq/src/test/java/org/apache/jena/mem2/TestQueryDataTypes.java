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

import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.DatasetGraphMapLink;
import org.apache.jena.sparql.exec.QueryExecDataset;
import org.apache.jena.sparql.graph.GraphFactory;
import org.junit.Test;

import java.util.stream.Collectors;

public class TestQueryDataTypes {

    @Test
    public void queryDataTypes() {
        var pss = new ParameterizedSparqlString("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX cims: <http://iec.ch/TC57/1999/rdf-schema-extensions-19990926#>\n" +
                "PREFIX uml: <http://iec.ch/TC57/NonStandard/UML#>\n" +
                "\n" +
                "SELECT ?property ?dataType ?primitiveType\n" +
                "WHERE {\n" +
                "\t ?property cims:dataType ?dataType.\n" +
                "\t\t{\n" +
                "\t\t\t?dataType cims:stereotype \"CIMDatatype\".\n" +
                "\t\t\t[] rdfs:domain ?dataType;\n" +
                "\t\t\t   cims:dataType/cims:stereotype \"Primitive\";\n" +
                "\t\t\t   cims:dataType/rdfs:label ?primitiveType\n" +
                "\t\t\t   FILTER (str(?primitiveType) != \"String\")\n" +
                "\t\t}\n" +
                "\t\tUNION\n" +
                "\t\t{\n" +
                "\t\t\t?dataType cims:stereotype \"Primitive\";\n" +
                "\t\t\t\trdfs:label ?primitiveType.\n" +
                "\t\t\t FILTER (str(?primitiveType) != \"String\")\n" +
                "\t\t}\n" +
                "}");
        var query = pss.asQuery();
        query.setBaseURI("urn:uuid");
        var g = GraphFactory.createGraphMem();
        RDFDataMgr.read(g, "./../jena-examples/src/main/resources/data/ENTSOE_CGMES_v2.4.15_04Jul2016_RDFS/EquipmentProfileCoreRDFSAugmented-v2_4_15-4Jul2016.rdf");
        var dataset = new DatasetGraphMapLink(g);
        var rowSet = QueryExecDataset.newBuilder().query(query).dataset(dataset).build().select();
        rowSet.stream().forEach(var ->
                System.out.println(String.format("%128s\t%s", var.get("property").getURI(), var.get("primitiveType").getLiteralLexicalForm())));

    }
}

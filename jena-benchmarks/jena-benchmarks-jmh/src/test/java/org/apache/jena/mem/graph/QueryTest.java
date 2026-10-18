/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *   SPDX-License-Identifier: Apache-2.0
 */

package org.apache.jena.mem.graph;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.resultset.ResultsFormat;
import org.junit.Test;

public class QueryTest {

    @Test
    public void queryTest()
    {
        var ds = DatasetGraphFactory.createGeneral();
        RDFDataMgr.read(ds,"C:/rd/sparql-schulung/CGMES_3_0/Svedala/Svedala_2_teilnezte.jsonld");
        var query = QueryFactory.create("""
                # [endpoint=http://localhost:3030/svedala/sparql]
                
                PREFIX cim:     <http://iec.ch/TC57/CIM100#>
                PREFIX cims:    <http://iec.ch/TC57/1999/rdf-schema-extensions-19990926#>
                PREFIX cimuml:  <http://iec.ch/TC57/NonStandard/UML#>
                PREFIX dcat:    <http://www.w3.org/ns/dcat#>
                PREFIX eu:      <http://iec.ch/TC57/CIM100-European#>
                PREFIX dm:      <http://iec.ch/TC57/61970-552/DifferenceModel/1#>
                PREFIX md:      <http://iec.ch/TC57/61970-552/ModelDescription/1#>
                PREFIX owl:     <http://www.w3.org/2002/07/owl#>
                PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>
                
                SELECT ?conductingEquipment1 ?count
                WHERE {
                    GRAPH <cgmes:schema:EQ> {
                        ?conductingEquipmentType rdfs:subClassOf cim:ConductingEquipment .
                    }
                    GRAPH <cgmes:model:EQ> {
                        ?conductingEquipment1 a ?conductingEquipmentType .
                        FILTER NOT EXISTS {
                            ?conductingEquipment2 a ?conductingEquipmentType .
                            FILTER ( ?conductingEquipmentType != cim:PowerTransformerEnd )
                            FILTER (STR(?conductingEquipment2) < STR(?conductingEquipment1)) .
                            ?conductingEquipment1 (^cim:Terminal.ConductingEquipment/cim:Terminal.ConnectivityNode/^cim:Terminal.ConnectivityNode/cim:Terminal.ConductingEquipment)+ ?conductingEquipment2 .
                        }
                    }
                    #GRAPH <cgmes:model:EQ> {
                    #    LATERAL {
                    #        SELECT (COUNT(?conductingEquipment2) AS ?count) {
                    #            ?conductingEquipment1 (^cim:Terminal.ConductingEquipment/cim:Terminal.ConnectivityNode/^cim:Terminal.ConnectivityNode/cim:Terminal.ConductingEquipment)+ ?conductingEquipment2 .
                    #            FILTER (?conductingEquipment1 != ?conductingEquipment2).
                    #        }
                    #    }
                    #}
                }
                LIMIT 10
                """);
        try(var queryExecution = QueryExecutionFactory.create(query, ds)) {
            // stop time
            var stopWatch = StopWatch.createStarted();
            var resultSet = queryExecution.execSelect();
            ResultSetFormatter.output(resultSet, ResultsFormat.TEXT);
            stopWatch.stop();
            System.out.println("Query execution time: " + stopWatch.getTime() + " ms");

        }
    }
}

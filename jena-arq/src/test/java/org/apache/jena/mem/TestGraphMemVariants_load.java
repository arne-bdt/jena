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

package org.apache.jena.mem;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.graph.Graph;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.function.Supplier;

@Ignore
public class TestGraphMemVariants_load extends TestGraphMemVariantsBase {

    @Test
    public void pizza_owl_rdf() {
        loadGraphsMeasureTimeAndMemory(graphImplementationsToTest, 100,
                "./../jena-examples/src/main/resources/data/pizza.owl.rdf");
    }

    @Test
    public void cheeses_ttl() {
        loadGraphsMeasureTimeAndMemory(graphImplementationsToTest, 50,
                "./../jena-examples/src/main/resources/data/cheeses-0.1.ttl");
    }

    /**
     * Generated large dataset.
     * Tool:
     * http://wbsg.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/spec/BenchmarkRules/index.html#datagenerator
     * Generated with: java -cp lib/* benchmark.generator.Generator -pc 50000 -s ttl -ud
     */
    @Test
    public void BSBM_50000() {
        loadGraphsMeasureTimeAndMemory(graphImplementationsToTest, 1,
                "./../jena-examples/src/main/resources/data/BSBM_50000.ttl.gz");
    }

    @Test
    public void BSBM_2500() {
        loadGraphsMeasureTimeAndMemory(graphImplementationsToTest, 1,
                "./../jena-examples/src/main/resources/data/BSBM_2500.ttl");
    }

    /**
     * Due to copyright, data cannot be added to the repository.
     * Download https://www.entsoe.eu/Documents/CIM_documents/Grid_Model_CIM/ENTSO-E_Conformity_Assessment_Scheme_v3.0.zip
     * from https://www.entsoe.eu/digital/cim/cim-conformity-and-interoperability/     *
     */
    @Test
    public void ENTSO_E_Test_Configurations_v3_0_RealGrid_EQ() {
        loadGraphsMeasureTimeAndMemory(graphImplementationsToTest, 1,
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml");
    }

    /**
     * Due to copyright, data cannot be added to the repository.
     * Download https://www.entsoe.eu/Documents/CIM_documents/Grid_Model_CIM/ENTSO-E_Conformity_Assessment_Scheme_v3.0.zip
     * from https://www.entsoe.eu/digital/cim/cim-conformity-and-interoperability/     *
     */
    @Test
    public void ENTSO_E_Test_Configurations_v3_0_RealGrid_SSH() {
        loadGraphsMeasureTimeAndMemory(graphImplementationsToTest, 1,
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml");
    }

    /**
     * Due to copyright, data cannot be added to the repository.
     * Download https://www.entsoe.eu/Documents/CIM_documents/Grid_Model_CIM/ENTSO-E_Conformity_Assessment_Scheme_v3.0.zip
     * from https://www.entsoe.eu/digital/cim/cim-conformity-and-interoperability/     *
     */
    @Test
    public void ENTSO_E_Test_Configurations_v3_0_RealGrid_SV() {
        loadGraphsMeasureTimeAndMemory(graphImplementationsToTest, 1,
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml");
    }

    /**
     * Due to copyright, data cannot be added to the repository.
     * Download https://www.entsoe.eu/Documents/CIM_documents/Grid_Model_CIM/ENTSO-E_Conformity_Assessment_Scheme_v3.0.zip
     * from https://www.entsoe.eu/digital/cim/cim-conformity-and-interoperability/     *
     */
    @Test
    public void ENTSO_E_Test_Configurations_v3_0_RealGrid_TP() {
        loadGraphsMeasureTimeAndMemory(graphImplementationsToTest, 1,
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml");
    }

    @Test
    public void xxx_CGMES_EQ() {
        loadGraphsMeasureTimeAndMemory(graphImplementationsToTest, 1,
                "C:/temp/res_test/xxx_CGMES_EQ.xml");
    }

    @Test
    public void xxx_CGMES_SSH() {
        loadGraphsMeasureTimeAndMemory(graphImplementationsToTest, 1,
                "C:/temp/res_test/xxx_CGMES_SSH.xml");
    }

    @Test
    public void xxx_CGMES_TP() {
        loadGraphsMeasureTimeAndMemory(graphImplementationsToTest, 1,
                "C:/temp/res_test/xxx_CGMES_TP.xml");
    }

    private void loadGraphsMeasureTimeAndMemory(List<Pair<String, Supplier<Graph>>> graphVariantSuppliersWithNames, int graphMultiplier, String... graphUris) {
        final var triplesPerGraph = loadTriples(graphMultiplier, graphUris);
        for (Pair<String, Supplier<Graph>> graphSuppliersWithName : graphVariantSuppliersWithNames) {
            System.out.println("graph variant: '" + graphSuppliersWithName.getKey() + "'");
            for(int i=0; i<1; i++) {
                var graphs = fillGraphs(graphSuppliersWithName.getValue(), triplesPerGraph);
            }
        }
    }
}

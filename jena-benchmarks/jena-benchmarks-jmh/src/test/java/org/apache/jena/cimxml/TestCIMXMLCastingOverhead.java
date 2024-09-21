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

import org.apache.commons.io.input.BufferedFileChannelInputStream;
import org.apache.jena.cimxml.schema.BaseURI;
import org.apache.jena.cimxml.schema.SchemaRegistry;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.lang.rdfxml.RRX;
import org.apache.jena.sparql.graph.GraphFactory;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.io.IOException;
import java.nio.file.StandardOpenOption;


@State(Scope.Benchmark)
public class TestCIMXMLCastingOverhead {

    final static Node IRI_FOR_PROFILE_EQ = NodeFactory.createURI("cgmes2_4_15:EQ");
    final static Node IRI_FOR_PROFILE_SSH = NodeFactory.createURI("cgmes2_4_15:SSH");
    final static Node IRI_FOR_PROFILE_SV = NodeFactory.createURI("cgmes2_4_15:SV");
    final static Node IRI_FOR_PROFILE_TP = NodeFactory.createURI("cgmes2_4_15:TP");

    final static Node[] ALL_PROFILES = new Node[] { IRI_FOR_PROFILE_EQ, IRI_FOR_PROFILE_SSH, IRI_FOR_PROFILE_SV, IRI_FOR_PROFILE_TP };

    private SchemaRegistry schemaRegistry;

    @Param({
            "C:\\rd\\bewegungsdaten-demo\\shared\\ENTSOE_RDF\\src\\main\\resources\\CGMES\\v2.4.15\\TestConfigurations_packageCASv2.0\\RealGrid\\CGMES_v2.4.15_RealGridTestConfiguration_v2\\CGMES_v2.4.15_RealGridTestConfiguration_EQ_V2.xml",
            "C:\\rd\\bewegungsdaten-demo\\shared\\ENTSOE_RDF\\src\\main\\resources\\CGMES\\v2.4.15\\TestConfigurations_packageCASv2.0\\RealGrid\\CGMES_v2.4.15_RealGridTestConfiguration_v2\\CGMES_v2.4.15_RealGridTestConfiguration_SSH_V2.xml",
            "C:\\rd\\bewegungsdaten-demo\\shared\\ENTSOE_RDF\\src\\main\\resources\\CGMES\\v2.4.15\\TestConfigurations_packageCASv2.0\\RealGrid\\CGMES_v2.4.15_RealGridTestConfiguration_v2\\CGMES_v2.4.15_RealGridTestConfiguration_SV_V2.xml",
            "C:\\rd\\bewegungsdaten-demo\\shared\\ENTSOE_RDF\\src\\main\\resources\\CGMES\\v2.4.15\\TestConfigurations_packageCASv2.0\\RealGrid\\CGMES_v2.4.15_RealGridTestConfiguration_v2\\CGMES_v2.4.15_RealGridTestConfiguration_TP_V2.xml",

    })
    public String param0_GraphUri;

    @Benchmark
    public Graph parseXML() throws Exception {
        final var g = GraphFactory.createGraphMem();
        try(final var is = new BufferedFileChannelInputStream.Builder()
                .setFile(param0_GraphUri)
                .setOpenOptions(StandardOpenOption.READ)
                .setBufferSize(64*4096)
                .get()) {
            RDFParser.source(is)
                    .base(BaseURI.DEFAULT_BASE_URI)  // base URI for the model and thus for al mRID's in the model
                    .forceLang(RRX.RDFXML_StAX2_ev_aalto)
                    .checking(false)
                    .parse(g);
        }
        return g;
    }

    @Benchmark
    public Graph parseCIMXML() throws Exception {
        final var profile = heuristicallyGuessProfile(param0_GraphUri);
        final var g = schemaRegistry.parseRDFXML(profile, param0_GraphUri);
        return g;
    }

    private static void readProfiles(SchemaRegistry schemaRegistry) throws IOException {
        for(var profile : ALL_PROFILES) {
            final var pathToRDFS = getPathToRDFS(profile);
            schemaRegistry.register(profile, pathToRDFS);
            //System.out.println("Read schema " + profile);
        }
    }

    private static String getPathToRDFS(Node profileAsNode) {
        if (IRI_FOR_PROFILE_EQ == profileAsNode)
            return "C:\\rd\\bewegungsdaten-demo\\shared\\ENTSOE_RDF\\src\\main\\resources\\CGMES\\v2.4.15\\CGMES2415_Components_2020\\RDFS\\EquipmentProfileCoreRDFSAugmented-v2_4_15-4Sep2020.rdf";
        if (IRI_FOR_PROFILE_SSH == profileAsNode)
            return "C:\\rd\\bewegungsdaten-demo\\shared\\ENTSOE_RDF\\src\\main\\resources\\CGMES\\v2.4.15\\CGMES2415_Components_2020\\RDFS\\SteadyStateHypothesisProfileRDFSAugmented-v2_4_15-4Sep2020.rdf";
        if (IRI_FOR_PROFILE_SV == profileAsNode)
            return "C:\\rd\\bewegungsdaten-demo\\shared\\ENTSOE_RDF\\src\\main\\resources\\CGMES\\v2.4.15\\CGMES2415_Components_2020\\RDFS\\StateVariableProfileRDFSAugmented-v2_4_15-4Sep2020.rdf";
        if (IRI_FOR_PROFILE_TP == profileAsNode)
            return "C:\\rd\\bewegungsdaten-demo\\shared\\ENTSOE_RDF\\src\\main\\resources\\CGMES\\v2.4.15\\CGMES2415_Components_2020\\RDFS\\TopologyProfileRDFSAugmented-v2_4_15-4Sep2020.rdf";
        throw new IllegalArgumentException("Unsupported profile: " + profileAsNode);
    }

    private static Node heuristicallyGuessProfile(String filename) {
        if(filename.contains("_EQ_"))
            return IRI_FOR_PROFILE_EQ;
        if(filename.contains("_SSH_"))
            return IRI_FOR_PROFILE_SSH;
        if(filename.contains("_SV_"))
            return IRI_FOR_PROFILE_SV;
        if(filename.contains("_TP_"))
            return IRI_FOR_PROFILE_TP;
        throw new RuntimeException("Unknown profile: " + filename);
    }

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        this.schemaRegistry = new SchemaRegistry();
        readProfiles(schemaRegistry);
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JMHDefaultOptions.getDefaults(this.getClass())
                //.warmupIterations(0)
                //.measurementIterations(1)
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }

}

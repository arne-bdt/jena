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
import org.apache.jena.jmh.helper.TestFileInventory;
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
            TestFileInventory.XML_REAL_GRID_V2_EQ,
            TestFileInventory.XML_REAL_GRID_V2_SSH,
            TestFileInventory.XML_REAL_GRID_V2_SV,
            TestFileInventory.XML_REAL_GRID_V2_TP

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
                    .forceLang(RRX.RDFXML_StAX2_sr_aalto)
                    .checking(false)
                    .parse(g);
        }
        return g;
    }

    @Benchmark
    public Graph parseCIMXML() throws Exception {
        final var filePath = TestFileInventory.getFilePath(param0_GraphUri);
        final var profile = heuristicallyGuessProfile(filePath);
        final var g = schemaRegistry.parseRDFXML(profile, filePath);
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
            return TestFileInventory.getFilePath(TestFileInventory.RDF_EQUIPMENT_CORE_PROFILE_RDFS_AUGMENTED_V2_4_15_4SEP2020);
        if (IRI_FOR_PROFILE_SSH == profileAsNode)
            return TestFileInventory.getFilePath(TestFileInventory.RDF_STEADY_STATE_HYPOTHESIS_PROFILE_RDFS_AUGMENTED_V2_4_15_4SEP2020);
        if (IRI_FOR_PROFILE_SV == profileAsNode)
            return TestFileInventory.getFilePath(TestFileInventory.RDF_STATE_VARIABLE_PROFILE_RDFS_AUGMENTED_V2_4_15_4SEP2020);
        if (IRI_FOR_PROFILE_TP == profileAsNode)
            return TestFileInventory.getFilePath(TestFileInventory.RDF_TOPOLOGY_PROFILE_RDFS_AUGMENTED_V2_4_15_4SEP2020);
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
//                .warmupIterations(5)
//                .measurementIterations(10)
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }

}

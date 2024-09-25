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

import org.apache.jena.cimxml.schema.SchemaRegistry;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.jmh.helper.TestFileInventory;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.ZipFile;

@State(Scope.Benchmark)
public class TestParseCIMXML {

    final static Node IRI_FOR_PROFILE_EQ = NodeFactory.createURI("cgmes2_4_15:EQ");
    final static Node IRI_FOR_PROFILE_SSH = NodeFactory.createURI("cgmes2_4_15:SSH");
    final static Node IRI_FOR_PROFILE_SV = NodeFactory.createURI("cgmes2_4_15:SV");
    final static Node IRI_FOR_PROFILE_TP = NodeFactory.createURI("cgmes2_4_15:TP");

    final static Node[] ALL_PROFILES = new Node[] { IRI_FOR_PROFILE_EQ, IRI_FOR_PROFILE_SSH, IRI_FOR_PROFILE_SV, IRI_FOR_PROFILE_TP };

    private ExecutorService executorService;
    private SchemaRegistry schemaRegistry;

    @Benchmark
    public ConcurrentHashMap<Node, Graph> readRealGrid() throws Exception {
        final var graphRegistry = new ConcurrentHashMap<Node, Graph>();
        readCGMESFromZipArchive("C:\\rd\\bewegungsdaten-demo\\shared\\ENTSOE_RDF\\src\\main\\resources\\CGMES\\v2.4.15\\TestConfigurations_packageCASv2.0\\RealGrid\\CGMES_v2.4.15_RealGridTestConfiguration_v2.zip",
                executorService, schemaRegistry, graphRegistry);
        return graphRegistry;
    }

    private static void readCGMESFromZipArchive(String pathToZipArchive, ExecutorService executorService, SchemaRegistry schemaRegistry, ConcurrentHashMap<Node, Graph> graphRegistry) throws Exception {
        try (final ZipFile zipFile = new ZipFile(pathToZipArchive)) {
            // Get all entries and filter for .xml files
            final var entries = zipFile.entries();
            final var futures = new ArrayList<Future<?>>();
            while(entries.hasMoreElements()) {
                final var entry = entries.nextElement();

                if (entry.isDirectory() || !entry.getName().endsWith(".xml"))
                    continue;

                futures.add(executorService.submit(() -> {
                    try (final InputStream is = zipFile.getInputStream(entry)) {
                        final var profile = heuristicallyGuessProfile(entry.getName());
                        while (!schemaRegistry.contains(profile)) {
                            Thread.onSpinWait();
                        }
                        graphRegistry.put(profile, schemaRegistry.parseRDFXML(profile, is));
                        //System.out.println("Read profile " + profile);
                    } catch (Exception e) {
                        System.out.println("Failed to read entry " + entry.getName() + ": " + e.getMessage());
                        throw new RuntimeException(e);
                    }
                }));
            }
            while (futures.stream().filter(f -> !f.isDone()).findAny().isPresent()) {
                Thread.onSpinWait();
            }
        }
    }

    private static void readProfiles(ExecutorService executorService, SchemaRegistry schemaRegistry) throws IOException {
        for(var profile : ALL_PROFILES) {
            final var pathToRDFS = getPathToRDFS(profile);
            executorService.submit(() -> {
                schemaRegistry.register(profile, pathToRDFS);
                //System.out.println("Read schema " + profile);
            });
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
        this.executorService = Executors.newWorkStealingPool();

        this.schemaRegistry = new SchemaRegistry();
        readProfiles(executorService, schemaRegistry);

        //wait for schemas to be loaded
        for (Node profile : ALL_PROFILES) {
            while (!schemaRegistry.contains(profile)) {
                Thread.onSpinWait();
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        executorService.shutdown();
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

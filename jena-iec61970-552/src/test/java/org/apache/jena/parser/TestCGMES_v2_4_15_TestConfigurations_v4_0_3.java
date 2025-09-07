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

package org.apache.jena.parser;

import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.cimxml.parser.CimXmlParser;
import org.apache.jena.cimxml.sparql.core.CimDatasetGraph;
import org.apache.jena.irix.SystemIRIx;
import org.apache.jena.sys.JenaSystem;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static org.junit.Assert.fail;

public class TestCGMES_v2_4_15_TestConfigurations_v4_0_3 {

    private final static Path RDFS_FOLDER = Paths.get("C://temp/CGMES2415_Components_2020/RDFS");
    private final static Path TEST_CONFIGURATIONS_FOLDER = Path.of("C://temp/CGMES_v2.4.15_TestConfigurations_v4.0.3");
    private final static ExecutorService executor = Executors.newWorkStealingPool();
    private final static CimXmlParser cimParser = new CimXmlParser();

    @BeforeClass
    public static void beforeClassRegisterAllProfiles() throws IOException, InterruptedException, ExecutionException {
        JenaSystem.init();
        SystemIRIx.reset();
        Lib.setenv(SystemIRIx.sysPropertyProvider, "IRI3986");
        final var futures = new ArrayList<Future<?>>();
        for(var rdfPath : Files.newDirectoryStream(RDFS_FOLDER, "*.rdf")) {
            //futures.add(executor.submit(() -> {
                System.out.println("Loading " + rdfPath.getFileName());
                try {
                    cimParser.parseAndRegisterCimProfile(rdfPath);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            //}));
        }
        for(var future : futures) {
            future.get();
        }
    }


    @Ignore
    @Test
    public void walkFolderAndParseAllXmlFilesAsCimXml() throws Exception {
        var cimDatasets = new ArrayList<CimDatasetGraph>();
        try (Stream<Path> paths = Files.walk(TEST_CONFIGURATIONS_FOLDER)) {
            paths.filter(p -> Files.isRegularFile(p))
                 .filter(p -> p.getFileName().toString().endsWith(".xml"))
                 .forEach(xmlFile -> {
                     System.out.println("Loading " + xmlFile.toAbsolutePath());
                        try {
                            var cimDataset = cimParser.parseCimModel(xmlFile);
                            cimDatasets.add(cimDataset);
                        } catch (Exception e) {
                            System.out.println("Error parsing " + xmlFile.toAbsolutePath());
                        }
                 });
        }
    }

    @Ignore
    @Test
    public void parseCimXml() throws Exception {
        final CimDatasetGraph cimDataset;
        try {
            cimDataset = cimParser.parseCimModel(Paths.get("C://temp/CGMES_v2.4.15_TestConfigurations_v4.0.3/MicroGrid/Type1_T1/T1_BE_Difference_v2/MicroGridTestConfiguration_T1_BE_EQ_DIFF1_V2.xml"));
        } catch (IOException e) {
            fail("IOException: " + e.getMessage());
        }
    }
}

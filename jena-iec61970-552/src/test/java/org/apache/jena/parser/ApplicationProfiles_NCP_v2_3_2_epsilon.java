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

public class ApplicationProfiles_NCP_v2_3_2_epsilon {

    private final static Path RDFS_FOLDER = Paths.get("C://temp/ApplicationProfiles_NCP_v2-3-2-epsilon");
    private final static CimXmlParser cimParser = new CimXmlParser();



    @Ignore
    @Test
    public void readeRDFS() throws Exception {
        JenaSystem.init();
        SystemIRIx.reset();
        Lib.setenv(SystemIRIx.sysPropertyProvider, "IRI3986");
        for(var rdfPath : Files.newDirectoryStream(RDFS_FOLDER, "*.rdf")) {
            System.out.println("Loading " + rdfPath.toAbsolutePath());
            try {
                cimParser.parseAndRegisterCimProfile(rdfPath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

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

package org.apache.jena.rfc3986;

import org.apache.commons.io.input.BufferedFileChannelInputStream;
import org.apache.jena.JMHDefaultOptions;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.irix.SystemIRIx;
import org.apache.jena.mem2.GraphMem2Roaring;
import org.apache.jena.mem2.IndexingStrategy;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sys.JenaSystem;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@State(Scope.Benchmark)
public class BenchmarkIRI3986 {

    static {
        Lib.setenv(SystemIRIx.sysPropertyProvider, "IRI3986");
        JenaSystem.init();
        org.apache.shadedJena550.sys.JenaSystem.init();
        SystemIRIx.reset();
    }

    @Param({"current", "5.5.0"})
    public String param0_jenaVersion;

    private Function<String, Object> parser;
    private List<String> iriStringsToResolve;

    private static List<String> interceptIriCreateCallsAndCollectIriStrings(String rdfXmlFilePath) throws IOException {
        final var irisToCreate = new ArrayList<String>();
        var iriProvider = SystemIRIx.getProvider();
        var spy = Mockito.spy(iriProvider);
        Mockito.doAnswer((invocation) -> {
            String iriStr = invocation.getArgument(0);
            if (iriStr == null || iriStr.isEmpty()) {
                throw new IllegalArgumentException("IRI cannot be null or empty");
            }
            irisToCreate.add(iriStr);
            return iriProvider.create(iriStr);
        }).when(spy).create(Mockito.anyString());
        SystemIRIx.setProvider(spy);

        var graph = new GraphMem2Roaring(IndexingStrategy.LAZY);
        var filePath = Path.of(rdfXmlFilePath);
        final int MAX_BUFFER_SIZE = 256*4096;
        final long fileSize = java.nio.file.Files.size(filePath);

        try (var channel = FileChannel.open(filePath, StandardOpenOption.READ);
             final var is = new BufferedFileChannelInputStream.Builder().setFileChannel(channel)
                     .setOpenOptions(StandardOpenOption.READ)
                     .setBufferSize((fileSize < MAX_BUFFER_SIZE) ? (int) fileSize : MAX_BUFFER_SIZE)
                     .get()) {
            RDFParser.create()
                    .source(is)
                    .lang(org.apache.jena.riot.Lang.RDFXML)
                    .parse(graph);
        }
        // print number of IRIs to resolve
        System.out.println("Number of IRIs to resolve: " + irisToCreate.size());

        // reset the IRI provider to the original one
        SystemIRIx.setProvider(iriProvider);

        return irisToCreate;
    }


    @Setup
    public void setupTrial() throws IOException {
        iriStringsToResolve = interceptIriCreateCallsAndCollectIriStrings(
                "C:\\rd\\jena\\jena-benchmarks\\testing\\BSBM\\bsbm-5m.xml");

        switch (param0_jenaVersion) {
            case "current":
                parser = IRI3986::create;
                break;
            case "5.5.0":
                parser = org.apache.shadedJena550.rfc3986.IRI3986::create;
                break;
            default:
                throw new IllegalArgumentException("Unknown jena version: " + param0_jenaVersion);
        }
    }

//    @Ignore
//    @Test
//    public void testParse() throws IOException {
//        iriStringsToResolve = interceptIriCreateCallsAndCollectIriStrings(
//                "C:\\rd\\jena\\jena-benchmarks\\testing\\BSBM\\bsbm-5m.xml");
//    }

    @Benchmark
    public Object parse() {
        var hash = 1;
        for(var i=0; i<5; i++) {
            for (String iriStr : iriStringsToResolve) {
                final Object iri = parser.apply(iriStr);
                hash ^= iri.hashCode();
            }
        }
        return hash;
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JMHDefaultOptions.getDefaults(this.getClass()).build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }
}

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
import org.apache.jena.graph.Graph;
import org.apache.jena.jmh.helper.TestFileInventory;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.lang.rdfxml.RRX;
import org.apache.jena.sparql.graph.GraphFactory;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;

import java.nio.file.StandardOpenOption;

@State(Scope.Benchmark)
public class TestXMLParser {

    @Param({
//            TestFileInventory.XML_XXX_CGMES_EQ,
//            TestFileInventory.XML_XXX_CGMES_SSH,
//            TestFileInventory.XML_XXX_CGMES_TP,

            TestFileInventory.XML_REAL_GRID_V2_EQ,
//            TestFileInventory.XML_REAL_GRID_V2_SSH,
//            TestFileInventory.XML_REAL_GRID_V2_SV,
//            TestFileInventory.XML_REAL_GRID_V2_TP,

//            TestFileInventory.XML_BSBM_5M,
//            TestFileInventory.XML_BSBM_25M,
//            TestFileInventory.RDF_CITATIONS,
    })
    public String param0_GraphUri;
    @Param({
            "RRX.RDFXML_SAX",
//            "RRX.RDFXML_StAX_ev",
//            "RRX.RDFXML_StAX_sr",

//            "RRX.RDFXML_StAX2_ev",
//            "RRX.RDFXML_StAX2_sr",

//            "RRX.RDFXML_StAX2_ev_aalto",
            "RRX.RDFXML_StAX2_sr_aalto",

//            "RRX.RDFXML_ARP0",
//            "RRX.RDFXML_ARP1"
    })
    public String param1_ParserLang;

//    @Param({ "1048576",
//             "524288",
//             "262144",
//             "131072",
//              "65536",
//              "32768",
//    })
//    public String param2_BufferSize;

    private static Lang getLang(String langName) {
        switch (langName) {
            case "RRX.RDFXML_SAX":
                return RRX.RDFXML_SAX;
            case "RRX.RDFXML_StAX_ev":
                return RRX.RDFXML_StAX_ev;
            case "RRX.RDFXML_StAX_sr":
                return RRX.RDFXML_StAX_sr;

            case "RRX.RDFXML_StAX2_ev":
                return RRX.RDFXML_StAX2_ev;
            case "RRX.RDFXML_StAX2_sr":
                return RRX.RDFXML_StAX2_sr;

            case "RRX.RDFXML_StAX2_ev_aalto":
                return RRX.RDFXML_StAX2_ev_aalto;
            case "RRX.RDFXML_StAX2_sr_aalto":
                return RRX.RDFXML_StAX2_sr_aalto;

            case "RRX.RDFXML_ARP0":
                return RRX.RDFXML_ARP0;
            case "RRX.RDFXML_ARP1":
                return RRX.RDFXML_ARP1;

            default:
                throw new IllegalArgumentException("Unknown lang: " + langName);
        }
    }

//    @Benchmark
//    public Graph parseXMLUsingDefault() throws Exception {
////        final var stopWatch = StopWatch.createStarted();
//        final var sink = GraphFactory.createGraphMem();
//        RDFParser.source(TestFileInventory.getFilePath(this.param0_GraphUri))
//                .base(BaseURI.DEFAULT_BASE_URI)  // base URI for the model and thus for al mRID's in the model
//                .forceLang(getLang(this.param1_ParserLang))
//                .checking(false)
//                .parse(sink);
////        stopWatch.stop();
////        System.out.println("Triples in graph: " + sink.size());
////        System.out.println(stopWatch);
//        return sink;
//    }

    @Benchmark
    public Graph parseXMLUsingBufferedInputStream() throws Exception {
//        final var stopWatch = StopWatch.createStarted();
        final var sink = GraphFactory.createGraphMem();
        try(final var is = new BufferedFileChannelInputStream.Builder()
                .setFile(TestFileInventory.getFilePath(this.param0_GraphUri))
                .setOpenOptions(StandardOpenOption.READ)
                .setBufferSize(64*4096)
                .get()) {
            RDFParser.source(is)
                    .base(BaseURI.DEFAULT_BASE_URI)  // base URI for the model and thus for al mRID's in the model
                    .forceLang(getLang(this.param1_ParserLang))
                    .checking(false)
                    .parse(sink);
        }
//        stopWatch.stop();
//        System.out.println("Triples in graph: " + sink.size());
//        System.out.println(stopWatch);
        return sink;
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JMHDefaultOptions.getDefaults(this.getClass())
//                .warmupIterations(3)
//                .measurementIterations(3)
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }

}

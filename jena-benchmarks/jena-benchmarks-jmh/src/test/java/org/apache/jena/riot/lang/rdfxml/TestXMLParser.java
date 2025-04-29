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

package org.apache.jena.riot.lang.rdfxml;

import org.apache.commons.io.input.BufferedFileChannelInputStream;
import org.apache.jena.graph.Graph;
import org.apache.jena.irix.SystemIRIx;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.mem2.GraphMem2Fast;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.math.BigDecimal;
import java.nio.file.StandardOpenOption;

@State(Scope.Benchmark)
public class TestXMLParser {

    @Param({
//            "IRI0",
            "IRI3986"
    })
    public String param0_IriProvider;


    @Param({
            "RRX.RDFXML_SAX",
//            "RRX.RDFXML_StAX_ev",
//            "RRX.RDFXML_StAX_sr",

//            "RRX.RDFXML_ARP0",
//            "RRX.RDFXML_ARP1"
    })
    public String param1_ParserLang;

    @Param({
            "AMP_Export.rdf",
//            "cheeses-0.1.ttl",
//            "pizza.owl.rdf",
//            "xxx_CGMES_EQ.xml",
//            "xxx_CGMES_SSH.xml",
//            "xxx_CGMES_TP.xml",
//            "RealGrid_EQ.xml",
//            "RealGrid_SSH.xml",
//            "RealGrid_TP.xml",
//            "RealGrid_SV.xml",
//            "../testing/BSBM/bsbm-1m.nt.gz",
//            "../testing/BSBM/bsbm-5m.nt.gz",
//            "../testing/BSBM/bsbm-25m.nt.gz",
//            "RealGrid_EQ100.xml"
    })
    public String param2_GraphUri;

    private static String getFilePath(String fileName) {
        switch (fileName){
            case "AMP_Export.rdf":
                return "C:/temp/AMP_Export.rdf";
            case "cheeses-0.1.ttl":
                return "C:/temp/res_test/cheeses-0.1.ttl";
            case "pizza.owl.rdf":
                return "C:/temp/res_test/pizza.owl.rdf";
            case "xxx_CGMES_EQ.xml":
                return "C:/temp/res_test/xxx_CGMES_EQ.xml";
            case "xxx_CGMES_SSH.xml":
                return "C:/temp/res_test/xxx_CGMES_SSH.xml";
            case "xxx_CGMES_TP.xml":
                return "C:/temp/res_test/xxx_CGMES_TP.xml";
            case "RealGrid_EQ.xml":
                return "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml";
            case "RealGrid_SSH.xml":
                return "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml";
            case "RealGrid_TP.xml":
                return "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml";
            case "RealGrid_SV.xml":
                return "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml";
            case "bsbm-1m.nt.gz":
                return "../testing/BSBM/bsbm-1m.nt.gz";
            case "bsbm-5m.nt.gz":
                return "../testing/BSBM/bsbm-5m.nt.gz";
            case "bsbm-25m.nt.gz":
                return "../testing/BSBM/bsbm-25m.nt.gz";
            case "RealGrid_EQ100.xml":
                return "C:/temp/RealGrid_EQ100.xml";

            default:
                throw new IllegalArgumentException("Unknown file: " + fileName);
        }
    }

    @SuppressWarnings("removal")
    private static Lang getLang(String langName) {
        switch (langName) {
            case "RRX.RDFXML_SAX":
                return RRX.RDFXML_SAX;
            case "RRX.RDFXML_StAX_ev":
                return RRX.RDFXML_StAX_ev;
            case "RRX.RDFXML_StAX_sr":
                return RRX.RDFXML_StAX_sr;

            case "RRX.RDFXML_ARP0":
                return RRX.RDFXML_ARP0;
            case "RRX.RDFXML_ARP1":
                return RRX.RDFXML_ARP1;

            default:
                throw new IllegalArgumentException("Unknown lang: " + langName);
        }
    }

    @SuppressWarnings("removal")
    private static org.apache.shadedJena530.riot.Lang getLangJena530(String langName) {
        switch (langName) {
            case "RRX.RDFXML_SAX":
                return org.apache.shadedJena530.riot.lang.rdfxml.RRX.RDFXML_SAX;
            case "RRX.RDFXML_StAX_ev":
                return org.apache.shadedJena530.riot.lang.rdfxml.RRX.RDFXML_StAX_ev;
            case "RRX.RDFXML_StAX_sr":
                return org.apache.shadedJena530.riot.lang.rdfxml.RRX.RDFXML_StAX_sr;

            case "RRX.RDFXML_ARP0":
                return org.apache.shadedJena530.riot.lang.rdfxml.RRX.RDFXML_ARP0;
            case "RRX.RDFXML_ARP1":
                return org.apache.shadedJena530.riot.lang.rdfxml.RRX.RDFXML_ARP1;

            default:
                throw new IllegalArgumentException("Unknown lang: " + langName);
        }
    }

    /**
     * This method is used to get the memory consumption of the current JVM.
     *
     * @return the memory consumption in MB
     */
    private static double runGcAndGetUsedMemoryInMB() {
        System.runFinalization();
        System.gc();
        Runtime.getRuntime().runFinalization();
        Runtime.getRuntime().gc();
        return BigDecimal.valueOf(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()).divide(BigDecimal.valueOf(1024L)).divide(BigDecimal.valueOf(1024L)).doubleValue();
    }

    @Benchmark
    public Graph parseXML() throws Exception {
        var memoryBefore = runGcAndGetUsedMemoryInMB();
        final var graph = new GraphMem2Fast();
        try(final var is = new BufferedFileChannelInputStream.Builder()
                .setFile(getFilePath(this.param2_GraphUri))
                .setOpenOptions(StandardOpenOption.READ)
                .setBufferSize(64*4096)
                .get()) {
            RDFParser.source(is)
                    .base("xx:")
                    .forceLang(getLang(this.param1_ParserLang))
                    .checking(false)
                    .parse(graph);
        }

        var memoryAfter = runGcAndGetUsedMemoryInMB();
        System.out.printf("graphs: %d additional memory: %5.3f MB%n",
                graph.size(),
                (memoryAfter - memoryBefore));
        return graph;
    }

//    @Benchmark
//    public org.apache.shadedJena530.graph.Graph parseXMLJena530() throws Exception {
//        final var graph = new org.apache.shadedJena530.mem2.GraphMem2Fast();
//        try(final var is = new BufferedFileChannelInputStream.Builder()
//                .setFile(getFilePath(this.param2_GraphUri))
//                .setOpenOptions(StandardOpenOption.READ)
//                .setBufferSize(64*4096)
//                .get()) {
//            org.apache.shadedJena530.riot.RDFParser.source(is)
//                    .base("xx:")
//                    .forceLang(getLangJena530(this.param1_ParserLang))
//                    .checking(false)
//                    .parse(graph);
//        }
//        return graph;
//    }

    @Setup(Level.Trial)
    public void setup() {
        System.setProperty("jena.iriprovider", param0_IriProvider);
        org.apache.shadedJena530.irix.SystemIRIx.reset();
        SystemIRIx.reset();


    }

    @Test
    public void benchmark() throws Exception {
        var opt = JMHDefaultOptions.getDefaults(this.getClass())
                .warmupIterations(1)
                .measurementIterations(1)
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }

}

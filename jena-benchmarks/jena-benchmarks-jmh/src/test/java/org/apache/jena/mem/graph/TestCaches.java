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

package org.apache.jena.mem.graph;

import org.apache.jena.atlas.lib.Cache;
import org.apache.jena.atlas.lib.CacheFactory;
import org.apache.jena.atlas.lib.cache.CacheSimpleFastConcurrent;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.mem.graph.helper.TripleReaderReadingCGMES_2_4_15_WithTypedLiterals;
import org.apache.jena.mem2.GraphMem2Fast;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

@State(Scope.Benchmark)
public class TestCaches {

    @Param({
//            "cheeses-0.1.ttl",
//            "pizza.owl.rdf",
//            "xxx_CGMES_EQ.xml",
//            "xxx_CGMES_SSH.xml",
//            "xxx_CGMES_TP.xml",
            "RealGrid_EQ.xml",
            "RealGrid_SSH.xml",
            "RealGrid_TP.xml",
            "RealGrid_SV.xml",
//            "bsbm-1m.nt.gz",
//            "bsbm-5m.nt.gz",
//            "bsbm-25m.nt.gz",
    })
    public String param0_GraphUri;

    private static String getFilePath(String fileName) {
        switch (fileName) {
            case "cheeses-0.1.ttl":
                return "../testing/cheeses-0.1.ttl";
            case "pizza.owl.rdf":
                return "../testing/pizza.owl.rdf";
            case "xxx_CGMES_EQ.xml":
                return "C:/temp/res_test/xxx_CGMES_EQ.xml";
            case "xxx_CGMES_SSH.xml":
                return "C:/temp/res_test/xxx_CGMES_SSH.xml";
            case "xxx_CGMES_TP.xml":
                return "C:/temp/res_test/xxx_CGMES_TP.xml";
            case "RealGrid_EQ.xml":
                return "C:/rd/CGMES/TestConfigurations_packageCASv2.0/RealGrid/CGMES_v2.4.15_RealGridTestConfiguration_EQ_V2.xml";
            case "RealGrid_SSH.xml":
                return "C:/rd/CGMES/TestConfigurations_packageCASv2.0/RealGrid/CGMES_v2.4.15_RealGridTestConfiguration_SSH_V2.xml";
            case "RealGrid_TP.xml":
                return "C:/rd/CGMES/TestConfigurations_packageCASv2.0/RealGrid/CGMES_v2.4.15_RealGridTestConfiguration_TP_V2.xml";
            case "RealGrid_SV.xml":
                return "C:/rd/CGMES/TestConfigurations_packageCASv2.0/RealGrid/CGMES_v2.4.15_RealGridTestConfiguration_SV_V2.xml";
            case "bsbm-1m.nt.gz":
                return "../testing/BSBM/bsbm-1m.nt.gz";
            case "bsbm-5m.nt.gz":
                return "../testing/BSBM/bsbm-5m.nt.gz";
            case "bsbm-25m.nt.gz":
                return "../testing/BSBM/bsbm-25m.nt.gz";
            default:
                throw new IllegalArgumentException("Unknown fileName: " + fileName);
        }
    }

    @Param({
            "Caffeine",
//            "Simple",
//            "SimpleFast",
            "SimpleFastConcurrent"
    })
    public String param1_Cache;

    private Graph graph;

    private Cache<String, Node> cache;

    private static Cache<String, Node> createCache(String cacheName) {
        //final var defaultCacheSize = FactoryRDFCaching.DftNodeCacheSize;
        final var defaultCacheSize = 1_000_000;
        // the cache size is a power of 2 --> that is a requirement for the CacheSimpleFast
        // to start with fair conditions for the caches, we use the same size for all caches
        var cacheSize = Integer.highestOneBit(defaultCacheSize);
        if (cacheSize < defaultCacheSize) {
            cacheSize <<= 1;
        }
        switch (cacheName) {
            case "Caffeine":
                return CacheFactory.createCache(cacheSize);
            case "Simple":
                return CacheFactory.createSimpleCache(cacheSize);
            case "SimpleFast":
                return CacheFactory.createSimpleFastCache(cacheSize);
            case "SimpleFastConcurrent":
                return new CacheSimpleFastConcurrent(cacheSize);
            default:
                throw new IllegalArgumentException("Unknown Cache: " + cacheName);
        }
    }

    @Benchmark
    public int updateFilledCache() {
        final int[] hash = {0};
        graph.find().forEachRemaining(t -> {
            if(t.getSubject().isURI()) {
                hash[0] += cache.get(t.getSubject().getURI(),
                        s -> t.getSubject()).getURI().hashCode();

            }
            if(t.getPredicate().isURI()) {
                hash[0] += cache.get(t.getPredicate().getURI(),
                        s -> t.getPredicate()).getURI().hashCode();

            }
            if(t.getObject().isURI()) {
                hash[0] += cache.get(t.getObject().getURI(),
                        s -> t.getObject()).getURI().hashCode();

            }
        });
        return hash[0];
    }

//    @Benchmark
//    public Cache<String, Node> createAndFillCacheByGet() {
//        var c = createCache(param1_Cache);
//        fillCacheByGet(c, graph);
//        return c;
//    }

    private static void fillCacheByGet(Cache<String, Node> cacheToFill, Graph g) {
        g.find().forEachRemaining(t -> {
            if(t.getSubject().isURI()) {
                cacheToFill.get(t.getSubject().getURI(), s -> t.getSubject());
            }
            if(t.getPredicate().isURI()) {
                cacheToFill.get(t.getPredicate().getURI(), s -> t.getPredicate());
            }
            if(t.getObject().isURI()) {
                cacheToFill.get(t.getObject().getURI(), s -> t.getObject());
            }
        });
    }

//    @Test
//    public void printHashCodesAsJson() throws IOException {
//        final var g = new GraphMem2Fast();
//        TripleReaderReadingCGMES_2_4_15_WithTypedLiterals.read(getFilePath("xxx_CGMES_EQ.xml"), g);
//        var jsonText = convertGraphToJson(g);
//        // write to c:/temp/xxx_CGMES_EQ.json using java nio
//        java.nio.file.Files.writeString(java.nio.file.Paths.get("c:/temp/xxx_CGMES_EQ.json"),
//                jsonText, StandardOpenOption.CREATE_NEW);
//    }

//    public static String convertGraphToJson(Graph graph) {
//        List<Map<String, Integer>> triplesList = new ArrayList<>();
//        ExtendedIterator<Triple> it = graph.find(null, null, null);
//
//        while (it.hasNext()) {
//            Triple triple = it.next();
//            Map<String, Integer> tripleMap = new HashMap<>();
//
//            // Assuming hashCodes are integers and directly usable
//            tripleMap.put("x", triple.getSubject().hashCode());
//            tripleMap.put("y", triple.getPredicate().hashCode());
//            tripleMap.put("z", triple.getObject().hashCode());
//
//            triplesList.add(tripleMap);
//        }
//
//        // Convert to JSON (you can use Gson, Jackson, or any other library)
//        // Here's a simple manual conversion for illustration
//        StringBuilder jsonBuilder = new StringBuilder("[");
//        for (Map<String, Integer> tripleMap : triplesList) {
//            jsonBuilder.append(String.format(
//                    "{\"x\": %d, \"y\": %d, \"z\": %d},",
//                    tripleMap.get("x"), tripleMap.get("y"), tripleMap.get("z")
//            ));
//        }
//        if (jsonBuilder.length() > 1) jsonBuilder.setLength(jsonBuilder.length() - 1); // Remove last comma
//        jsonBuilder.append("]");
//
//        return jsonBuilder.toString();
//    }

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        this.graph = new GraphMem2Fast();
        TripleReaderReadingCGMES_2_4_15_WithTypedLiterals.read(getFilePath(param0_GraphUri), this.graph);
        this.cache = createCache(param1_Cache);
    }

//    @Setup(Level.Iteration)
//    public void setupIteration() {
//        fillCacheByGet(this.cache, this.graph);
//    }

    @Test
    public void benchmark() throws Exception {
        var opt = JMHDefaultOptions.getDefaults(this.getClass())
                .threads(8)
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }
}

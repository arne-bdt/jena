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
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.mem.graph.helper.TripleReaderReadingCGMES_2_4_15_WithTypedLiterals;
import org.apache.jena.mem2.GraphMem2Fast;
import org.apache.jena.riot.system.FactoryRDFCaching;
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
//            "RealGrid_EQ.xml",
//            "RealGrid_SSH.xml",
//            "RealGrid_TP.xml",
//            "RealGrid_SV.xml",
            "bsbm-1m.nt.gz",
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
            default:
                throw new IllegalArgumentException("Unknown fileName: " + fileName);
        }
    }

    @Param({
            "Simple",
            "SimpleFast",
    })
    public String param1_Cache;

    private Graph graph;

    private Cache<String, Node> cache;

    private static Cache<String, Node> createCache(String cacheName) {
        // the cache size is a power of 2 --> that is a requirement for the CacheSimpleFast
        // to start with fair conditions for the caches, we use the same size for all caches
        var cacheSize = Integer.highestOneBit(FactoryRDFCaching.DftNodeCacheSize);
        if (cacheSize < FactoryRDFCaching.DftNodeCacheSize) {
            cacheSize <<= 1;
        }
        switch (cacheName) {
            case "Simple":
                return CacheFactory.createSimpleCache(cacheSize);
            case "SimpleFast":
                return CacheFactory.createSimpleFastCache(cacheSize);
            default:
                throw new IllegalArgumentException("Unknown Cache: " + cacheName);
        }
    }

    @Benchmark
    public int updateFilledCache() {
        final int[] count = {0};
        graph.find().forEachRemaining(t -> {
            if(t.getSubject().isURI()) {
                cache.get(t.getSubject().getURI(),
                        s -> {
                            count[0]++;
                            return t.getSubject();
                        });
            }
            if(t.getPredicate().isURI()) {
                cache.get(t.getPredicate().getURI(),
                        s -> {
                            count[0]++;
                            return t.getPredicate();
                        });
            }
            if(t.getObject().isURI()) {
                cache.get(t.getObject().getURI(),
                        s -> {
                            count[0]++;
                            return t.getObject();
                        });
            }
        });
        return count[0];
    }

    @Benchmark
    public Cache<String, Node> createAndFillCacheByGet() {
        var c = createCache(param1_Cache);
        fillCacheByGet(c, graph);
        return c;
    }

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

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        this.graph = new GraphMem2Fast();
        TripleReaderReadingCGMES_2_4_15_WithTypedLiterals.read(getFilePath(param0_GraphUri), this.graph);
    }

    @Setup(Level.Iteration)
    public void setupIteration() {
        this.cache = createCache(param1_Cache);
        fillCacheByGet(this.cache, this.graph);
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JMHDefaultOptions.getDefaults(this.getClass())
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }
}

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

package org.apache.jena.mem.set.triple;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem.set.helper.JMHDefaultOptions;
import org.apache.jena.mem.graph.helper.Releases;
import org.apache.jena.mem2.collection.*;
import org.apache.jena.mem2.collection.discarded.FastTripleHashSet;
import org.apache.jena.mem2.collection.discarded.FastTripleHashSet3;
import org.apache.jena.mem2.collection.discarded.FastTripleHashSet4;
import org.apache.jena.mem2.collection.discarded.FastTripleHashSet5;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.util.HashSet;
import java.util.List;

@State(Scope.Benchmark)
public class TestSetRemove {

    @Param({
//            "../testing/cheeses-0.1.ttl",
//            "../testing/pizza.owl.rdf",
            "C:/temp/res_test/xxx_CGMES_EQ.xml",
            "C:/temp/res_test/xxx_CGMES_SSH.xml",
            "C:/temp/res_test/xxx_CGMES_TP.xml",
//            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml",
//            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml",
//            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml",
            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml",
//            "../testing/BSBM/bsbm-1m.nt.gz",
//            "../testing/BSBM/bsbm-5m.nt.gz",
//            "../testing/BSBM/bsbm-25m.nt.gz",
    })
    public String param0_GraphUri;

    @Param({
//            "HashSet",
            "TripleSet",
//            "TripleSet2",
            "FastTripleSet",
//            "FastTripleSet2",
//            "FastTripleHashSet",
            "FastTripleHashSet2",
//            "FastTripleHashSet3",
//            "FastTripleHashSet4",
//            "FastTripleHashSet5",
    })
    public String param1_SetImplementation;

    private List<Triple> triples;
    private List<Triple> triplesToRemove;
    private HashSet<Triple> hashSet;
    private TripleSet tripleSet;
    private FastTripleSet fastTripleSet;
    private FastTripleHashSet fastTripleHashSet;
    private FastTripleHashSet2 fastTripleHashSet2;
    private FastTripleHashSet3 fastTripleHashSet3;
    private FastTripleHashSet4 fastTripleHashSet4;
    private FastTripleHashSet5 fastTripleHashSet5;



    java.util.function.Supplier<Integer> removeFromSet;

    @Benchmark
    public int setRemove() {
        return removeFromSet.get();
    }

    private int removeFromHashSet() {
        triplesToRemove.forEach(t -> this.hashSet.remove(t));
        Assert.assertTrue(this.hashSet.isEmpty());
        return this.hashSet.size();
    }
    private int removeFromTripleSet() {
        triplesToRemove.forEach(t -> this.tripleSet.remove(t));
        Assert.assertTrue(this.tripleSet.isEmpty());
        return this.tripleSet.size();
    }

    private int removeFromFastTripleSet() {
        triplesToRemove.forEach(t -> this.fastTripleSet.removeKey(t));
        Assert.assertTrue(this.fastTripleSet.isEmpty());
        return this.fastTripleSet.size();
    }

    private int removeFromFastTripleHashSet() {
        triplesToRemove.forEach(t -> this.fastTripleHashSet.remove(t));
        Assert.assertTrue(this.fastTripleHashSet.isEmpty());
        return this.fastTripleHashSet.size();
    }

    private int removeFromFastTripleHashSet2() {
        triplesToRemove.forEach(t -> this.fastTripleHashSet2.remove(t));
        Assert.assertTrue(this.fastTripleHashSet2.isEmpty());
        return this.fastTripleHashSet2.size();
    }

    private int removeFromFastTripleHashSet3() {
        triplesToRemove.forEach(t -> this.fastTripleHashSet3.remove(t));
        Assert.assertTrue(this.fastTripleHashSet3.isEmpty());
        return this.fastTripleHashSet3.size();
    }

    private int removeFromFastTripleHashSet4() {
        triplesToRemove.forEach(t -> this.fastTripleHashSet4.remove(t));
        Assert.assertTrue(this.fastTripleHashSet4.isEmpty());
        return this.fastTripleHashSet4.size();
    }

    private int removeFromFastTripleHashSet5() {
        triplesToRemove.forEach(t -> this.fastTripleHashSet5.remove(t));
        Assert.assertTrue(this.fastTripleHashSet5.isEmpty());
        return this.fastTripleHashSet5.size();
    }

//    @Test
//    public void testDeleteFromFastTripleHashSet4() {
//        var triples = Releases.current.readTriples("../testing/cheeses-0.1.ttl").subList(0, 3);
//        var triplesToRemove = Releases.current.cloneTriples(triples);
//        var set = new FastTripleHashSet4(triples.size());
//        triples.forEach(t -> {
//            if(!set.add(t)) {
//                Assert.fail();
//            }
//        });
//        assert set.size() == triples.size();
//        triplesToRemove.forEach(t -> {
//            if(!set.contains(t)) {
//                Assert.fail();
//            }
//        });
//        triplesToRemove.forEach(t -> {
//            if(set.remove(t)) {
//               if(set.contains(t)) {
//                     Assert.fail();
//               }
//            } else {
//                Assert.fail();
//            }
//        });
//        Assert.assertTrue(set.isEmpty());
//    }




    @Setup(Level.Invocation)
    public void setupInvocation() {
        switch (param1_SetImplementation) {
            case "HashSet":
                this.hashSet = new HashSet<>(triples.size());
                this.triples.forEach(hashSet::add);
                break;
            case "TripleSet":
                this.tripleSet = new TripleSet(triples.size());
                this.triples.forEach(tripleSet::add);
                break;
            case "FastTripleSet":
                this.fastTripleSet = new FastTripleSet(triples.size());
                this.triples.forEach(fastTripleSet::addKey);
                break;
            case "FastTripleHashSet":
                this.fastTripleHashSet = new FastTripleHashSet(triples.size());
                this.triples.forEach(fastTripleHashSet::add);
                break;
            case "FastTripleHashSet2":
                this.fastTripleHashSet2 = new FastTripleHashSet2(triples.size());
                this.triples.forEach(fastTripleHashSet2::add);
                break;
            case "FastTripleHashSet3":
                this.fastTripleHashSet3 = new FastTripleHashSet3(triples.size());
                this.triples.forEach(fastTripleHashSet3::add);
                break;
            case "FastTripleHashSet4":
                this.fastTripleHashSet4 = new FastTripleHashSet4(triples.size());
                this.triples.forEach(fastTripleHashSet4::add);
                break;
            case "FastTripleHashSet5":
                this.fastTripleHashSet5 = new FastTripleHashSet5(triples.size());
                this.triples.forEach(fastTripleHashSet5::add);
                break;
            default:
                throw new IllegalArgumentException("Unknown set implementation: " + param1_SetImplementation);
        }
    }

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        this.triples = Releases.current.readTriples(param0_GraphUri);
        this.triplesToRemove = Releases.current.cloneTriples(triples);
        switch (param1_SetImplementation) {
            case "HashSet":
                this.removeFromSet = this::removeFromHashSet;
                break;
            case "TripleSet":
                this.removeFromSet = this::removeFromTripleSet;
                break;
            case "FastTripleSet":
                this.removeFromSet = this::removeFromFastTripleSet;
                break;
            case "FastTripleHashSet":
                this.removeFromSet = this::removeFromFastTripleHashSet;
                break;
            case "FastTripleHashSet2":
                this.removeFromSet = this::removeFromFastTripleHashSet2;
                break;
            case "FastTripleHashSet3":
                this.removeFromSet = this::removeFromFastTripleHashSet3;
                break;
            case "FastTripleHashSet4":
                this.removeFromSet = this::removeFromFastTripleHashSet4;
                break;
            case "FastTripleHashSet5":
                this.removeFromSet = this::removeFromFastTripleHashSet5;
                break;
            default:
                throw new IllegalArgumentException("Unknown set implementation: " + param1_SetImplementation);
        }
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JMHDefaultOptions.getDefaults(this.getClass())
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }
}

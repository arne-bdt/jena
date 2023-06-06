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
import org.apache.jena.mem2.collection.discarded.*;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.util.HashSet;
import java.util.List;


@State(Scope.Benchmark)
public class TestSetAdd {

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

    java.util.function.Supplier<Object> addToSet;

    @Benchmark
    public Object addToSet() {
        return addToSet.get();
    }

    private Object addToHashSet() {
        var sut = new HashSet<Triple>();
        triples.forEach(sut::add);
        Assert.assertEquals(triples.size(), sut.size());
        return sut;
    }
    private Object addToTripleSet() {
        var sut = new TripleSet();
        triples.forEach(sut::add);
        Assert.assertEquals(triples.size(), sut.size());
        return sut;
    }

    private Object addToTripleSet2() {
        var sut = new TripleSet2();
        triples.forEach(sut::addKey);
        Assert.assertEquals(triples.size(), sut.size());
        return sut;
    }

    private Object addToFastTripleSet() {
        var sut = new FastTripleSet();
        triples.forEach(sut::addKey);
        Assert.assertEquals(triples.size(), sut.size());
        return sut;
    }

    private Object addToFastTripleSet2() {
        var sut = new FastTripleSet2();
        triples.forEach(sut::addKey);
        Assert.assertEquals(triples.size(), sut.size());
        return sut;
    }

    private Object addToFastTripleHashSet() {
        var sut = new FastTripleHashSet();
        triples.forEach(sut::add);
        Assert.assertEquals(triples.size(), sut.size());
        return sut;
    }

    private Object addToFastTripleHashSet2() {
        var sut = new FastTripleHashSet2();
        triples.forEach(sut::add);
        Assert.assertEquals(triples.size(), sut.size());
        return sut;
    }

    private Object addToFastTripleHashSet3() {
        var sut = new FastTripleHashSet3();
        triples.forEach(sut::add);
        Assert.assertEquals(triples.size(), sut.size());
        return sut;
    }

    private Object addToFastTripleHashSet4() {
        var sut = new FastTripleHashSet4();
        triples.forEach(sut::add);
        Assert.assertEquals(triples.size(), sut.size());
        return sut;
    }

    private Object addToFastTripleHashSet5() {
        var sut = new FastTripleHashSet5();
        triples.forEach(sut::add);
        Assert.assertEquals(triples.size(), sut.size());
        return sut;
    }

//    @Test
//    public void testAddToFastTripleHashSet2() {
//        var triples = Releases.current.readTriples("../testing/cheeses-0.1.ttl");
//        var sut = new FastTripleHashSet2();
//        triples.forEach(sut::add);
//        Assert.assertEquals(triples.size(), sut.size());
//    }


    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        triples = Releases.current.readTriples(param0_GraphUri);
        switch (param1_SetImplementation) {
            case "HashSet":
                this.addToSet = this::addToHashSet;
                break;
            case "TripleSet":
                this.addToSet = this::addToTripleSet;
                break;
            case "TripleSet2":
                this.addToSet = this::addToTripleSet2;
                break;
            case "FastTripleSet":
                this.addToSet = this::addToFastTripleSet;
                break;
            case "FastTripleSet2":
                this.addToSet = this::addToFastTripleSet2;
                break;
            case "FastTripleHashSet":
                this.addToSet = this::addToFastTripleHashSet;
                break;
            case "FastTripleHashSet2":
                this.addToSet = this::addToFastTripleHashSet2;
                break;
            case "FastTripleHashSet3":
                this.addToSet = this::addToFastTripleHashSet3;
                break;
            case "FastTripleHashSet4":
                this.addToSet = this::addToFastTripleHashSet4;
                break;
            case "FastTripleHashSet5":
                this.addToSet = this::addToFastTripleHashSet5;
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

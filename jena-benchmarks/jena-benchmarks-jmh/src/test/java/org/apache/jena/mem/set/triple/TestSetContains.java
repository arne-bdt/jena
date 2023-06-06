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
public class TestSetContains {

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

    private List<Triple> triplesToFind;
    private HashSet<Triple> tripleHashSet;
    private TripleSet tripleSet;
    private FastTripleSet fastTripleSet;
    private FastTripleHashSet fastTripleHashSet;
    private FastTripleHashSet2 fastTripleHashSet2;
    private FastTripleHashSet3 fastTripleHashSet3;
    private FastTripleHashSet4 fastTripleHashSet4;
    private FastTripleHashSet5 fastTripleHashSet5;

    java.util.function.Supplier<Boolean> setContains;

    @Benchmark
    public boolean setContains() {
        return setContains.get();
    }

    private boolean hashSetContains() {
        var found = false;
        for(var t: triplesToFind) {
            found = tripleHashSet.contains(t);
            Assert.assertTrue(found);
        }
        return found;
    }
    private boolean tripleSetContains() {
        var found = false;
        for(var t: triplesToFind) {
            found = tripleSet.contains(t);
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean fastTripleSetContains() {
        var found = false;
        for(var t: triplesToFind) {
            found = fastTripleSet.contains(t);
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean fastTripleHashSetContains() {
        var found = false;
        for(var t: triplesToFind) {
            found = fastTripleHashSet.contains(t);
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean fastTripleHashSet2Contains() {
        var found = false;
        for(var t: triplesToFind) {
            found = fastTripleHashSet2.contains(t);
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean fastTripleHashSet3Contains() {
        var found = false;
        for(var t: triplesToFind) {
            found = fastTripleHashSet3.contains(t);
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean fastTripleHashSet4Contains() {
        var found = false;
        for(var t: triplesToFind) {
            found = fastTripleHashSet4.contains(t);
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean fastTripleHashSet5Contains() {
        var found = false;
        for(var t: triplesToFind) {
            found = fastTripleHashSet5.contains(t);
            Assert.assertTrue(found);
        }
        return found;
    }


    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        var triples = Releases.current.readTriples(param0_GraphUri);
        this.triplesToFind = Releases.current.cloneTriples(triples);
        switch (param1_SetImplementation) {
            case "HashSet":
                this.tripleHashSet = new HashSet<>(triples.size());
                triples.forEach(tripleHashSet::add);
                this.setContains = this::hashSetContains;
                break;
            case "TripleSet":
                this.tripleSet = new TripleSet(triples.size());
                triples.forEach(tripleSet::add);
                this.setContains = this::tripleSetContains;
                break;
            case "FastTripleSet":
                this.fastTripleSet = new FastTripleSet(triples.size());
                triples.forEach(fastTripleSet::addKey);
                this.setContains = this::fastTripleSetContains;
                break;
            case "FastTripleHashSet":
                this.fastTripleHashSet = new FastTripleHashSet(triples.size());
                triples.forEach(fastTripleHashSet::add);
                this.setContains = this::fastTripleHashSetContains;
                break;
            case "FastTripleHashSet2":
                this.fastTripleHashSet2 = new FastTripleHashSet2(triples.size());
                triples.forEach(fastTripleHashSet2::add);
                this.setContains = this::fastTripleHashSet2Contains;
                break;
            case "FastTripleHashSet3":
                this.fastTripleHashSet3 = new FastTripleHashSet3(triples.size());
                triples.forEach(fastTripleHashSet3::add);
                this.setContains = this::fastTripleHashSet3Contains;
                break;
            case "FastTripleHashSet4":
                this.fastTripleHashSet4 = new FastTripleHashSet4(triples.size());
                triples.forEach(fastTripleHashSet4::add);
                this.setContains = this::fastTripleHashSet4Contains;
                break;
            case "FastTripleHashSet5":
                this.fastTripleHashSet5 = new FastTripleHashSet5(triples.size());
                triples.forEach(fastTripleHashSet5::add);
                this.setContains = this::fastTripleHashSet5Contains;
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

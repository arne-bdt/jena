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
            "HashSet",
            "HashCommonTripleSet",
            "FastHashSetOfTriples"
    })
    public String param1_SetImplementation;

    private List<Triple> triplesToFind;
    private HashSet<Triple> tripleHashSet;
    private HashCommonTripleSet hashCommonTripleSet;
    private FastHashSetOfTriples fastHashSetOfTriples;

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

    private boolean hashCommonTripleSetContains() {
        var found = false;
        for(var t: triplesToFind) {
            found = hashCommonTripleSet.containsKey(t);
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean fastHashSetOfTriplesContains() {
        var found = false;
        for(var t: triplesToFind) {
            found = fastHashSetOfTriples.containsKey(t);
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
            case "HashCommonTripleSet":
                this.hashCommonTripleSet = new HashCommonTripleSet(triples.size());
                triples.forEach(hashCommonTripleSet::addUnchecked);
                this.setContains = this::hashCommonTripleSetContains;
                break;
            case "FastHashSetOfTriples":
                this.fastHashSetOfTriples = new FastHashSetOfTriples(triples.size());
                triples.forEach(fastHashSetOfTriples::addUnchecked);
                this.setContains = this::fastHashSetOfTriplesContains;
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

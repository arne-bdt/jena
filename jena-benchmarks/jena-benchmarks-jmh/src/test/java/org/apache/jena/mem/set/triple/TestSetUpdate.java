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
import org.apache.jena.mem.graph.helper.Releases;
import org.apache.jena.mem.set.helper.JMHDefaultOptions;
import org.apache.jena.mem2.collection.FastTripleHashSet;
import org.apache.jena.mem2.collection.FastTripleHashSet2;
import org.apache.jena.mem2.collection.FastTripleSet;
import org.apache.jena.mem2.collection.TripleSet;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;

@State(Scope.Benchmark)
public class TestSetUpdate {

    @Param({
            "../testing/cheeses-0.1.ttl",
            "../testing/pizza.owl.rdf",
            "C:/temp/res_test/xxx_CGMES_EQ.xml",
            "C:/temp/res_test/xxx_CGMES_SSH.xml",
            "C:/temp/res_test/xxx_CGMES_TP.xml",
//            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml",
//            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml",
//            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml",
//            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml",
//            "../testing/BSBM/bsbm-1m.nt.gz",
//            "../testing/BSBM/bsbm-5m.nt.gz",
//            "../testing/BSBM/bsbm-25m.nt.gz",
    })
    public String param0_GraphUri;

    @Param({
            "FastTripleHashSet2",
            "HashSet",
            "TripleSet",
            "FastTripleSet",
            "FastTripleHashSet"
    })
    public String param1_SetImplementation;

    private List<Triple> triples;
    private List<Triple> triplesToRemove;
    private HashSet<Triple> hashSet;
    private TripleSet tripleSet;
    private FastTripleSet fastTripleSet;
    private FastTripleHashSet fastTripleHashSet;
    private FastTripleHashSet2 fastTripleHashSet2;


    java.util.function.Supplier<Integer> removeFromSet;

    @Benchmark
    public int setRemove() {
        return removeFromSet.get();
    }

    private int removeFromHashSet() {
        for(int i=0; i<triplesToRemove.size(); i+=10) {
            triplesToRemove.subList(i, Math.min(i+10, triplesToRemove.size()))
                    .forEach(t -> this.hashSet.remove(t));
            triplesToRemove.subList(i, Math.min(i+10, triplesToRemove.size()))
                    .forEach(t -> this.hashSet.add(t));
            assert this.hashSet.size() == triples.size();
        }
        return this.hashSet.size();
    }
    private int removeFromTripleSet() {
        for(int i=0; i<triplesToRemove.size(); i+=10) {
            triplesToRemove.subList(i, Math.min(i+10, triplesToRemove.size()))
                    .forEach(t -> this.tripleSet.removeKey(t));
            triplesToRemove.subList(i, Math.min(i+10, triplesToRemove.size()))
                    .forEach(t -> this.tripleSet.addKey(t));
            assert this.tripleSet.size() == triples.size();
        }
        return this.tripleSet.size();
    }

    private int removeFromFastTripleSet() {
        for(int i=0; i<triplesToRemove.size(); i+=10) {
            triplesToRemove.subList(i, Math.min(i+10, triplesToRemove.size()))
                    .forEach(t -> this.fastTripleSet.removeKey(t));
            triplesToRemove.subList(i, Math.min(i+10, triplesToRemove.size()))
                    .forEach(t -> this.fastTripleSet.addKey(t));
            assert this.fastTripleSet.size() == triples.size();
        }
        return this.fastTripleSet.size();
    }

    private int removeFromFastTripleHashSet() {
        for(int i=0; i<triplesToRemove.size(); i+=10) {
            triplesToRemove.subList(i, Math.min(i+10, triplesToRemove.size()))
                    .forEach(t -> this.fastTripleHashSet.remove(t));
            triplesToRemove.subList(i, Math.min(i+10, triplesToRemove.size()))
                    .forEach(t -> this.fastTripleHashSet.add(t));
            assert this.fastTripleHashSet.size() == triples.size();
        }
        return this.fastTripleHashSet.size();
    }

    private int removeFromFastTripleHashSet2() {
        for(int i=0; i<triplesToRemove.size(); i+=10) {
            triplesToRemove.subList(i, Math.min(i+10, triplesToRemove.size()))
                    .forEach(t -> this.fastTripleHashSet2.remove(t));
            triplesToRemove.subList(i, Math.min(i+10, triplesToRemove.size()))
                    .forEach(t -> this.fastTripleHashSet2.add(t));
            assert this.fastTripleHashSet2.size() == triples.size();
        }
        return this.fastTripleHashSet2.size();
    }


    @Setup(Level.Invocation)
    public void setupInvocation() {
        switch (param1_SetImplementation) {
            case "HashSet":
                this.hashSet = new HashSet<>(triples.size());
                this.triples.forEach(hashSet::add);
                break;
            case "TripleSet":
                this.tripleSet = new TripleSet(triples.size());
                this.triples.forEach(tripleSet::addKey);
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
            default:
                throw new IllegalArgumentException("Unknown set implementation: " + param1_SetImplementation);
        }
    }

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        this.triples = Releases.current.readTriples(param0_GraphUri);
        this.triplesToRemove = Releases.current.cloneTriples(triples);
        Collections.shuffle(triplesToRemove);
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

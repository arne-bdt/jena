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

import org.apache.jena.atlas.iterator.ActionCount;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.graph.helper.Releases;
import org.apache.jena.mem.set.helper.JMHDefaultOptions;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;


@State(Scope.Benchmark)
public class TestSetIterate {

    @Param({
//            "../testing/cheeses-0.1.ttl",
//            "../testing/pizza.owl.rdf",
            "C:/temp/res_test/xxx_CGMES_EQ.xml",
//            "C:/temp/res_test/xxx_CGMES_SSH.xml",
//            "C:/temp/res_test/xxx_CGMES_TP.xml",
            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml",
            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml",
            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml",
            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml",
            "../testing/BSBM/bsbm-1m.nt.gz",
//            "../testing/BSBM/bsbm-5m.nt.gz",
//            "../testing/BSBM/bsbm-25m.nt.gz",
    })
    public String param0_GraphUri;

    @Param({
            "HashSet",
            "HashCommonTripleSet",
            "FastHashSetOfTriples",
            "FastHashSetOfTriples2"
    })
    public String param1_SetImplementation;

    private List<Triple> triples;
    private HashSet<Triple> hashSet;
    private HashCommonTripleSet hashCommonTripleSet;
    private FastHashSetOfTriples fastHashSetOfTriples;
    private FastHashSetOfTriples2 fastHashSetOfTriples2;

    java.util.function.Supplier<Iterator<Triple>> getIterator;

    @Benchmark
    public Object foreachRemaining() {
        var it = getIterator.get();
        ActionCount<Triple> counter = new ActionCount<>();
        it.forEachRemaining(counter);
        assertEquals(triples.size(), counter.getCount());
        return counter;
    }

    @Benchmark
    public Object hasNextNext() {
        var it = getIterator.get();
        int i= 0;
        while (it.hasNext()) {
            it.next();
            i++;
        }
        assertEquals(triples.size(), i);
        return i;
    }

    private Iterator<Triple> getIteratorFromHashSet() {
        return hashSet.iterator();
    }

    private Iterator<Triple> getIteratorFromHashCommonTripleSet() {
        return hashCommonTripleSet.keyIterator();
    }

    private Iterator<Triple> getIteratorFromFastHashSetOfTriples() {
        return fastHashSetOfTriples.keyIterator();
    }
    private Iterator<Triple> getIteratorFromFastHashSetOfTriples2() {
        return fastHashSetOfTriples2.keyIterator();
    }


    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        this.triples = Releases.current.readTriples(param0_GraphUri);
        switch (param1_SetImplementation) {
            case "HashSet":
                this.hashSet = new HashSet<>(triples.size());
                triples.forEach(hashSet::add);
                this.getIterator = this::getIteratorFromHashSet;
                break;
            case "HashCommonTripleSet":
                this.hashCommonTripleSet = new HashCommonTripleSet(triples.size());
                triples.forEach(hashCommonTripleSet::addUnchecked);
                this.getIterator = this::getIteratorFromHashCommonTripleSet;
                break;
            case "FastHashSetOfTriples":
                this.fastHashSetOfTriples = new FastHashSetOfTriples(triples.size());
                triples.forEach(fastHashSetOfTriples::addUnchecked);
                this.getIterator = this::getIteratorFromFastHashSetOfTriples;
                break;
            case "FastHashSetOfTriples2":
                this.fastHashSetOfTriples2 = new FastHashSetOfTriples2(triples.size());
                triples.forEach(fastHashSetOfTriples2::addUnchecked);
                this.getIterator = this::getIteratorFromFastHashSetOfTriples2;
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
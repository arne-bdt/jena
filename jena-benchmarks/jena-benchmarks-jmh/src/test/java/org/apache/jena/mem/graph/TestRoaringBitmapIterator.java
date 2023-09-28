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

import org.apache.jena.graph.Triple;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.mem.graph.helper.Releases;
import org.apache.jena.mem2.collection.FastHashSet;
import org.apache.jena.mem2.store.roaring.RoaringBitmapTripleIterator;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.roaringbitmap.RoaringBitmap;

import java.util.Iterator;


@State(Scope.Benchmark)
public class TestRoaringBitmapIterator {

    @Param({
            "../testing/cheeses-0.1.ttl",
            "../testing/pizza.owl.rdf",
//            "../testing/BSBM/bsbm-1m.nt.gz",
    })
    public String param0_GraphUri;
    @Param({
            "RoaringBitmapTripleIterator",
    })
    public String param1_IteratorImplementation;

    private FastHashSet<Triple> triples;

    private RoaringBitmap bitmap;
    private Iterator<Triple> sut;

    @Benchmark
    public Triple iterateAllViaHasNextNext() {
        Triple t = null;
        while (sut.hasNext()) {
            t = sut.next();
        }
        return t;
    }

    @Benchmark
    public Triple iterateAllViaForEachRemaining() {
        Triple[] t = new Triple[1];
        sut.forEachRemaining(triple -> t[0] = triple);
        return t[0];
    }


    @Setup(Level.Invocation)
    public void setupInvocation() {
        switch (param1_IteratorImplementation) {
            case "RoaringBitmapTripleIterator":
                sut = new RoaringBitmapTripleIterator(bitmap, triples);
                break;

            default:
                throw new IllegalArgumentException("Unknown iterator implementation: " + param1_IteratorImplementation);
        }
    }

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        final var tripleList = Releases.current.readTriples(param0_GraphUri);
        System.out.println();
        System.out.println("Loaded " + tripleList.size() + " triples from " + param0_GraphUri);
        this.triples = new FastHashSet<Triple>(tripleList.size()) {
            @Override
            protected Triple[] newKeysArray(int size) {
                return new Triple[size];
            }
        };
        this.bitmap = new RoaringBitmap();
        for (var triple : tripleList) {
            bitmap.add(triples.addAndGetIndex(triple));
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

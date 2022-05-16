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

package org.apache.jena.mem2;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.GraphMem;
import org.apache.jena.mem.GraphMemWithArrayListOnly;
import org.apache.jena.mem2.specialized.LowMemoryTripleHashSet;
import org.apache.jena.mem2.specialized.SortedTripleListSet;
import org.apache.jena.mem2.specialized.TripleHashSet;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.Assert;
import org.junit.Test;

import java.io.Console;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.*;
import java.util.function.Supplier;

public class TestGraphMemVariants_LowMemoryHashSet_remove_fails extends TestGraphMemVariantsBase {

    @Test
    public void cheeses_ttl() throws FileNotFoundException {
        loadGraph_randomly_add_all_triples_and_remove_all_triples(
                "./../jena-examples/src/main/resources/data/cheeses-0.1.ttl");
    }

    @Test
    public void broke_delete_ttl() throws FileNotFoundException {
        loadGraph_add_delete(
                "c:/temp/broken_graph.ttl");
    }

    @Test
    public void broke_delete_2_ttl() throws FileNotFoundException {
        loadGraph_add_delete(
                "c:/temp/broken_graph2.ttl");
    }

    protected static List<List<Triple>> selectFistXTriples(final List<List<Triple>> triplesPerGraph, final int numerOfTriplesToSelectPerGraph) {
        var randomlySelectedTriples = new ArrayList<List<Triple>>(triplesPerGraph.size());
        /*find random triples*/
        for (List<Triple> triples : triplesPerGraph) {
            if(numerOfTriplesToSelectPerGraph < 1) {
                randomlySelectedTriples.add(Collections.emptyList());
                continue;
            }
            randomlySelectedTriples.add(triples.subList(0, Math.min(numerOfTriplesToSelectPerGraph, triples.size())));
        }
        return randomlySelectedTriples;
    }

    private void printSet(LowMemoryTripleHashSet set) {
//        for(var i=0; i<set.entries.length; i++) {
//            var e = (Triple) set.entries[i];
//            if(e != null) {
//                System.out.println(String.format("index: %d calculatedIndex: %d value: %s", i, set.calcStartIndexByHashCode(e), e));
//            } else {
//                System.out.println(String.format("index: %d - empty", i));
//            }
//        }
    }

    private void loadGraph_add_delete(String graphUri) throws FileNotFoundException {
        var loadingGraph = new GraphMemWithArrayListOnly();
        RDFDataMgr.read(loadingGraph, graphUri);
        var triples = new ArrayList<>(loadingGraph.triples);

        Collections.sort(triples, Comparator.comparingInt((Triple t) -> t.hashCode()).reversed());

        var set = new LowMemoryTripleHashSet();
        for (var k=0; k<triples.size(); k++) {
            var t = triples.get(k);
            if(!set.add(Triple.create(t.getSubject(), t.getPredicate(), t.getObject()))) {
                Assert.fail();
            }
            //System.out.println(String.format("added triple index: %d value: %s", set.calcStartIndexByHashCode(t), t));
            printSet(set);
        }
        var mid = (int)triples.size() /2;
        //Collections.shuffle(triples);
        for(int k=mid; k<triples.size(); k++) {
            var t = triples.get(k);
            t = Triple.create(t.getSubject(), t.getPredicate(), t.getObject()); /*important to avoid identity equality*/
            var removed = set.remove(t);
            //System.out.println(String.format("removed triple index: %d value: %s", set.calcStartIndexByHashCode(t), t));
            printSet(set);
            if(!removed) {
                Assert.fail();
            }
        }
        for(int k=mid-1; k>=0; k--) {
            var t = triples.get(k);
            t = Triple.create(t.getSubject(), t.getPredicate(), t.getObject()); /*important to avoid identity equality*/
            var removed = set.remove(t);
            //System.out.println(String.format("removed triple index: %d value: %s", set.calcStartIndexByHashCode(t), t));
            printSet(set);
            if(!removed) {
                Assert.fail();
            }
        }
        Assert.assertTrue(set.isEmpty());
    }

    private void loadGraph_randomly_add_all_triples_and_remove_all_triples(String graphUri) throws FileNotFoundException {
        var loadingGraph = new GraphMemWithArrayListOnly();
        RDFDataMgr.read(loadingGraph, graphUri);
        var allTriples = new ArrayList<>(loadingGraph.triples);

        for(int i=0; i<100000; i++) {
            Collections.shuffle(allTriples);
            var triples = allTriples.subList(0, 1);

            var set = new LowMemoryTripleHashSet();
            for (var k=0; k<triples.size(); k++) {
                var t = triples.get(k);
                if(!set.add(Triple.create(t.getSubject(), t.getPredicate(), t.getObject()))) {
                    Assert.fail();
                }
            }
            var mid = (int)triples.size() /2;
            //Collections.shuffle(triples);
            for(int k=mid; k<triples.size(); k++) {
                var t = triples.get(k);
                t = Triple.create(t.getSubject(), t.getPredicate(), t.getObject()); /*important to avoid identity equality*/
                if(!set.remove(t)) {
//                    var g = new GraphMem();
//                    triples.forEach(g::add);
//                    RDFDataMgr.write(new FileOutputStream("c:/temp/broken_graph3.ttl"), g, Lang.TTL);
                    Assert.fail();
                }
            }
            for(int k=mid-1; k>=0; k--) {
                var t = triples.get(k);
                t = Triple.create(t.getSubject(), t.getPredicate(), t.getObject()); /*important to avoid identity equality*/
                if(!set.remove(t)) {
                    Assert.fail();
                }
            }
//            for (var k=0; k<triples.size(); k++) {
//                var t = triples.get(k);
//                if(!set.remove(Triple.create(t.getSubject(), t.getPredicate(), t.getObject()))) {
//                    Assert.fail();
//                }
//            }
            Assert.assertTrue(set.isEmpty());
        }
    }
}

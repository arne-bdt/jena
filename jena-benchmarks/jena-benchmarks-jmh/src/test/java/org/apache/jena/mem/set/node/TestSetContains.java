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

package org.apache.jena.mem.set.node;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem.graph.helper.Releases;
import org.apache.jena.mem.set.helper.JMHDefaultOptions;
import org.apache.jena.mem2.collection.*;
import org.apache.jena.mem2.collection.discarded.FastNodeHashSet2;
import org.apache.jena.mem2.collection.discarded.FastNodeSet2;
import org.apache.jena.mem2.collection.discarded.NodeSet2;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.util.List;


@State(Scope.Benchmark)
public class TestSetContains {

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
            "NodeSet",
//            "NodeSet2",
            "FastNodeSet",
//            "FastNodeSet2",
            "FastNodeHashSet2"
    })
    public String param1_SetImplementation;

    private List<Triple> triplesToFind;
    private NodeSet subjectSet;
    private NodeSet predicateSet;
    private NodeSet objectSet;

    private NodeSet2 subjectSet2;
    private NodeSet2 predicateSet2;
    private NodeSet2 objectSet2;

    private FastNodeSet fastSubjectSet;
    private FastNodeSet fastPredicateSet;
    private FastNodeSet fastObjectSet;

    private FastNodeSet2 fastSubjectSet2;
    private FastNodeSet2 fastPredicateSet2;
    private FastNodeSet2 fastObjectSet2;

    private FastNodeHashSet fastSubjectHashSet;
    private FastNodeHashSet fastPredicateHashSet;
    private FastNodeHashSet fastObjectHashSet;

    private FastNodeHashSet2 fastSubjectHashSet2;
    private FastNodeHashSet2 fastPredicateHashSet2;
    private FastNodeHashSet2 fastObjectHashSet2;

    java.util.function.Supplier<Boolean> setContainsSubjects;
    java.util.function.Supplier<Boolean> setContainsPredicates;
    java.util.function.Supplier<Boolean> setContainsObjects;

    @Benchmark
    public boolean setContainsSubjects() {
        return setContainsSubjects.get();
    }

    @Benchmark
    public boolean setContainsPredicates() {
        return setContainsPredicates.get();
    }
    @Benchmark
    public boolean setContainsObjects() {
        return setContainsObjects.get();
    }

    private boolean tripleSetContainsSubjects() {
        var found = false;
        for(var t: triplesToFind) {
            found = subjectSet.contains(t.getSubject());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean tripleSetContainsPredicates() {
        var found = false;
        for(var t: triplesToFind) {
            found = predicateSet.contains(t.getPredicate());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean tripleSetContainsObjects() {
        var found = false;
        for(var t: triplesToFind) {
            found = objectSet.contains(t.getObject());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean tripleSet2ContainsSubjects() {
        var found = false;
        for(var t: triplesToFind) {
            found = subjectSet2.contains(t.getSubject());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean tripleSet2ContainsPredicates() {
        var found = false;
        for(var t: triplesToFind) {
            found = predicateSet2.contains(t.getPredicate());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean tripleSet2ContainsObjects() {
        var found = false;
        for(var t: triplesToFind) {
            found = objectSet2.contains(t.getObject());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean fastTripleSetContainsSubjects() {
        var found = false;
        for(var t: triplesToFind) {
            found = fastSubjectSet.contains(t.getSubject());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean fastTripleSetContainsPredicates() {
        var found = false;
        for(var t: triplesToFind) {
            found = fastPredicateSet.contains(t.getPredicate());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean fastTripleSetContainsObjects() {
        var found = false;
        for(var t: triplesToFind) {
            found = fastObjectSet.contains(t.getObject());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean fastTripleSet2ContainsSubjects() {
        var found = false;
        for(var t: triplesToFind) {
            found = fastSubjectSet2.contains(t.getSubject());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean fastTripleSet2ContainsPredicates() {
        var found = false;
        for(var t: triplesToFind) {
            found = fastPredicateSet2.contains(t.getPredicate());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean fastTripleSet2ContainsObjects() {
        var found = false;
        for(var t: triplesToFind) {
            found = fastObjectSet2.contains(t.getObject());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean fastTripleHashSetContainsSubjects() {
        var found = false;
        for(var t: triplesToFind) {
            found = fastSubjectHashSet.contains(t.getSubject());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean fastTripleHashSetContainsPredicates() {
        var found = false;
        for(var t: triplesToFind) {
            found = fastPredicateHashSet.contains(t.getPredicate());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean fastTripleHashSetContainsObjects() {
        var found = false;
        for(var t: triplesToFind) {
            found = fastObjectHashSet.contains(t.getObject());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean fastTripleHashSet2ContainsSubjects() {
        var found = false;
        for(var t: triplesToFind) {
            found = fastSubjectHashSet2.contains(t.getSubject());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean fastTripleHashSet2ContainsPredicates() {
        var found = false;
        for(var t: triplesToFind) {
            found = fastPredicateHashSet2.contains(t.getPredicate());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean fastTripleHashSet2ContainsObjects() {
        var found = false;
        for(var t: triplesToFind) {
            found = fastObjectHashSet2.contains(t.getObject());
            Assert.assertTrue(found);
        }
        return found;
    }


    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        var triples = Releases.current.readTriples(param0_GraphUri);
        this.triplesToFind = Releases.current.cloneTriples(triples);
        switch (param1_SetImplementation) {
            case "NodeSet":
                this.subjectSet = new NodeSet();
                this.predicateSet = new NodeSet();
                this.objectSet = new NodeSet();
                triples.forEach(t -> {
                    subjectSet.tryPut(t.getSubject());
                    predicateSet.tryPut(t.getPredicate());
                    objectSet.tryPut(t.getObject());
                });
                this.setContainsSubjects = this::tripleSetContainsSubjects;
                this.setContainsPredicates = this::tripleSetContainsPredicates;
                this.setContainsObjects = this::tripleSetContainsObjects;
                break;
            case "NodeSet2":
                this.subjectSet2 = new NodeSet2();
                this.predicateSet2 = new NodeSet2();
                this.objectSet2 = new NodeSet2();
                triples.forEach(t -> {
                    subjectSet2.addKey(t.getSubject());
                    predicateSet2.addKey(t.getPredicate());
                    objectSet2.addKey(t.getObject());
                });
                this.setContainsSubjects = this::tripleSet2ContainsSubjects;
                this.setContainsPredicates = this::tripleSet2ContainsPredicates;
                this.setContainsObjects = this::tripleSet2ContainsObjects;
                break;
            case "FastNodeSet":
                this.fastSubjectSet = new FastNodeSet();
                this.fastPredicateSet = new FastNodeSet();
                this.fastObjectSet = new FastNodeSet();
                triples.forEach(t -> {
                    fastSubjectSet.addKey(t.getSubject());
                    fastPredicateSet.addKey(t.getPredicate());
                    fastObjectSet.addKey(t.getObject());
                });
                this.setContainsSubjects = this::fastTripleSetContainsSubjects;
                this.setContainsPredicates = this::fastTripleSetContainsPredicates;
                this.setContainsObjects = this::fastTripleSetContainsObjects;
                break;
            case "FastNodeSet2":
                this.fastSubjectSet2 = new FastNodeSet2();
                this.fastPredicateSet2 = new FastNodeSet2();
                this.fastObjectSet2 = new FastNodeSet2();
                triples.forEach(t -> {
                    fastSubjectSet2.addKey(t.getSubject());
                    fastPredicateSet2.addKey(t.getPredicate());
                    fastObjectSet2.addKey(t.getObject());
                });
                this.setContainsSubjects = this::fastTripleSet2ContainsSubjects;
                this.setContainsPredicates = this::fastTripleSet2ContainsPredicates;
                this.setContainsObjects = this::fastTripleSet2ContainsObjects;
                break;
            case "FastNodeHashSet":
                this.fastSubjectHashSet = new FastNodeHashSet();
                this.fastPredicateHashSet = new FastNodeHashSet();
                this.fastObjectHashSet = new FastNodeHashSet();
                triples.forEach(t -> {
                    fastSubjectHashSet.add(t.getSubject());
                    fastPredicateHashSet.add(t.getPredicate());
                    fastObjectHashSet.add(t.getObject());
                });
                this.setContainsSubjects = this::fastTripleHashSetContainsSubjects;
                this.setContainsPredicates = this::fastTripleHashSetContainsPredicates;
                this.setContainsObjects = this::fastTripleHashSetContainsObjects;
                break;
            case "FastNodeHashSet2":
                this.fastSubjectHashSet2 = new FastNodeHashSet2();
                this.fastPredicateHashSet2 = new FastNodeHashSet2();
                this.fastObjectHashSet2 = new FastNodeHashSet2();
                triples.forEach(t -> {
                    fastSubjectHashSet2.add(t.getSubject());
                    fastPredicateHashSet2.add(t.getPredicate());
                    fastObjectHashSet2.add(t.getObject());
                });
                this.setContainsSubjects = this::fastTripleHashSet2ContainsSubjects;
                this.setContainsPredicates = this::fastTripleHashSet2ContainsPredicates;
                this.setContainsObjects = this::fastTripleHashSet2ContainsObjects;
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

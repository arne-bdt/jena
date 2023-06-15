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

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.graph.helper.Releases;
import org.apache.jena.mem.set.helper.JMHDefaultOptions;
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
//            "C:/temp/res_test/xxx_CGMES_SSH.xml",
//            "C:/temp/res_test/xxx_CGMES_TP.xml",
            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml",
//            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml",
//            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml",
//            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml",
//            "../testing/BSBM/bsbm-1m.nt.gz",
//            "../testing/BSBM/bsbm-5m.nt.gz",
//            "../testing/BSBM/bsbm-25m.nt.gz",
    })
    public String param0_GraphUri;

    @Param({
            "HashSet",
            "FastHashSetOfNodes",
            "FastHashSetOfNodes2",
            "HashCommonNodeSet"
    })
    public String param1_SetImplementation;
    java.util.function.Supplier<Boolean> setContainsSubjects;
    java.util.function.Supplier<Boolean> setContainsPredicates;
    java.util.function.Supplier<Boolean> setContainsObjects;
    private List<Triple> triplesToFind;
    private HashSet<Node> subjectHashSet;
    private HashSet<Node> predicateHashSet;
    private HashSet<Node> objectHashSet;
    private HashCommonNodeSet subjectHashCommonNodeSet;
    private HashCommonNodeSet predicateHashCommonNodeSet;
    private HashCommonNodeSet objectHashCommonNodeSet;
    private FastHashSetOfNodes subjectFastHashSetOfNodes;
    private FastHashSetOfNodes predicateFastHashSetOfNodes;
    private FastHashSetOfNodes objectFastHashSetOfNodes;
    private FastHashSetOfNodes2 subjectFastHashSetOfNodes2;
    private FastHashSetOfNodes2 predicateFastHashSetOfNodes2;
    private FastHashSetOfNodes2 objectFastHashSetOfNodes2;

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

    private boolean hashSetContainsSubjects() {
        var found = false;
        for (var t : triplesToFind) {
            found = subjectHashSet.contains(t.getSubject());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean hashSetContainsPredicates() {
        var found = false;
        for (var t : triplesToFind) {
            found = predicateHashSet.contains(t.getPredicate());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean hashSetSetContainsObjects() {
        var found = false;
        for (var t : triplesToFind) {
            found = objectHashSet.contains(t.getObject());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean FastHashSetOfNodesContainsSubjects() {
        var found = false;
        for (var t : triplesToFind) {
            found = subjectFastHashSetOfNodes.containsKey(t.getSubject());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean FastHashSetOfNodesContainsPredicates() {
        var found = false;
        for (var t : triplesToFind) {
            found = predicateFastHashSetOfNodes.containsKey(t.getPredicate());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean FastHashSetOfNodesContainsObjects() {
        var found = false;
        for (var t : triplesToFind) {
            found = objectFastHashSetOfNodes.containsKey(t.getObject());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean FastHashSetOfNodes2ContainsSubjects() {
        var found = false;
        for (var t : triplesToFind) {
            found = subjectFastHashSetOfNodes2.containsKey(t.getSubject());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean FastHashSetOfNodes2ContainsPredicates() {
        var found = false;
        for (var t : triplesToFind) {
            found = predicateFastHashSetOfNodes2.containsKey(t.getPredicate());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean FastHashSetOfNodes2ContainsObjects() {
        var found = false;
        for (var t : triplesToFind) {
            found = objectFastHashSetOfNodes2.containsKey(t.getObject());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean HashCommonNodeSetContainsSubjects() {
        var found = false;
        for (var t : triplesToFind) {
            found = subjectHashCommonNodeSet.containsKey(t.getSubject());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean HashCommonNodeSetContainsPredicates() {
        var found = false;
        for (var t : triplesToFind) {
            found = predicateHashCommonNodeSet.containsKey(t.getPredicate());
            Assert.assertTrue(found);
        }
        return found;
    }

    private boolean HashCommonNodeSetContainsObjects() {
        var found = false;
        for (var t : triplesToFind) {
            found = objectHashCommonNodeSet.containsKey(t.getObject());
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
                this.subjectHashSet = new HashSet<>();
                this.predicateHashSet = new HashSet<>();
                this.objectHashSet = new HashSet<>();
                triples.forEach(t -> {
                    subjectHashSet.add(t.getSubject());
                    predicateHashSet.add(t.getPredicate());
                    objectHashSet.add(t.getObject());
                });
                this.setContainsSubjects = this::hashSetContainsSubjects;
                this.setContainsPredicates = this::hashSetContainsPredicates;
                this.setContainsObjects = this::hashSetSetContainsObjects;
                break;
            case "FastHashSetOfNodes":
                this.subjectFastHashSetOfNodes = new FastHashSetOfNodes();
                this.predicateFastHashSetOfNodes = new FastHashSetOfNodes();
                this.objectFastHashSetOfNodes = new FastHashSetOfNodes();
                triples.forEach(t -> {
                    subjectFastHashSetOfNodes.tryAdd(t.getSubject());
                    predicateFastHashSetOfNodes.tryAdd(t.getPredicate());
                    objectFastHashSetOfNodes.tryAdd(t.getObject());
                });
                this.setContainsSubjects = this::FastHashSetOfNodesContainsSubjects;
                this.setContainsPredicates = this::FastHashSetOfNodesContainsPredicates;
                this.setContainsObjects = this::FastHashSetOfNodesContainsObjects;
                break;
            case "FastHashSetOfNodes2":
                this.subjectFastHashSetOfNodes2 = new FastHashSetOfNodes2();
                this.predicateFastHashSetOfNodes2 = new FastHashSetOfNodes2();
                this.objectFastHashSetOfNodes2 = new FastHashSetOfNodes2();
                triples.forEach(t -> {
                    subjectFastHashSetOfNodes2.tryAdd(t.getSubject());
                    predicateFastHashSetOfNodes2.tryAdd(t.getPredicate());
                    objectFastHashSetOfNodes2.tryAdd(t.getObject());
                });
                this.setContainsSubjects = this::FastHashSetOfNodes2ContainsSubjects;
                this.setContainsPredicates = this::FastHashSetOfNodes2ContainsPredicates;
                this.setContainsObjects = this::FastHashSetOfNodes2ContainsObjects;
                break;
            case "HashCommonNodeSet":
                this.subjectHashCommonNodeSet = new HashCommonNodeSet();
                this.predicateHashCommonNodeSet = new HashCommonNodeSet();
                this.objectHashCommonNodeSet = new HashCommonNodeSet();
                triples.forEach(t -> {
                    subjectHashCommonNodeSet.tryAdd(t.getSubject());
                    predicateHashCommonNodeSet.tryAdd(t.getPredicate());
                    objectHashCommonNodeSet.tryAdd(t.getObject());
                });
                this.setContainsSubjects = this::HashCommonNodeSetContainsSubjects;
                this.setContainsPredicates = this::HashCommonNodeSetContainsPredicates;
                this.setContainsObjects = this::HashCommonNodeSetContainsObjects;
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

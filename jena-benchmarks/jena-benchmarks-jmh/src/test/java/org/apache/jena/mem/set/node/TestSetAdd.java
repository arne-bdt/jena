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
import org.apache.jena.mem2.collection.FastNodeHashSet;
import org.apache.jena.mem2.collection.FastNodeSet;
import org.apache.jena.mem2.collection.FastTripleSet;
import org.apache.jena.mem2.collection.NodeSet;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.util.List;


@State(Scope.Benchmark)
public class TestSetAdd {

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
            "FastNodeSet",
            "FastNodeHashSet"
    })
    public String param1_SetImplementation;


    private List<Triple> triples;

    java.util.function.Function<Triple.Field, Object> addToSet;

    @Benchmark
    public Object addSubjectsToSet() {
        return addToSet.apply(Triple.Field.fieldSubject);
    }

    @Benchmark
    public Object addPredicatesToSet() {
        return addToSet.apply(Triple.Field.fieldPredicate);
    }

    @Benchmark
    public Object addObjectsToSet() {
        return addToSet.apply(Triple.Field.fieldObject);
    }

    private Object addToNodeSet(Triple.Field field) {
        var sut = new NodeSet();
        triples.forEach(t -> sut.addKey(field.getField(t)));
        return sut;
    }

    private Object addToFastNodeSet(Triple.Field field) {
        var sut = new FastNodeSet();
        triples.forEach(t -> sut.addKey(field.getField(t)));
        return sut;
    }

    private Object addToFastNodeHashSet(Triple.Field field) {
        var sut = new FastNodeHashSet();
        triples.forEach(t -> sut.add(field.getField(t)));
        return sut;
    }

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        triples = Releases.current.readTriples(param0_GraphUri);
        switch (param1_SetImplementation) {
            case "NodeSet":
                this.addToSet = this::addToNodeSet;
                break;
            case "FastNodeSet":
                this.addToSet = this::addToFastNodeSet;
                break;
            case "FastNodeHashSet":
                this.addToSet = this::addToFastNodeHashSet;
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

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
import org.apache.jena.mem.graph.helper.Context;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.mem.graph.helper.Releases;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.Transactional;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.util.List;


@State(Scope.Benchmark)
public class TestGraphAdd {

    @Param({
//            "../testing/cheeses-0.1.ttl",
//            "../testing/pizza.owl.rdf",
            "C:/temp/res_test/xxx_CGMES_EQ.xml",
            "C:/temp/res_test/xxx_CGMES_SSH.xml",
            "C:/temp/res_test/xxx_CGMES_TP.xml",
            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml",
            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml",
            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml",
            "C:/rd/CGMES/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml",
            "../testing/BSBM/bsbm-1m.nt.gz",
            "../testing/BSBM/bsbm-5m.nt.gz",
//            "../testing/BSBM/bsbm-25m.nt.gz",
    })
    public String param0_GraphUri;

    @Param({
//            "GraphMem (current)",
//            "GraphMem2Fast (current)",
//            "GraphMem2Legacy (current)",
//            "GraphMem2Roaring (current)",
            "GraphWrapperTransactional (current)",
            "GraphTxn (current)",
    })
    public String param1_GraphImplementation;
    java.util.function.Supplier<Object> graphAdd;
    private Context trialContext;
    private List<Triple> triplesCurrent;
    private List<org.apache.shadedJena530.graph.Triple> triples530;

    @Benchmark
    public Object graphAdd() {
        return graphAdd.get();
    }

    private Object graphAddCurrent() {
        var sutCurrent = Releases.current.createGraph(trialContext.getGraphClass());
        if (sutCurrent instanceof Transactional transactional) {
            transactional.begin(TxnType.WRITE);
            triplesCurrent.forEach(sutCurrent::add);
            transactional.commit();
            transactional.begin(TxnType.READ);
            Assert.assertEquals(triplesCurrent.size(), sutCurrent.size());
            transactional.end();
        } else {
            triplesCurrent.forEach(sutCurrent::add);
            Assert.assertEquals(triplesCurrent.size(), sutCurrent.size());
        }
        return sutCurrent;
    }

    private Object graphAdd530() {
        var sut530 = Releases.v530.createGraph(trialContext.getGraphClass());
        triples530.forEach(sut530::add);
        Assert.assertEquals(triples530.size(), sut530.size());
        return sut530;
    }


    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        this.trialContext = new Context(param1_GraphImplementation);
        switch (this.trialContext.getJenaVersion()) {
            case CURRENT:
                triplesCurrent = Releases.current.readTriples(param0_GraphUri);
                this.graphAdd = this::graphAddCurrent;
                break;
            case JENA_5_3_0:
                triples530 = Releases.v530.readTriples(param0_GraphUri);
                this.graphAdd = this::graphAdd530;
                break;
            default:
                throw new IllegalArgumentException("Unknown Jena version: " + this.trialContext.getJenaVersion());
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

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

package org.apache.jena.mem;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.GraphMem2;
import org.apache.jena.mem2.GraphMemWithAdaptiveTripleStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.jena.mem.jmh.AbstractTestGraphBaseWithFilledGraph.cloneNode;
import static org.junit.Assert.assertEquals;

@Ignore
public class TestLoadGraph {

    List<Triple> triples;
    List<Triple> clones;

    Graph sut0;
    Graph sut1;

    @Before
    public void setUp() throws Exception {
        this.triples = TripleReaderReadingCGMES_2_4_15_WithTypedLiterals.read("C:/temp/res_test/xxx_CGMES_SSH.xml");
        this.clones = new ArrayList<>(triples.size());
        sut0 = new GraphMem2();
        sut1 = new GraphMemWithAdaptiveTripleStore();
        triples.forEach(t -> {
            clones.add(Triple.create(cloneNode(t.getSubject()), cloneNode(t.getPredicate()), cloneNode(t.getObject())));
            sut0.add(t);
            sut1.add(t);
        });
    }

    @Test
    public void testContains() {
        for(int i=0; i<100; i++) {
            for (var t : clones) {
                Assert.assertTrue(sut0.contains(t));
                Assert.assertTrue(sut1.contains(t));
            }
        }
    }

    @Test
    public void testFindAll() {
        for(var i=0; i<100; i++) {
            findAll(sut0);
            findAll(sut1);
        }
    }

    private static void findAll(Graph sut) {
        var found = new ArrayList<Triple>(sut.size());
        var it = sut.find();
        while(it.hasNext()) {
            found.add(it.next());
        }
        it.close();
        assertEquals(sut.size(), found.size());
    }

}

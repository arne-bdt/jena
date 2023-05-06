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
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.GraphMem2;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.jena.mem.jmh.AbstractTestGraphBaseWithFilledGraph.cloneNode;
import static org.junit.Assert.*;

@Ignore
public class TestLoadFindBySamples {

    List<Triple> triples;
    List<Triple> samples;

    final int samplesSize = 0;

    Graph sut0;
    Graph sut1;

    @Before
    public void setUp() throws Exception {
        this.triples = TripleReaderReadingCGMES_2_4_15_WithTypedLiterals.read("C:/temp/res_test/xxx_CGMES_EQ.xml");
        sut0 = new GraphMem();
        sut1 = new GraphMem2();
        triples.forEach(t -> {
            sut0.add(Triple.create(cloneNode(t.getSubject()), cloneNode(t.getPredicate()), cloneNode(t.getObject())));
            sut1.add(Triple.create(cloneNode(t.getSubject()), cloneNode(t.getPredicate()), cloneNode(t.getObject())));
        });

        if(samplesSize < 1) {
            this.samples = new ArrayList<>(triples.size());
            this.triples.forEach(t -> {
                this.samples.add(Triple.create(cloneNode(t.getSubject()), cloneNode(t.getPredicate()), cloneNode(t.getObject())));
            });
        } else {
            this.samples = new ArrayList<>(samplesSize);
            var sampleIncrement = triples.size() / samplesSize;
            Triple t;
            for(var i=0; i< triples.size(); i+=sampleIncrement) {
                t = triples.get(i);
                this.samples.add(Triple.create(cloneNode(t.getSubject()), cloneNode(t.getPredicate()), cloneNode(t.getObject())));
            }
        }
    }

    @Test
    public void graphFindBySamples_ANY_Predicate_ANY() {
        for(int i=0; i<20; i++) {
            for (Triple sample : samples) {
                graphFindBySamples_sut0(Node.ANY, sample.getPredicate(), Node.ANY);
                graphFindBySamples_sut1(Node.ANY, sample.getPredicate(), Node.ANY);
            }
        }
    }

    @Test
    public void graphFindBySamples_Subject_Predicate_ANY() {
        for(int i=0; i<100; i++) {
            for (Triple sample : samples) {
                graphFindBySamples_sut0(sample.getSubject(), sample.getPredicate(), Node.ANY);
                graphFindBySamples_sut1(sample.getSubject(), sample.getPredicate(), Node.ANY);
            }
        }
    }

    @Test
    public void graphFindBySamples_Subject_ANY_Object() {
        for(int i=0; i<10; i++) {
            for (Triple sample : samples) {
                graphFindBySamples_sut0(sample.getSubject(), Node.ANY, sample.getObject());
                graphFindBySamples_sut1(sample.getSubject(), Node.ANY, sample.getObject());
            }
        }
    }

    private void graphFindBySamples_sut0(Node sm, Node pm, Node om) {
        var it = sut0.find(sm, pm, om);
        assertTrue(it.hasNext());
        assertNotNull(it.next());
    }

    private void graphFindBySamples_sut1(Node sm, Node pm, Node om) {
        var it = sut1.find(sm, pm, om);
        assertTrue(it.hasNext());
        assertNotNull(it.next());
    }

    @Test
    public void graphContains() {
        var found = false;
        for(int i=0; i<100; i++) {
            for (var t : triples) {
                found = sut0Contains(t);
                Assert.assertTrue(found);
            }
            Assert.assertTrue(found);
        }
        for(int i=0; i<100; i++) {
            for (var t : triples) {
                found = sut1Contains(t);
                Assert.assertTrue(found);
            }
            Assert.assertTrue(found);
        }
    }

    private boolean sut0Contains(final Triple triple) {
        return sut0.contains(triple);
    }

    private boolean sut1Contains(final Triple triple) {
        return sut1.contains(triple);
    }

    @Test
    public void graphFind() {
        var counter = 0;
        var it0 = sut0.find();
        var it1 = sut1.find();
        Triple t0;
        Triple t1;
        while(it0.hasNext() && it1.hasNext()) {
            t0 = it0.next();
            Assert.assertNotNull(t0);
            t1 = it1.next();
            Assert.assertNotNull(t1);
            counter++;
        }
        it0.close();
        it1.close();
        assertEquals(sut0.size(), counter);
        assertEquals(sut1.size(), counter);
    }
}

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

import org.apache.jena.graph.Triple;
import org.apache.jena.mem.GraphMem;
import org.apache.jena.mem.GraphMemWithArrayListOnly;
import org.apache.jena.mem2.specialized.LowMemoryTripleHashSet;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class TestGraphMemVariants_Profile_Memory extends TestGraphMemVariantsBase {

    @Test
    public void ENTSO_E_Test_Configurations_v3_0_RealGrid_EQ() throws InterruptedException {
        load_and_wait(
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml");
    }




    private void load_and_wait(String graphUri) throws InterruptedException {
        var loadingGraph = new GraphMemWithArrayListOnly();
        RDFDataMgr.read(loadingGraph, graphUri);
        var allTriples = new ArrayList<>(loadingGraph.triples);

        var sut = new GraphMem2();
        allTriples.forEach(sut::add);

        var sut2 = new GraphMem();
        allTriples.forEach(sut2::add);

        System.out.println("waiting...");
        Thread.sleep(60000);
    }
}

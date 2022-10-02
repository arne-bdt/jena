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

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.GraphMem;
import org.apache.jena.mem.GraphMemWithArrayListOnly;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.Test;

import java.util.*;
import java.util.function.Function;

public class TestGraphMemVariants_Profile_Memory extends TestGraphMemVariantsBase {

    @Test
    public void ENTSO_E_Test_Configurations_v3_0_RealGrid_EQ() throws InterruptedException {
        hashCollisions(
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml");
    }

    private void hashCollisions(String graphUri) {
        var loadingGraph = new GraphMemWithArrayListOnly();
        RDFDataMgr.read(loadingGraph, graphUri);
        var triples = loadingGraph.triples;
        var subjectsByHashCode = getMapWithNodes(triples, Triple::getSubject, 524288);
        var predicatesByHashCode = getMapWithNodes(triples, Triple::getPredicate, 512);
        var objectsByHashCode = getMapWithNodes(triples, Triple::getObject, 1048576);
        var subjectCollisions = new CollisionSummary(subjectsByHashCode);
        var predicateCollisions = new CollisionSummary(predicatesByHashCode);
        var objectCollisions = new CollisionSummary(objectsByHashCode);
        int i=0;
    }

    private static class CollisionSummary {
        public int hashes;
        public int nodes;
        public int collisions;
        public final double collisionsPerNodePerCent;

        public CollisionSummary(List<HashCodeAndNodes> hashesAndNodes) {
            for (HashCodeAndNodes hashAndNodes : hashesAndNodes) {
                hashes++;
                nodes += hashAndNodes.nodes.size();
                if(hashAndNodes.nodes.size() > 1) {
                    collisions += hashAndNodes.nodes.size() - 1;
                }
            }
            collisions = nodes - hashes;
            collisionsPerNodePerCent = collisions != 0 ? (double)collisions/(double)nodes * 100d : 0;
        }
    }

    private List<HashCodeAndNodes> getMapWithNodes(List<Triple> triples, Function<Triple, Node> nodeResolver, int size) {
        var nodesByIndexingHash = new HashMap<Integer, Set<Node>>();
        triples.forEach(t -> {
            var node = nodeResolver.apply(t);
            var hashCode = node.getIndexingValue().hashCode();
            if(node.isLiteral()) {
                hashCode = (hashCode ^ ((hashCode*31) >>> 16) & (size - 1));
            } else {
                hashCode = (hashCode ^ (hashCode >>> 16) & (size - 1));
            }


            //hashCode = hashCode & (size-1);
            nodesByIndexingHash.compute(hashCode,
                    (hc, set) -> {
                        if(set == null) {
                            set = new HashSet<>();
                        }
                        set.add(node);
                        return set;
                    });
        });
        var hashCodesAndNodes = new ArrayList<HashCodeAndNodes>(nodesByIndexingHash.size());
        nodesByIndexingHash.entrySet()
                .forEach(entrySet -> {
                    hashCodesAndNodes.add(new HashCodeAndNodes(entrySet.getKey(), entrySet.getValue()));
        });
        hashCodesAndNodes.sort(HashCodeAndNodes.mostNodesPerHashCodeComparator);
        return hashCodesAndNodes;
    }

    private static class HashCodeAndNodes {
        public final int hashCode;
        public final Set<Node> nodes;

        public HashCodeAndNodes(int hashCode, Set<Node> nodes) {
            this.hashCode = hashCode;
            this.nodes = nodes;
        }
        public static Comparator<HashCodeAndNodes> mostNodesPerHashCodeComparator
                = Comparator.comparingInt((HashCodeAndNodes o) -> o.nodes.size()).reversed();
    }



    private void load_and_wait(String graphUri) throws InterruptedException {
        var loadingGraph = new GraphMemWithArrayListOnly();
        RDFDataMgr.read(loadingGraph, graphUri);
        var allTriples = new ArrayList<>(loadingGraph.triples);

        var sut = new GraphMem2LowMemory();
        allTriples.forEach(sut::add);

        var sut2 = new GraphMem();
        allTriples.forEach(sut2::add);

        System.out.println("waiting...");
        Thread.sleep(60000);
    }
}

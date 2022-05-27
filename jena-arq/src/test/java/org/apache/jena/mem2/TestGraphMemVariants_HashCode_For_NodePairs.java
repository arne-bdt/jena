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

import org.apache.jena.ext.com.google.common.hash.Hashing;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.GraphMemWithArrayListOnly;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.Test;

import java.util.*;
import java.util.function.Function;

public class TestGraphMemVariants_HashCode_For_NodePairs extends TestGraphMemVariantsBase {

    @Test
    public void ENTSO_E_Test_Configurations_v3_0_RealGrid_EQ() throws InterruptedException {
        hashCollisions(
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml");
    }


    private void hashCollisions(String graphUri) {
        var loadingGraph = new GraphMemWithArrayListOnly();
        RDFDataMgr.read(loadingGraph, graphUri);
        var triples = loadingGraph.triples;
        var byHashCodeOP = getMapWithNodes(triples, Triple::getObject, Triple::getPredicate);
        var byHashCodePO = getMapWithNodes(triples, Triple::getPredicate, Triple::getObject);
        var byHashCodeSO = getMapWithNodes(triples, Triple::getSubject, Triple::getObject);
        var byHashCodeOS = getMapWithNodes(triples, Triple::getObject, Triple::getSubject);
        var byHashCodeSP = getMapWithNodes(triples, Triple::getSubject, Triple::getPredicate);
        var byHashCodePS = getMapWithNodes(triples, Triple::getPredicate, Triple::getSubject);
        var collisionsOP = new CollisionSummary(byHashCodeOP);
        var collisionsPO = new CollisionSummary(byHashCodePO);
        var collisionsSO = new CollisionSummary(byHashCodeSO);
        var collisionsOS = new CollisionSummary(byHashCodeOS);
        var collisionsSP = new CollisionSummary(byHashCodeSP);
        var collisionsPS = new CollisionSummary(byHashCodePS);
        int i=0;
    }

    private static class CollisionSummary {
        public int hashes;
        public int nodes;
        public int collisions;
        public final double collisionsPerNodePerCent;

        public CollisionSummary(List<HashCodeAndNodePairs> hashesAndNodes) {
            for (HashCodeAndNodePairs hashAndNodes : hashesAndNodes) {
                hashes++;
                nodes += hashAndNodes.nodePairs.size();
                if(hashAndNodes.nodePairs.size() > 1) {
                    collisions += hashAndNodes.nodePairs.size() - 1;
                }
            }
            collisions = nodes - hashes;
            collisionsPerNodePerCent = collisions != 0 ? (double)collisions/(double)nodes * 100d : 0;
        }
    }

    private List<HashCodeAndNodePairs> getMapWithNodes(List<Triple> triples, Function<Triple, Node> nodeOneResolver, Function<Triple, Node> nodeTwoResolver) {
        var nodesByIndexingHash = new HashMap<Integer, Set<NodePair>>();
        triples.forEach(t -> {
            var pair = new NodePair(nodeOneResolver.apply(t), nodeTwoResolver.apply(t));
            nodesByIndexingHash.compute(pair.hashCode(),
                    (hc, set) -> {
                        if(set == null) {
                            set = new HashSet<>();
                        }
                        set.add(pair);
                        return set;
                    });
        });
        var hashCodesAndNodes = new ArrayList<HashCodeAndNodePairs>(nodesByIndexingHash.size());
        nodesByIndexingHash.entrySet()
                .forEach(entrySet -> {
                    hashCodesAndNodes.add(new HashCodeAndNodePairs(entrySet.getKey(), entrySet.getValue()));
        });
        hashCodesAndNodes.sort(HashCodeAndNodePairs.mostNodesPerHashCodeComparator);
        return hashCodesAndNodes;
    }

    private static class HashCodeAndNodePairs {
        public final int hashCode;
        public final Set<NodePair> nodePairs;

        public HashCodeAndNodePairs(int hashCode, Set<NodePair> nodePairs) {
            this.hashCode = hashCode;
            this.nodePairs = nodePairs;
        }
        public static Comparator<HashCodeAndNodePairs> mostNodesPerHashCodeComparator
                = Comparator.comparingInt((HashCodeAndNodePairs o) -> o.nodePairs.size()).reversed();
    }

    private static class NodePair {
        public final Node one;
        public final Node two;

        public NodePair(Node one, Node two) {
            this.one = one;
            this.two = two;
        }

        @Override
        public boolean equals(Object o) {
            var other = (NodePair) o;
            return this.one.matches(other.one) && this.two.matches(other.two);
        }

        @Override
        public int hashCode() {
            return (31 * one.getIndexingValue().hashCode()) + two.getIndexingValue().hashCode();
        }
    }
}

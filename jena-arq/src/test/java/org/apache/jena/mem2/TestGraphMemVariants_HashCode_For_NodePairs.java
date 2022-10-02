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
import org.apache.jena.rdf.model.InfModel;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.rulesys.RDFSRuleReasonerFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.ReasonerVocabulary;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.function.Function;

public class TestGraphMemVariants_HashCode_For_NodePairs extends TestGraphMemVariantsBase {

    @Test
    public void ENTSO_E_Test_Configurations_v3_0_RealGrid() throws InterruptedException {
        hashCollisions(
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_EQ.xml",
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_TP.xml",
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml",
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SV.xml");
    }

    @Test
    public void ENTSO_E_Test_Configurations_v3_0_RealGrid_SSH() throws InterruptedException {
        hashCollisions(
                "./../jena-examples/src/main/resources/data/ENTSO-E_Test_Configurations_v3.0/RealGrid/RealGrid_SSH.xml");
    }

    @Test
    public void BSBM_2500() {
        hashCollisions(
                "./../jena-examples/src/main/resources/data/BSBM_2500.ttl");
    }

    @Test
    public void xxx_CGMES() throws InterruptedException {
        hashCollisions(
                "C:/temp/res_test/xxx_CGMES_EQ.xml",
                "C:/temp/res_test/xxx_CGMES_TP.xml",
                "C:/temp/res_test/xxx_CGMES_SSH.xml");
    }

    /*"C:\Users\bern\Downloads\ENTSOE_CGMES_v2.4.15_04Jul2016_RDFS\EquipmentProfileCoreRDFSAugmented-v2_4_15-4Jul2016.rdf"*/

    @Test
    public void cheeses_ttl() throws FileNotFoundException {
        hashCollisions(
                "./../jena-examples/src/main/resources/data/cheeses-0.1.ttl");
    }

    @Test
    public void pizza_owl_rdf() {
        hashCollisions(
                "./../jena-examples/src/main/resources/data/pizza.owl.rdf");
    }

    private void hashCollisions(String... graphUris) {
        ArrayList<Triple> triples = new ArrayList<>();
        for (String uri : graphUris) {
            var loadingGraph = new GraphMemWithArrayListOnly();

            RDFDataMgr.read(loadingGraph, uri);
            triples.addAll(loadingGraph.triples);
        }
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
        System.out.println(String.format("OP: %2.6f - PO: %2.6f - SO: %2.6f - OS:  %2.6f - SP: %2.6f - PS: %2.6f",
                collisionsOP.collisionsPerNodePerCent,
                collisionsPO.collisionsPerNodePerCent,
                collisionsSO.collisionsPerNodePerCent,
                collisionsOS.collisionsPerNodePerCent,
                collisionsSP.collisionsPerNodePerCent,
                collisionsPS.collisionsPerNodePerCent));
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
            var hash = 17 * 37 +one.hashCode();
            return hash * 37 + two.hashCode();

            //return (31 * one.hashCode()) ^ two.hashCode();
            //return (31 * one.hashCode()) + two.hashCode();
            //return (127 * one.hashCode()) ^ two.hashCode();
            //return (one.hashCode() >> 1) ^ two.hashCode();
        }
    }
}

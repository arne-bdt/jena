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

package org.apache.jena.cimxml;

import org.apache.jena.cimxml.schema.SchemaRegistry;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.jmh.helper.TestFileInventory;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.algebra.walker.Walker;
import org.apache.jena.sparql.core.*;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.jena.sparql.path.*;
import org.apache.jena.vocabulary.RDF;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.ZipFile;

@State(Scope.Benchmark)
public class TestParseCIMXML {

    final static Node IRI_FOR_PROFILE_EQ = NodeFactory.createURI("http://entsoe.eu/CIM/EQ");
    final static Node IRI_FOR_PROFILE_SSH = NodeFactory.createURI("http://entsoe.eu/CIM/SSH");
    final static Node IRI_FOR_PROFILE_SV = NodeFactory.createURI("http://entsoe.eu/CIM/SV");
    final static Node IRI_FOR_PROFILE_TP = NodeFactory.createURI("http://entsoe.eu/CIM/TP");

    final static Node[] ALL_PROFILES = new Node[] { IRI_FOR_PROFILE_EQ, IRI_FOR_PROFILE_SSH, IRI_FOR_PROFILE_SV, IRI_FOR_PROFILE_TP };

    private ExecutorService executorService;
    private SchemaRegistry schemaRegistry;


    @Test
    public void query() throws Exception {
        this.executorService = Executors.newWorkStealingPool();

        this.schemaRegistry = new SchemaRegistry();
        readProfiles(executorService, schemaRegistry);

        //wait for schemas to be loaded
        for (Node profile : ALL_PROFILES) {
            while (!schemaRegistry.contains(profile)) {
                Thread.onSpinWait();
            }
        }

        final var graphRegistry = new ConcurrentHashMap<Node, Graph>();
        readCGMESFromZipArchive("C:\\temp\\CGMES_v2.4.15_TestConfigurations_v4.0.3\\MicroGrid\\BaseCase_BC\\CGMES_v2.4.15_MicroGridTestConfiguration_BC_BE_v2.zip",
                executorService, schemaRegistry, graphRegistry);
        var datasetGraph = new DatasetGraphMapLink(GraphFactory.createGraphMem());
        for (var entry : graphRegistry.entrySet()) {
            datasetGraph.addGraph(entry.getKey(), entry.getValue());
        }
        //var queryString = "SELECT ?s ?p ?o WHERE { GRAPH ?g { ?s ?p ?o } }";
        // select cim:Terminal and the property ACDCTermin.connected
        var queryString = """
                PREFIX cim:<http://iec.ch/TC57/2013/CIM-schema-cim16#>
                PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                SELECT ?s ?name ?connected
                FROM NAMED <http://entsoe.eu/CIM/EQ>
                FROM NAMED <http://entsoe.eu/CIM/SSH>
                WHERE { 
                    GRAPH <http://entsoe.eu/CIM/EQ> {
                       ?s a cim:Terminal;
                        cim:IdentifiedObject.name ?name.
                    }
                    GRAPH <http://entsoe.eu/CIM/SSH> {
                        ?s cim:ACDCTerminal.connected ?connected.
                    }
                }
                """;
        var query = QueryFactory.create(queryString);
        var op = Algebra.compile(query);
        var collector = new QueryElementCollector();
        Walker.walk(op, collector);
        var qe = QueryExecutionFactory.create(query, datasetGraph);
        qe.execSelect().forEachRemaining(result -> {
            result.varNames().forEachRemaining(var -> {
                System.out.print(var + ": " + result.get(var) + " ");
            });
            System.out.println();
        });
    }

    @Test
    public void queryAlgebra() throws Exception {
        var queryString = """
                PREFIX res:<http://entsoe.eu/CIM/RES#>
                PREFIX rfn:<http://entsoe.eu/CIM/RES/Function#>
                SELECT ?resObject ?resProcess
                 FROM NAMED <RES_EQ>
                 WHERE {
                 GRAPH <RES_EQ>
                     {
                         ?resObject a res:AOGeneratingUnit.
                         { ?resObject rfn:existsInProcess ?resProcess. }
                         UNION 
                         { BIND("SHARED" AS ?resProcess) }
                     }
                 }
                """;
        var query = QueryFactory.create(queryString);
        var op = Algebra.compile(query);
        System.out.println(op.toString());
    }

    @Test
    public void queryAlgebraName1() throws Exception {
        var queryString1 = """
                PREFIX cim:<http://iec.ch/TC57/2013/CIM-schema-cim16#>
                PREFIX rfn:<http://entsoe.eu/CIM/RES/Function#>
                PREFIX entsoe:<http://entsoe.eu/CIM/RES#>
                SELECT ?mrid (COALESCE(?shortName, ?name) as ?name1)
                   FROM <RES_EQ>
                   WHERE {
                       ?mrid cim:IdentifiedObject.name ?name.
                       OPTIONAL { ?mrid entsoe:IdentifiedObject.shortName ?shortName. }
                   }
                """;
        var query1 = QueryFactory.create(queryString1);
        var op1 = Algebra.compile(query1);
        System.out.println(op1.toString());

        var queryString2 = """
                PREFIX cim:<http://iec.ch/TC57/2013/CIM-schema-cim16#>
                PREFIX rfn:<http://entsoe.eu/CIM/RES/Function#>
                PREFIX entsoe:<http://entsoe.eu/CIM/RES#>
                SELECT ?mrid ?name1
                  FROM <RES_EQ>
                  WHERE {
                      ?mrid cim:IdentifiedObject.name ?name.
                      OPTIONAL { ?mrid entsoe:IdentifiedObject.shortName ?shortName. }
                      BIND(IF(BOUND(?shortName), ?shortName, ?name) as ?name1)
                  }
                """;
        var query2 = QueryFactory.create(queryString2);
        var op2 = Algebra.compile(query2);
        System.out.println(op2.toString());
    }


    private class QueryElementCollector extends OpVisitorBase {
        final Set<Node> usedClasses = new HashSet<>();
        final Set<Node> usedProperties = new HashSet<>();
        final Map<String, Set<String>> propertyUsages = new HashMap<>();
        final Map<String, String> variableTypes = new HashMap<>();
        final Set<Node> usedGraphs = new HashSet<>();

        private void processTriple(Triple triple) {
            Node subject = triple.getSubject();
            Node predicate = triple.getPredicate();
            Node object = triple.getObject();
            // RDF-Type Assertions sammeln
            if (predicate.isURI() && predicate.getURI().equals(RDF.type.getURI())) {
                if (object.isURI()) {
                    usedClasses.add(object);
                    if (subject.isVariable()) {
                        variableTypes.put(subject.getName(), object.getURI());
                    }
                }
            }
            // Properties sammeln
            if (predicate.isURI() && !predicate.getURI().equals(RDF.type.getURI())) {
                usedProperties.add(predicate);

                // Datentypen für Properties sammeln
                if (object.isLiteral()) {
                    String datatype = object.getLiteralDatatypeURI();
                    if (datatype != null) {
                        propertyUsages.computeIfAbsent(predicate.getURI(),
                                k -> new HashSet<>()).add(datatype);
                    }
                }
            }
        }
        private void processTriplePath(TriplePath triplePath) {
            if (triplePath.isTriple()) {
                processTriple(triplePath.asTriple());
            } else {
                Path path = triplePath.getPath();
                if (path != null) {
                    processPath(path);
                }
            }
        }
        private void processPath(Path path) {
            PathVisitor visitor = new PathVisitorBase() {
                @Override
                public void visit(P_Link pathNode) {
                    Node node = pathNode.getNode();
                    if (node.isURI()) {
                        usedProperties.add(node);
                    }
                }

                @Override
                public void visit(P_Seq pathSeq) {
                    pathSeq.getLeft().visit(this);
                    pathSeq.getRight().visit(this);
                }

                @Override
                public void visit(P_Alt pathAlt) {
                    pathAlt.getLeft().visit(this);
                    pathAlt.getRight().visit(this);
                }

                @Override
                public void visit(P_OneOrMore1 path) {
                    path.getSubPath().visit(this);
                }

                @Override
                public void visit(P_ZeroOrMore1 path) {
                    path.getSubPath().visit(this);
                }
            };

            path.visit(visitor);
        }

        @Override
        public void visit(OpBGP opBGP) {
            opBGP.getPattern().forEach(triple -> {
                processTriple(triple);
            });
        }
        @Override
        public void visit(OpQuadPattern quadPattern) {
            quadPattern.getPattern().forEach(quad -> {
                processTriple(quad.asTriple());
            });
        }
        @Override
        public void visit(OpQuadBlock quadBlock) {
            quadBlock.getPattern().forEach(quad -> {
                processTriple(quad.asTriple());
            });
        }
        @Override
        public void visit(OpTriple opTriple) {
            processTriple(opTriple.getTriple());
        }
        @Override
        public void visit(OpQuad opQuad) {
            processTriple(opQuad.getQuad().asTriple());
        }
        @Override
        public void visit(OpPath opPath) {
            processTriplePath(opPath.getTriplePath());
        }
        @Override
        public void visit(OpFilter opFilter) {
            opFilter.getExprs().forEach(expr -> {
                if (expr.isConstant()) {
                    var nodeValue = expr.getConstant();
                    if (nodeValue.isLiteral()) {
                        String datatype = nodeValue.getDatatypeURI();
                        if (datatype != null) {
                            String property = expr.getFunction().getFunctionIRI();
                            propertyUsages.computeIfAbsent(property, k -> new HashSet<>()).add(datatype);
                        }
                    } else if (nodeValue.isIRI()) {
                        usedProperties.add(nodeValue.getNode());
                    }
                }
            });
        }
        @Override
        public void visit(OpGraph opGraph) {
            if (opGraph.getNode().isURI()) {
                usedGraphs.add(opGraph.getNode());
            }
        }
    }


    @Benchmark
    public ConcurrentHashMap<Node, Graph> readRealGrid() throws Exception {
        final var graphRegistry = new ConcurrentHashMap<Node, Graph>();
        readCGMESFromZipArchive("C:\\rd\\bewegungsdaten-demo\\shared\\ENTSOE_RDF\\src\\main\\resources\\CGMES\\v2.4.15\\TestConfigurations_packageCASv2.0\\RealGrid\\CGMES_v2.4.15_RealGridTestConfiguration_v2.zip",
                executorService, schemaRegistry, graphRegistry);
        return graphRegistry;
    }

    private static void readCGMESFromZipArchive(String pathToZipArchive, ExecutorService executorService, SchemaRegistry schemaRegistry, ConcurrentHashMap<Node, Graph> graphRegistry) throws Exception {
        try (final ZipFile zipFile = new ZipFile(pathToZipArchive)) {
            // Get all entries and filter for .xml files
            final var entries = zipFile.entries();
            final var futures = new ArrayList<Future<?>>();
            while(entries.hasMoreElements()) {
                final var entry = entries.nextElement();

                if (entry.isDirectory() || !entry.getName().endsWith(".xml"))
                    continue;

                futures.add(executorService.submit(() -> {
                    try (final InputStream is = zipFile.getInputStream(entry)) {
                        final var profile = heuristicallyGuessProfile(entry.getName());
                        while (!schemaRegistry.contains(profile)) {
                            Thread.onSpinWait();
                        }
                        graphRegistry.put(profile, schemaRegistry.parseRDFXML(profile, is));
                        //System.out.println("Read profile " + profile);
                    } catch (Exception e) {
                        System.out.println("Failed to read entry " + entry.getName() + ": " + e.getMessage());
                        throw new RuntimeException(e);
                    }
                }));
            }
            while (futures.stream().filter(f -> !f.isDone()).findAny().isPresent()) {
                Thread.onSpinWait();
            }
        }
    }

    private static void readProfiles(ExecutorService executorService, SchemaRegistry schemaRegistry) throws IOException {
        for(var profile : ALL_PROFILES) {
            final var pathToRDFS = getPathToRDFS(profile);
            executorService.submit(() -> {
                schemaRegistry.register(profile, pathToRDFS);
                //System.out.println("Read schema " + profile);
            });
        }
    }

    private static String getPathToRDFS(Node profileAsNode) {
        if (IRI_FOR_PROFILE_EQ == profileAsNode)
            return TestFileInventory.getFilePath(TestFileInventory.RDF_EQUIPMENT_CORE_PROFILE_RDFS_AUGMENTED_V2_4_15_4SEP2020);
        if (IRI_FOR_PROFILE_SSH == profileAsNode)
            return TestFileInventory.getFilePath(TestFileInventory.RDF_STEADY_STATE_HYPOTHESIS_PROFILE_RDFS_AUGMENTED_V2_4_15_4SEP2020);
        if (IRI_FOR_PROFILE_SV == profileAsNode)
            return TestFileInventory.getFilePath(TestFileInventory.RDF_STATE_VARIABLE_PROFILE_RDFS_AUGMENTED_V2_4_15_4SEP2020);
        if (IRI_FOR_PROFILE_TP == profileAsNode)
            return TestFileInventory.getFilePath(TestFileInventory.RDF_TOPOLOGY_PROFILE_RDFS_AUGMENTED_V2_4_15_4SEP2020);
        throw new IllegalArgumentException("Unsupported profile: " + profileAsNode);
    }

    private static Node heuristicallyGuessProfile(String filename) {
        if(filename.contains("_EQ_"))
            return IRI_FOR_PROFILE_EQ;
        if(filename.contains("_SSH_"))
            return IRI_FOR_PROFILE_SSH;
        if(filename.contains("_SV_"))
            return IRI_FOR_PROFILE_SV;
        if(filename.contains("_TP_"))
            return IRI_FOR_PROFILE_TP;
        throw new RuntimeException("Unknown profile: " + filename);
    }

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        this.executorService = Executors.newWorkStealingPool();

        this.schemaRegistry = new SchemaRegistry();
        readProfiles(executorService, schemaRegistry);

        //wait for schemas to be loaded
        for (Node profile : ALL_PROFILES) {
            while (!schemaRegistry.contains(profile)) {
                Thread.onSpinWait();
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        executorService.shutdown();
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JMHDefaultOptions.getDefaults(this.getClass())
                //.warmupIterations(0)
                //.measurementIterations(1)
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }

}

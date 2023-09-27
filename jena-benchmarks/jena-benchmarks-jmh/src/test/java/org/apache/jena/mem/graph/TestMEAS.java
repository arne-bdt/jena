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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.mem.GraphMem;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.mem2.GraphMem2Fast;
import org.apache.jena.mem2.GraphMem2Legacy;
import org.apache.jena.mem2.GraphMem2Roaring;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@State(Scope.Benchmark)
public class TestMEAS {

    @Param({
            "GraphMem",
            "GraphMem2Fast",
            "GraphMem2Legacy",
            "GraphMem2Roaring"
    })
    public String param0_GraphImplementation;
    @Param({"90000"})
    public String param1_numberOfAnalogValues;
    @Param({"10000"})
    public String param2_numberOfDiscreteValues;
    private Graph sutGraph;
    private DatasetGraph sutDatasetGraph;
    private Query sutAnalogQuery;
    private Query sutDiscreteQuery;
    private List<QuerySolution> sutQuerySolutions;
    private int numberOfAnalogValues;
    private int numberOfDiscreteValues;

    public Graph createGraph() {
        switch (param0_GraphImplementation) {
            case "GraphMem":
                return new GraphMem();
            case "GraphMem2Fast":
                return new GraphMem2Fast();
            case "GraphMem2Legacy":
                return new GraphMem2Legacy();
            case "GraphMem2Roaring":
                return new GraphMem2Roaring();
            default:
                throw new IllegalArgumentException("Unknown graph implementation: " + param0_GraphImplementation);
        }
    }


    private List<QuerySolution> queryAnalogAndDigitalValues(Graph graph) {
        var model = ModelFactory.createModelForGraph(graph);

        var result = new ArrayList<QuerySolution>(100000);
        QueryExecutionFactory.create("""
                        PREFIX meas: <http://www.fancyTSO.org/OurCIMModel/MEASv1#>
                        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        SELECT ?s ?analogValue ?timeStamp ?status  
                        WHERE { ?s meas:AnalogValue.value ?analogValue;
                                   meas:MeasurementValue.timeStamp ?timeStamp;
                                   meas:MeasurementValue.status ?status.}
                        """, model)
                .execSelect()
                .forEachRemaining(result::add);
        QueryExecutionFactory.create("""
                        PREFIX meas: <http://www.fancyTSO.org/OurCIMModel/MEASv1#>
                        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        SELECT ?s ?discreteValue ?timeStamp ?status  
                        WHERE { ?s meas:DiscreteValue.value ?discreteValue;
                                   meas:MeasurementValue.timeStamp ?timeStamp;
                                   meas:MeasurementValue.status ?status.}
                        """, model)
                .execSelect()
                .forEachRemaining(result::add);
        return result;
    }

    private List<QuerySolution> queryAnalogAndDigitalValuesWithPreparedQueries(DatasetGraph datasetGraph) {

        var result = new ArrayList<QuerySolution>(100000);

        var queryExecutionAnalog = QueryExecutionFactory.create(sutAnalogQuery, datasetGraph);
        var queryExecutionDiscrete = QueryExecutionFactory.create(sutDiscreteQuery, datasetGraph);

        queryExecutionAnalog.execSelect().forEachRemaining(result::add);
        queryExecutionDiscrete.execSelect().forEachRemaining(result::add);

        return result;
    }

    private Pair<List<AnalogValue>, List<DiscreteValue>> fillQueryResultsIntoLists(List<QuerySolution> result) {
        final var analogValues = new ArrayList<AnalogValue>(numberOfAnalogValues);
        final var discreteValues = new ArrayList<DiscreteValue>(numberOfDiscreteValues);
        for (var querySolution : result) {
            if (querySolution.contains("analogValue")) {
                analogValues.add(new AnalogValue((float) querySolution.getLiteral("analogValue").getValue(), ((XSDDateTime) querySolution.getLiteral("timeStamp").getValue()).asCalendar().toInstant(), (int) querySolution.getLiteral("status").getValue()));
            } else {
                discreteValues.add(new DiscreteValue((int) querySolution.getLiteral("discreteValue").getValue(), ((XSDDateTime) querySolution.getLiteral("timeStamp").getValue()).asCalendar().toInstant(), (int) querySolution.getLiteral("status").getValue()));
            }
        }
        return Pair.of(analogValues, discreteValues);
    }

    @Test
    public void loadRDFSAndProfile() {
        /*TODO: Create JMH-Benchmark out of this to prevent the JVM from taking shortcuts */
        final var g = new GraphMem2Fast();

        MEASData.fillGraphWithMEASData(g, numberOfAnalogValues, numberOfDiscreteValues);

        var result = queryAnalogAndDigitalValues(g);

        fillQueryResultsIntoLists(result);

        //result = queryAnalogAndDigitalValuesWithPreparedQueries(DatasetGraphFactory.wrap(g));

        //fillQueryResultsIntoLists(result);

        fillListsByGraph(g);

        int i = 0;
    }

//    @Benchmark
//    public Graph createGraphAndFillWithMEASData() {
//        final var g = createGraph();
//        MEASData.fillGraphWithMEASData(g, numberOfAnalogValues, numberOfDiscreteValues);
//        return g;
//    }

    @Benchmark
    public List<QuerySolution> queryAnalogAndDigitalValuesWithPreparedQueries() {
        return queryAnalogAndDigitalValuesWithPreparedQueries(sutDatasetGraph);
    }

    @Benchmark
    public List<QuerySolution> queryAnalogAndDigitalValues() {
        return queryAnalogAndDigitalValues(sutGraph);
    }

    @Benchmark
    public Pair<List<AnalogValue>, List<DiscreteValue>> fillQueryResultsIntoLists() {
        return fillQueryResultsIntoLists(sutQuerySolutions);
    }

    @Benchmark
    public Pair<List<AnalogValue>, List<DiscreteValue>> fillListsByGraph() {
        return fillListsByGraph(sutGraph);
    }

    private Pair<List<AnalogValue>, List<DiscreteValue>> fillListsByGraph(Graph g) {
        final var analogValues = new ArrayList<AnalogValue>(numberOfAnalogValues);
        final var discreteValues = new ArrayList<DiscreteValue>(numberOfDiscreteValues);
        g.stream(Node.ANY, MEASData.AnalogValueValue.asNode(), Node.ANY)
                .forEach(triple -> {
                    final var s = triple.getSubject();
                    final var timeStamp = ((XSDDateTime) g.find(s, MEASData.MeasurementValueTimeStamp.asNode(), Node.ANY).next().getObject().getLiteralValue()).asCalendar().toInstant();
                    final var status = (int) g.find(s, MEASData.MeasurementValueStatus.asNode(), Node.ANY).next().getObject().getLiteralValue();
                    analogValues.add(new AnalogValue((float) triple.getObject().getLiteralValue(), timeStamp, status));
                });
        g.stream(Node.ANY, MEASData.DiscreteValueValue.asNode(), Node.ANY)
                .forEach(triple -> {
                    final var s = triple.getSubject();
                    final var timeStamp = ((XSDDateTime) g.find(s, MEASData.MeasurementValueTimeStamp.asNode(), Node.ANY).next().getObject().getLiteralValue()).asCalendar().toInstant();
                    final var status = (int) g.find(s, MEASData.MeasurementValueStatus.asNode(), Node.ANY).next().getObject().getLiteralValue();
                    discreteValues.add(new DiscreteValue((int) triple.getObject().getLiteralValue(), timeStamp, status));
                });
        return Pair.of(analogValues, discreteValues);
    }



    private record AnalogValue(float value, Instant timeStamp, int status) {
    }

    private record DiscreteValue(int value, Instant timeStamp, int status) {
    }

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        this.numberOfAnalogValues = Integer.parseInt(param1_numberOfAnalogValues);
        this.numberOfDiscreteValues = Integer.parseInt(param2_numberOfDiscreteValues);
        this.sutGraph = createGraph();
        MEASData.fillGraphWithMEASData(this.sutGraph, numberOfAnalogValues, numberOfDiscreteValues);
        this.sutDatasetGraph = DatasetGraphFactory.wrap(this.sutGraph);
        this.sutAnalogQuery = QueryFactory.create("""
                PREFIX meas: <http://www.fancyTSO.org/OurCIMModel/MEASv1#>
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                SELECT ?s ?analogValue ?timeStamp ?status  
                WHERE { ?s meas:AnalogValue.value ?analogValue;
                           meas:MeasurementValue.timeStamp ?timeStamp;
                           meas:MeasurementValue.status ?status.}
                """);
        this.sutDiscreteQuery = QueryFactory.create("""
                PREFIX meas: <http://www.fancyTSO.org/OurCIMModel/MEASv1#>
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                SELECT ?s ?discreteValue ?timeStamp ?status  
                WHERE { ?s meas:DiscreteValue.value ?discreteValue;
                           meas:MeasurementValue.timeStamp ?timeStamp;
                           meas:MeasurementValue.status ?status.}
                """);
        this.sutQuerySolutions = queryAnalogAndDigitalValuesWithPreparedQueries(this.sutDatasetGraph);
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JMHDefaultOptions.getDefaults(this.getClass())
                .measurementIterations(15)
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }

}

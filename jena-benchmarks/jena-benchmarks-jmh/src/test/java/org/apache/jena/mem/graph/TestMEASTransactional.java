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
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.mem2.GraphWrapperTransactional;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@State(Scope.Benchmark)
public class TestMEASTransactional {

    @Param({"100000"})
    public String p1_totalAnalogValues;
    @Param({"25000"})
    public String p2_totalDiscreteValues;
    @Param({"100000"})
    public String p3_numAnalogValuesToUpdate;
    @Param({"25000"})
    public String p4_numDiscreteValuesToUpdate;
    private GraphWrapperTransactional sutGraph;
    private Query sutAnalogQuery;
    private Query sutDiscreteQuery;
    private List<QuerySolution> sutQuerySolutions;
    private List<MEASData.AnalogValue> sutAnalogValues;
    private List<MEASData.DiscreteValue> sutDiscreteValues;

    private int totalAnalogValues;
    private int totalDiscreteValues;
    private int numAnalogValuesToUpdate;
    private int numDiscreteValuesToUpdate;

    private static void updateAnalogAndDiscreteValues(GraphWrapperTransactional g, List<MEASData.AnalogValue> analogValues, List<MEASData.DiscreteValue> discreteValues, int numAnalogValuesToUpdate, int numDiscreteValuesToUpdate) {
        g.begin(ReadWrite.WRITE);
        for (final var analogValue : analogValues.subList(0, numAnalogValuesToUpdate)) {
            final var subject = NodeFactory.createURI(analogValue.uuid());

            final var tAnalogValueValue = g.find(subject, MEASData.AnalogValueValue.asNode(), Node.ANY).next();
            g.delete(tAnalogValueValue);
            g.add(Triple.create(subject, tAnalogValueValue.getPredicate(), NodeFactory.createLiteralByValue(analogValue.value(), XSDDatatype.XSDfloat)));

            final var tMeasurementValueTimeStamp = g.find(subject, MEASData.MeasurementValueTimeStamp.asNode(), Node.ANY).next();
            g.delete(tMeasurementValueTimeStamp);
            g.add(Triple.create(subject, tMeasurementValueTimeStamp.getPredicate(), NodeFactory.createLiteral(DateTimeFormatter.ISO_INSTANT.format(analogValue.timeStamp()), XSDDatatype.XSDdateTimeStamp)));

            final var tMeasurementValueStatus = g.find(subject, MEASData.MeasurementValueStatus.asNode(), Node.ANY).next();
            g.delete(tMeasurementValueStatus);
            g.add(Triple.create(subject, tMeasurementValueStatus.getPredicate(), NodeFactory.createLiteralByValue(analogValue.status(), XSDDatatype.XSDinteger)));
        }
        for (final var discreteValue : discreteValues.subList(0, numDiscreteValuesToUpdate)) {
            final var subject = NodeFactory.createURI(discreteValue.uuid());

            final var tDiscreteValueValue = g.find(subject, MEASData.DiscreteValueValue.asNode(), Node.ANY).next();
            g.delete(tDiscreteValueValue);
            g.add(Triple.create(subject, tDiscreteValueValue.getPredicate(), NodeFactory.createLiteralByValue(discreteValue.value(), XSDDatatype.XSDinteger)));

            final var tMeasurementValueTimeStamp = g.find(subject, MEASData.MeasurementValueTimeStamp.asNode(), Node.ANY).next();
            g.delete(tMeasurementValueTimeStamp);
            g.add(Triple.create(subject, tMeasurementValueTimeStamp.getPredicate(), NodeFactory.createLiteral(DateTimeFormatter.ISO_INSTANT.format(discreteValue.timeStamp()), XSDDatatype.XSDdateTimeStamp)));

            final var tMeasurementValueStatus = g.find(subject, MEASData.MeasurementValueStatus.asNode(), Node.ANY).next();
            g.delete(tMeasurementValueStatus);
            g.add(Triple.create(subject, tMeasurementValueStatus.getPredicate(), NodeFactory.createLiteralByValue(discreteValue.status(), XSDDatatype.XSDinteger)));
        }
        g.commit();
    }

    @Test
    @Ignore("Only for debugging.")
    public void loadRDFSAndProfile() {
        final var g = createGraph();

        final var analogValues = MEASData.generateRandomAnalogValues(90000);
        final var discreteValues = MEASData.generateRandomDiscreteValues(10000);

        g.begin(ReadWrite.WRITE);
        MEASData.addAnalogValuesToGraph(g, analogValues);
        MEASData.addDiscreteValuesToGraph(g, discreteValues);
        g.commit();

        var result = queryAnalogAndDigitalValueSolutions(g);

        var lists = fillQueryResultsIntoLists(result);

        result = queryAnalogAndDigitalValuesWithPreparedQueries(DatasetGraphFactory.wrap(g));

        fillQueryResultsIntoLists(result);

        var lists2 = fillListsByGraph(g);

        final var updatedAnalogValues = MEASData.getRandomlyUpdatedAnalogValues(analogValues);
        final var updatedDiscreteValues = MEASData.getRandomlyUpdatedDiscreteValues(discreteValues);

        updateAnalogAndDiscreteValues(g, updatedAnalogValues, updatedDiscreteValues, 1000, 1000);

        var lists3 = queryAnalogAndDigitalValues(g);

        int i = 0;
    }

    @Benchmark
    public Graph createGraphAndFillWithMEASData() {
        final var g = createGraph();
        g.begin(ReadWrite.WRITE);
        MEASData.addAnalogValuesToGraph(g, sutAnalogValues);
        MEASData.addDiscreteValuesToGraph(g, sutDiscreteValues);
        g.commit();
        return g;
    }

    @Benchmark
    public List<QuerySolution> queryAnalogAndDigitalValuesWithPreparedQueries() {
        this.sutGraph.begin(ReadWrite.READ);
        final var result = queryAnalogAndDigitalValuesWithPreparedQueries(DatasetGraphFactory.wrap(this.sutGraph));
        this.sutGraph.end();
        return result;
    }

    @Benchmark
    public List<QuerySolution> queryAnalogAndDigitalValueSolutions() {
        return queryAnalogAndDigitalValueSolutions(sutGraph);
    }

    @Benchmark
    public Pair<List<MEASData.AnalogValue>, List<MEASData.DiscreteValue>> fillQueryResultsIntoLists() {
        return fillQueryResultsIntoLists(sutQuerySolutions);
    }

    @Benchmark
    public Pair<List<MEASData.AnalogValue>, List<MEASData.DiscreteValue>> queryAnalogAndDigitalValues() {
        return queryAnalogAndDigitalValues(sutGraph);
    }

    @Benchmark
    public Pair<List<MEASData.AnalogValue>, List<MEASData.DiscreteValue>> fillListsByGraph() {
        return fillListsByGraph(sutGraph);
    }

    @Benchmark
    public Graph updateAllValuesInGraph() {
        updateAnalogAndDiscreteValues(sutGraph, sutAnalogValues, sutDiscreteValues, numAnalogValuesToUpdate, numDiscreteValuesToUpdate);
        return sutGraph;
    }

    public GraphWrapperTransactional createGraph() {
        return new GraphWrapperTransactional();
    }

    private List<QuerySolution> queryAnalogAndDigitalValueSolutions(GraphWrapperTransactional graph) {
        graph.begin(ReadWrite.READ);
        var model = ModelFactory.createModelForGraph(graph);

        var result = new ArrayList<QuerySolution>(totalAnalogValues + totalDiscreteValues);
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
        graph.end();
        return result;
    }

    private Pair<List<MEASData.AnalogValue>, List<MEASData.DiscreteValue>> queryAnalogAndDigitalValues(GraphWrapperTransactional graph) {
        graph.begin(ReadWrite.READ);
        final var model = ModelFactory.createModelForGraph(graph);
        final var analogValues = new ArrayList<MEASData.AnalogValue>(totalAnalogValues);
        final var discreteValues = new ArrayList<MEASData.DiscreteValue>(totalDiscreteValues);
        QueryExecutionFactory.create("""
                        PREFIX meas: <http://www.fancyTSO.org/OurCIMModel/MEASv1#>
                        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        SELECT ?s ?analogValue ?timeStamp ?status  
                        WHERE { ?s meas:AnalogValue.value ?analogValue;
                                   meas:MeasurementValue.timeStamp ?timeStamp;
                                   meas:MeasurementValue.status ?status.}
                        """, model)
                .execSelect()
                .forEachRemaining(row -> {
                    analogValues.add(new MEASData.AnalogValue(row.getResource("s").getURI(), (float) row.getLiteral("analogValue").getValue(), ((XSDDateTime) row.getLiteral("timeStamp").getValue()).asCalendar().toInstant(), (int) row.getLiteral("status").getValue()));
                });
        QueryExecutionFactory.create("""
                        PREFIX meas: <http://www.fancyTSO.org/OurCIMModel/MEASv1#>
                        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        SELECT ?s ?discreteValue ?timeStamp ?status  
                        WHERE { ?s meas:DiscreteValue.value ?discreteValue;
                                   meas:MeasurementValue.timeStamp ?timeStamp;
                                   meas:MeasurementValue.status ?status.}
                        """, model)
                .execSelect()
                .forEachRemaining(row -> {
                    discreteValues.add(new MEASData.DiscreteValue(row.getResource("s").getURI(), (int) row.getLiteral("discreteValue").getValue(), ((XSDDateTime) row.getLiteral("timeStamp").getValue()).asCalendar().toInstant(), (int) row.getLiteral("status").getValue()));
                });
        graph.end();
        return Pair.of(analogValues, discreteValues);
    }

    private List<QuerySolution> queryAnalogAndDigitalValuesWithPreparedQueries(DatasetGraph datasetGraph) {
        var result = new ArrayList<QuerySolution>(totalAnalogValues + totalDiscreteValues);

        var queryExecutionAnalog = QueryExecutionFactory.create(sutAnalogQuery, datasetGraph);
        var queryExecutionDiscrete = QueryExecutionFactory.create(sutDiscreteQuery, datasetGraph);

        queryExecutionAnalog.execSelect().forEachRemaining(result::add);
        queryExecutionDiscrete.execSelect().forEachRemaining(result::add);

        return result;
    }

    private Pair<List<MEASData.AnalogValue>, List<MEASData.DiscreteValue>> fillQueryResultsIntoLists(List<QuerySolution> result) {
        final var analogValues = new ArrayList<MEASData.AnalogValue>(totalAnalogValues);
        final var discreteValues = new ArrayList<MEASData.DiscreteValue>(totalDiscreteValues);
        for (var querySolution : result) {
            if (querySolution.contains("analogValue")) {
                analogValues.add(new MEASData.AnalogValue(querySolution.getResource("s").getURI(), (float) querySolution.getLiteral("analogValue").getValue(), ((XSDDateTime) querySolution.getLiteral("timeStamp").getValue()).asCalendar().toInstant(), (int) querySolution.getLiteral("status").getValue()));
            } else {
                discreteValues.add(new MEASData.DiscreteValue(querySolution.getResource("s").getURI(), (int) querySolution.getLiteral("discreteValue").getValue(), ((XSDDateTime) querySolution.getLiteral("timeStamp").getValue()).asCalendar().toInstant(), (int) querySolution.getLiteral("status").getValue()));
            }
        }
        return Pair.of(analogValues, discreteValues);
    }

    private Pair<List<MEASData.AnalogValue>, List<MEASData.DiscreteValue>> fillListsByGraph(GraphWrapperTransactional g) {

        final var analogValues = new ArrayList<MEASData.AnalogValue>(totalAnalogValues);
        final var discreteValues = new ArrayList<MEASData.DiscreteValue>(totalDiscreteValues);
        g.begin(ReadWrite.READ);
        g.stream(Node.ANY, MEASData.AnalogValueValue.asNode(), Node.ANY)
                .forEach(triple -> {
                    final var s = triple.getSubject();
                    final var timeStamp = ((XSDDateTime) g.find(s, MEASData.MeasurementValueTimeStamp.asNode(), Node.ANY).next().getObject().getLiteralValue()).asCalendar().toInstant();
                    final var status = (int) g.find(s, MEASData.MeasurementValueStatus.asNode(), Node.ANY).next().getObject().getLiteralValue();
                    analogValues.add(new MEASData.AnalogValue(s.getURI(), (float) triple.getObject().getLiteralValue(), timeStamp, status));
                });
        g.stream(Node.ANY, MEASData.DiscreteValueValue.asNode(), Node.ANY)
                .forEach(triple -> {
                    final var s = triple.getSubject();
                    final var timeStamp = ((XSDDateTime) g.find(s, MEASData.MeasurementValueTimeStamp.asNode(), Node.ANY).next().getObject().getLiteralValue()).asCalendar().toInstant();
                    final var status = (int) g.find(s, MEASData.MeasurementValueStatus.asNode(), Node.ANY).next().getObject().getLiteralValue();
                    discreteValues.add(new MEASData.DiscreteValue(s.getURI(), (int) triple.getObject().getLiteralValue(), timeStamp, status));
                });
        g.end();
        return Pair.of(analogValues, discreteValues);
    }

    @Setup(Level.Iteration)
    public void setupIteration() {
        this.sutAnalogValues = MEASData.getRandomlyUpdatedAnalogValues(sutAnalogValues);
        this.sutDiscreteValues = MEASData.getRandomlyUpdatedDiscreteValues(sutDiscreteValues);
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        this.totalAnalogValues = Integer.parseInt(p1_totalAnalogValues);
        this.totalDiscreteValues = Integer.parseInt(p2_totalDiscreteValues);
        this.numAnalogValuesToUpdate = Integer.parseInt(p3_numAnalogValuesToUpdate);
        this.numDiscreteValuesToUpdate = Integer.parseInt(p4_numDiscreteValuesToUpdate);
        this.sutGraph = createGraph();
        this.sutAnalogValues = MEASData.generateRandomAnalogValues(totalAnalogValues);
        this.sutDiscreteValues = MEASData.generateRandomDiscreteValues(totalDiscreteValues);

        sutGraph.begin(ReadWrite.WRITE);
        MEASData.addAnalogValuesToGraph(sutGraph, sutAnalogValues);
        MEASData.addDiscreteValuesToGraph(sutGraph, sutDiscreteValues);
        sutGraph.commit();

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
        sutGraph.begin(ReadWrite.READ);
        this.sutQuerySolutions = queryAnalogAndDigitalValuesWithPreparedQueries(DatasetGraphFactory.wrap(this.sutGraph));
        sutGraph.end();
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

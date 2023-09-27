package org.apache.jena.mem.graph;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.GraphMemFactory;
import org.apache.jena.graph.Node;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.vocabulary.RDF;
import org.junit.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class TestMEAS {

    private static final Model m = ModelFactory.createDefaultModel();

    private static final String MEAS_NS = "http://www.fancyTSO.org/OurCIMModel/MEASv1#";

    private static final String[] AnalogTypes = {"SomeAnalog", "ActivePowerAnalog", "ReactivePowerAnalog", "VoltageAnalog", "PhaseAngleAnalog", "GlobalRadiationAnalog", "HumidityAnalog", "TemperatureAnalog", "WindSpeedAnalog", "WindDirectionAnalog", "FrequencyAnalog", "PowerFactorAnalog", "CurrentAnalog"};
    private static final String[] DiscreteTypes = {"PhaseChangerStep", "BreakerStatus", "SwitchStatus", "TapChangerStatus", "TapChangerStep", "TapChangerControlMode", "TapChangerNeutralStatus", "TapChangerKind", "TapChangerMode", "TapChangerControlKind"};

    private static final Property AnalogValueValue = m.createProperty(MEAS_NS + "AnalogValue.value");
    private static final Property DiscreteValueValue = m.createProperty(MEAS_NS + "DiscreteValue.value");

    private static final Property MeasurementValueTimeStamp = m.createProperty(MEAS_NS + "MeasurementValue.timeStamp");
    private static final Property MeasurementValueStatus = m.createProperty(MEAS_NS + "MeasurementValue.status");

    public static Graph createGraphWithMEASData(int numberOfAnalogValues, int numberOfDiscreteValues) {
        final var graph = GraphMemFactory.createGraphMem2Basic();
        final var model = ModelFactory.createModelForGraph(graph);

        final var random = new Random(4711);

        final var analogTypeProvider = new NextStringInArrayProvider(AnalogTypes);
        for (int i = 0; i < numberOfAnalogValues; i++) {
            model.createResource("_" + UUID.randomUUID())
                    .addProperty(RDF.type, model.createResource(MEAS_NS + analogTypeProvider.next()))
                    .addProperty(MeasurementValueTimeStamp, DateTimeFormatter.ISO_INSTANT.format(Clock.systemUTC().instant()), XSDDatatype.XSDdateTimeStamp)
                    .addProperty(MeasurementValueStatus, Integer.toString(random.nextInt()), XSDDatatype.XSDinteger)
                    .addProperty(AnalogValueValue, Float.toString(random.nextFloat()), XSDDatatype.XSDfloat);
        }

        final var discreteTypeProvider = new NextStringInArrayProvider(DiscreteTypes);
        for (int i = 0; i < numberOfDiscreteValues; i++) {
            model.createResource("_" + UUID.randomUUID())
                    .addProperty(RDF.type, model.createResource(MEAS_NS + discreteTypeProvider.next()))
                    .addProperty(MeasurementValueTimeStamp, DateTimeFormatter.ISO_INSTANT.format(Clock.systemUTC().instant()), XSDDatatype.XSDdateTimeStamp)
                    .addProperty(MeasurementValueStatus, Integer.toString(random.nextInt()), XSDDatatype.XSDinteger)
                    .addProperty(DiscreteValueValue, Integer.toString(random.nextInt()), XSDDatatype.XSDinteger);
        }

        return graph;
    }

    private List<QuerySolution> queryAnalogAndDigitalValues(Graph graph) {
        var model = ModelFactory.createModelForGraph(graph);

        var result = new ArrayList<QuerySolution>(100000);
        var stopWatch = StopWatch.createStarted();
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
        stopWatch.stop();
        System.out.println("Query took " + stopWatch.getTime() + "ms and returned " + result.size() + " results.");

        return result;
    }

    private List<QuerySolution> queryAnalogAndDigitalValuesWithPreparedQueries(Graph graph) {
        var datasetGraph = DatasetGraphFactory.wrap(graph);


        var result = new ArrayList<QuerySolution>(100000);
        var analogQuery = QueryFactory.create("""
                PREFIX meas: <http://www.fancyTSO.org/OurCIMModel/MEASv1#>
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                SELECT ?s ?analogValue ?timeStamp ?status  
                WHERE { ?s meas:AnalogValue.value ?analogValue;
                           meas:MeasurementValue.timeStamp ?timeStamp;
                           meas:MeasurementValue.status ?status.}
                """);
        var discreteQuery = QueryFactory.create("""
                PREFIX meas: <http://www.fancyTSO.org/OurCIMModel/MEASv1#>
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                SELECT ?s ?discreteValue ?timeStamp ?status  
                WHERE { ?s meas:DiscreteValue.value ?discreteValue;
                           meas:MeasurementValue.timeStamp ?timeStamp;
                           meas:MeasurementValue.status ?status.}
                """);

        var stopWatch = StopWatch.createStarted();

        var queryExecutionAnalog = QueryExecutionFactory.create(analogQuery, datasetGraph);
        var queryExecutionDiscrete = QueryExecutionFactory.create(discreteQuery, datasetGraph);

        queryExecutionAnalog.execSelect().forEachRemaining(result::add);
        queryExecutionDiscrete.execSelect().forEachRemaining(result::add);

        stopWatch.stop();
        System.out.println("Query took " + stopWatch.getTime() + "ms and returned " + result.size() + " results.");

        return result;
    }

    private void fillQueryResultsIntoLists(List<QuerySolution> result) {
        final var analogValues = new ArrayList<AnalogValue>(90000);
        final var discreteValues = new ArrayList<DiscreteValue>(10000);
        var stopWatch = StopWatch.createStarted();
        for (var querySolution : result) {
            if (querySolution.contains("analogValue")) {
                analogValues.add(new AnalogValue((float) querySolution.getLiteral("analogValue").getValue(), ((XSDDateTime) querySolution.getLiteral("timeStamp").getValue()).asCalendar().toInstant(), (int) querySolution.getLiteral("status").getValue()));
            } else {
                discreteValues.add(new DiscreteValue((int) querySolution.getLiteral("discreteValue").getValue(), ((XSDDateTime) querySolution.getLiteral("timeStamp").getValue()).asCalendar().toInstant(), (int) querySolution.getLiteral("status").getValue()));
            }
        }
        stopWatch.stop();
        System.out.println("Iterating over " + result.size() + " results took " + stopWatch.getTime() + "ms.");
    }

    @Test
    public void loadRDFSAndProfile() {
        /*TODO: Create JMH-Benchmark out of this to prevent the JVM from taking shortcuts */

        var g = createGraphWithMEASData(90000, 10000);

        var result = queryAnalogAndDigitalValues(g);
        result = queryAnalogAndDigitalValues(g);
        result = queryAnalogAndDigitalValues(g);
        result = queryAnalogAndDigitalValues(g);
        result = queryAnalogAndDigitalValues(g);

        fillQueryResultsIntoLists(result);
        fillQueryResultsIntoLists(result);
        fillQueryResultsIntoLists(result);
        fillQueryResultsIntoLists(result);
        fillQueryResultsIntoLists(result);

        result = queryAnalogAndDigitalValuesWithPreparedQueries(g);
        result = queryAnalogAndDigitalValuesWithPreparedQueries(g);
        result = queryAnalogAndDigitalValuesWithPreparedQueries(g);
        result = queryAnalogAndDigitalValuesWithPreparedQueries(g);
        result = queryAnalogAndDigitalValuesWithPreparedQueries(g);

        fillQueryResultsIntoLists(result);
        fillQueryResultsIntoLists(result);
        fillQueryResultsIntoLists(result);
        fillQueryResultsIntoLists(result);
        fillQueryResultsIntoLists(result);

        fillListsByGraph(g);
        fillListsByGraph(g);
        fillListsByGraph(g);
        fillListsByGraph(g);
        fillListsByGraph(g);

        int i = 0;
    }

    private void fillListsByGraph(Graph g) {
        final var analogValues = new ArrayList<AnalogValue>(90000);
        final var discreteValues = new ArrayList<DiscreteValue>(10000);
        var stopWatch = StopWatch.createStarted();
        g.stream(Node.ANY, AnalogValueValue.asNode(), Node.ANY)
                .forEach(triple -> {
                    final var s = triple.getSubject();
                    final var timeStamp = ((XSDDateTime) g.find(s, MeasurementValueTimeStamp.asNode(), Node.ANY).next().getObject().getLiteralValue()).asCalendar().toInstant();
                    final var status = (int) g.find(s, MeasurementValueStatus.asNode(), Node.ANY).next().getObject().getLiteralValue();
                    analogValues.add(new AnalogValue((float) triple.getObject().getLiteralValue(), timeStamp, status));
                });
        g.stream(Node.ANY, DiscreteValueValue.asNode(), Node.ANY)
                .forEach(triple -> {
                    final var s = triple.getSubject();
                    final var timeStamp = ((XSDDateTime) g.find(s, MeasurementValueTimeStamp.asNode(), Node.ANY).next().getObject().getLiteralValue()).asCalendar().toInstant();
                    final var status = (int) g.find(s, MeasurementValueStatus.asNode(), Node.ANY).next().getObject().getLiteralValue();
                    discreteValues.add(new DiscreteValue((int) triple.getObject().getLiteralValue(), timeStamp, status));
                });
        stopWatch.stop();
        System.out.println("Iterating over graph took " + stopWatch.getTime() + "ms and returned " + (analogValues.size() + discreteValues.size()) + " results.");
    }

    private static class NextStringInArrayProvider {
        private final String[] array;
        private int index = 0;

        public NextStringInArrayProvider(String[] array) {
            this.array = array;
        }

        public String next() {
            if (index == array.length) {
                index = 0;
            }
            return array[index++];
        }
    }

    private record AnalogValue(float value, Instant timeStamp, int status) {
    }

    private record DiscreteValue(int value, Instant timeStamp, int status) {
    }

}

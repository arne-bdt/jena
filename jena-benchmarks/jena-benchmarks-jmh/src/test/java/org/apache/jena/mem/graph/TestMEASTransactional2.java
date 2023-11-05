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

import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.graph.helper.JMHDefaultOptions;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.core.mem2.GraphWrapperTransactional2;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@State(Scope.Benchmark)
public class TestMEASTransactional2 {


    private static void updateAnalogAndDiscreteValues(GraphWrapperTransactional2 g, List<MEASData.AnalogValue> analogValues, List<MEASData.DiscreteValue> discreteValues) {
        updateAnalogAndDiscreteValues(g, analogValues, discreteValues, analogValues.size(), discreteValues.size());
    }

    private static void updateAnalogAndDiscreteValues(GraphWrapperTransactional2 g, List<MEASData.AnalogValue> analogValues, List<MEASData.DiscreteValue> discreteValues, int numAnalogValuesToUpdate, int numDiscreteValuesToUpdate) {
        boolean beginTransactionAndCommitIsNeeded = !g.isInTransaction();
        if(beginTransactionAndCommitIsNeeded) {
            g.begin(ReadWrite.WRITE);
        }
        for (final var analogValue : analogValues.subList(0, numAnalogValuesToUpdate)) {
            final var subject = NodeFactory.createURI(analogValue.uuid());

            final var tAnalogValueValue = g.find(subject, MEASData.AnalogValueValue.asNode(), Node.ANY).next();
            if(analogValue.value() != (Float)tAnalogValueValue.getObject().getLiteralValue()) {
                g.add(Triple.create(subject, tAnalogValueValue.getPredicate(), NodeFactory.createLiteralByValue(analogValue.value(), XSDDatatype.XSDfloat)));
                g.delete(tAnalogValueValue);
            }


            final var tMeasurementValueTimeStamp = g.find(subject, MEASData.MeasurementValueTimeStamp.asNode(), Node.ANY).next();
            final var newTimeStamp = NodeFactory.createLiteral(DateTimeFormatter.ISO_INSTANT.format(analogValue.timeStamp()), XSDDatatype.XSDdateTimeStamp);
            if(!tMeasurementValueTimeStamp.getObject().equals(newTimeStamp)) {
                g.add(Triple.create(subject, tMeasurementValueTimeStamp.getPredicate(), newTimeStamp));
                g.delete(tMeasurementValueTimeStamp);
            }


            final var tMeasurementValueStatus = g.find(subject, MEASData.MeasurementValueStatus.asNode(), Node.ANY).next();
            if(analogValue.status() != (Integer)tMeasurementValueStatus.getObject().getLiteralValue()) {
                g.add(Triple.create(subject, tMeasurementValueStatus.getPredicate(), NodeFactory.createLiteralByValue(analogValue.status(), XSDDatatype.XSDinteger)));
                g.delete(tMeasurementValueStatus);
            }
        }
        for (final var discreteValue : discreteValues.subList(0, numDiscreteValuesToUpdate)) {
            final var subject = NodeFactory.createURI(discreteValue.uuid());

            final var tDiscreteValueValue = g.find(subject, MEASData.DiscreteValueValue.asNode(), Node.ANY).next();
            if(discreteValue.value() != (Integer)tDiscreteValueValue.getObject().getLiteralValue()) {
                g.add(Triple.create(subject, tDiscreteValueValue.getPredicate(), NodeFactory.createLiteralByValue(discreteValue.value(), XSDDatatype.XSDinteger)));
                g.delete(tDiscreteValueValue);
            }

            final var tMeasurementValueTimeStamp = g.find(subject, MEASData.MeasurementValueTimeStamp.asNode(), Node.ANY).next();
            final var newTimeStamp = NodeFactory.createLiteral(DateTimeFormatter.ISO_INSTANT.format(discreteValue.timeStamp()), XSDDatatype.XSDdateTimeStamp);
            if(!tMeasurementValueTimeStamp.getObject().equals(newTimeStamp)) {
                g.add(Triple.create(subject, tMeasurementValueTimeStamp.getPredicate(), newTimeStamp));
                g.delete(tMeasurementValueTimeStamp);
            }

            final var tMeasurementValueStatus = g.find(subject, MEASData.MeasurementValueStatus.asNode(), Node.ANY).next();
            if(discreteValue.status() != (Integer)tMeasurementValueStatus.getObject().getLiteralValue()) {
                g.add(Triple.create(subject, tMeasurementValueStatus.getPredicate(), NodeFactory.createLiteralByValue(discreteValue.status(), XSDDatatype.XSDinteger)));
                g.delete(tMeasurementValueStatus);
            }
        }
        if(beginTransactionAndCommitIsNeeded) {
            g.commit();
        }
    }

    @Test
    public void testOneThreadUpdatingWithSeveralThreadsReading() throws InterruptedException {
        final var bulkUpdateRateInSeconds = 3;
        final var spontaneousUpdateRateInSeconds = 1;
        final var queryRateInSeconds = 1;
        final var numberOfSponataneousUpdateThreads = 12;
        final var numberOfQueryThreads = 8;
        final var numerOfSponataneousUpdatesPerSecond = 100;

        final var version = new AtomicInteger(0);
        final var versionTriple = Triple.create(NodeFactory.createURI("_" + UUID.randomUUID().toString()), NodeFactory.createLiteralByValue("jena.apache.org/jena-jmh-benchmarks#version"), NodeFactory.createLiteralByValue(version.intValue()));

        final var analogValues = MEASData.generateRandomAnalogValues(100000);
        final var discreteValues = MEASData.generateRandomDiscreteValues(25000);

        final var g = createGraph();
        g.begin(ReadWrite.WRITE);
        g.add(versionTriple);
        MEASData.addAnalogValuesToGraph(g, analogValues);
        MEASData.addDiscreteValuesToGraph(g, discreteValues);
        g.commit();

        final var overallStopwatch = StopWatch.createStarted();

        final var updateScheduler = Executors.newSingleThreadScheduledExecutor();
        var scheduledFutureForBulkUpdates = updateScheduler.scheduleAtFixedRate(() -> {
            try {
                final var stopwatch = StopWatch.createStarted();
                final var updatedAnalogValues = MEASData.getRandomlyUpdatedAnalogValues(analogValues);
                final var updatedDiscreteValues = MEASData.getRandomlyUpdatedDiscreteValues(discreteValues);
                g.begin(ReadWrite.WRITE);
                var verTriple = g.find(versionTriple.getSubject(), versionTriple.getPredicate(), Node.ANY).next();
                final var ver = (Integer)version.incrementAndGet();
                g.add(versionTriple.getSubject(), versionTriple.getPredicate(), NodeFactory.createLiteralByValue(ver));
                g.delete(verTriple);
                updateAnalogAndDiscreteValues(g, updatedAnalogValues, updatedDiscreteValues);
                g.commit();
                stopwatch.stop();
                //printf: Bulk-Updated from version X to Y in XX.XXXs
                //if(stopwatch.getTime(TimeUnit.MILLISECONDS) > bulkUpdateRateInSeconds*1000) {
                    System.out.printf("Bulk-Update from version %d to %d in %s (total time: %s)%n", verTriple.getObject().getLiteralValue(), ver, stopwatch, overallStopwatch);
                    g.printDeltaChainLengths();
                //}
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 0, bulkUpdateRateInSeconds, TimeUnit.SECONDS);


        final var spontaneousUpdateScheduler = Executors.newScheduledThreadPool(numberOfSponataneousUpdateThreads);
        final var spontaneousUpdateFutures = new ArrayList<ScheduledFuture>(numerOfSponataneousUpdatesPerSecond);

        for (int i = 0; i < numerOfSponataneousUpdatesPerSecond; i++) {
            final var threadNumber = Integer.toString(i);
            final var future = spontaneousUpdateScheduler.scheduleAtFixedRate(() -> {
                try {
                    final var stopwatch = StopWatch.createStarted();
                    final var updatedAnalogValues = MEASData.getRandomlyUpdatedAnalogValues(analogValues);
                    final var updatedDiscreteValues = MEASData.getRandomlyUpdatedDiscreteValues(discreteValues);
                    g.begin(ReadWrite.WRITE);
                    var verTriple = g.find(versionTriple.getSubject(), versionTriple.getPredicate(), Node.ANY).next();
                    final var ver = version.incrementAndGet();
                    g.add(versionTriple.getSubject(), versionTriple.getPredicate(), NodeFactory.createLiteralByValue(ver));
                    g.delete(verTriple);
                    updateAnalogAndDiscreteValues(g, updatedAnalogValues, updatedDiscreteValues, 80, 20);
                    g.commit();
                    stopwatch.stop();
                    if(stopwatch.getTime(TimeUnit.MILLISECONDS) > bulkUpdateRateInSeconds*1000) {
                        System.out.printf("Spontaneous-Update-Thread %s from version %d to %d in %s (total time: %s)%n", threadNumber, version.get(), ver, stopwatch, overallStopwatch);
                        g.printDeltaChainLengths();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, new Random().nextInt(0, spontaneousUpdateRateInSeconds*1000), spontaneousUpdateRateInSeconds*1000, TimeUnit.MILLISECONDS);
            spontaneousUpdateFutures.add(future);
        }

        final var queryFutures = new ArrayList<ScheduledFuture>(numberOfQueryThreads);
        final var queryScheduler = Executors.newScheduledThreadPool(numberOfQueryThreads);

        for (int i = 0; i < numberOfQueryThreads; i++) {
            final var threadNumber = Integer.toString(i);
            final var queryFuture = queryScheduler.scheduleAtFixedRate(() -> {
                try {
                    final var stopwatch = StopWatch.createStarted();
                    g.begin(ReadWrite.READ);
                    final var verTriple = g.find(versionTriple.getSubject(), versionTriple.getPredicate(), Node.ANY).next();
                    final var ver = (Integer) verTriple.getObject().getLiteralValue();
                    final var result = fillListsByGraph(g, analogValues.size(), discreteValues.size());
                    Assert.assertEquals(analogValues.size(), result.getLeft().size());
                    Assert.assertEquals(discreteValues.size(), result.getRight().size());
                    g.end();
                    stopwatch.stop();
                    //printf: Thread x reading version y in XX.XXXs
                    if(threadNumber.equals("0") || stopwatch.getTime(TimeUnit.MILLISECONDS) > queryRateInSeconds*1000) {
                        System.out.printf("Thread %s reading version %d in %s%n", threadNumber, ver, stopwatch);
                        g.printDeltaChainLengths();
                    }
                } catch (Exception e) {
                    try{
                        g.end();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    e.printStackTrace();
                }
            }, new Random().nextInt(0, queryRateInSeconds*1000), queryRateInSeconds*1000, TimeUnit.MILLISECONDS);
            queryFutures.add(queryFuture);
        }

        Thread.sleep(360000L);

        scheduledFutureForBulkUpdates.cancel(true);
        updateScheduler.shutdown();

        spontaneousUpdateFutures.forEach(future -> future.cancel(true));
        spontaneousUpdateScheduler.shutdown();

        queryFutures.forEach(future -> future.cancel(true));
        queryScheduler.shutdown();

        g.close();

//        return g;
    }


    public GraphWrapperTransactional2 createGraph() {
        return new GraphWrapperTransactional2(8);
    }

    private static Pair<List<MEASData.AnalogValue>, List<MEASData.DiscreteValue>> fillListsByGraph(GraphWrapperTransactional2 g, int totalAnalogValues, int totalDiscreteValues) {

        final var analogValues = new ArrayList<MEASData.AnalogValue>(totalAnalogValues);
        final var discreteValues = new ArrayList<MEASData.DiscreteValue>(totalDiscreteValues);
        boolean beginTransactionAndCommitIsNeeded = !g.isInTransaction();
        if(beginTransactionAndCommitIsNeeded) {
            g.begin(ReadWrite.READ);
        }
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
        if(beginTransactionAndCommitIsNeeded) {
            g.end();
        }
        return Pair.of(analogValues, discreteValues);
    }

    @Test
    public void benchmark() throws Exception {
        var opt = JMHDefaultOptions.getDefaults(this.getClass())
                .warmupIterations(0)
                .measurementIterations(15)
                .build();
        var results = new Runner(opt).run();
        Assert.assertNotNull(results);
    }

}

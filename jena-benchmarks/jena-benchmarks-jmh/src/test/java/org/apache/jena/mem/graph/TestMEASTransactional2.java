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
import org.apache.jena.mem.graph.helper.Serialization;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.core.mem2.GraphMem2Txn;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;

import java.math.BigDecimal;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@State(Scope.Benchmark)
public class TestMEASTransactional2 {



    private static int updateAnalogAndDiscreteValues(GraphMem2Txn g, List<MEASData.AnalogValue> analogValues, List<MEASData.DiscreteValue> discreteValues) {
        var updates = 0;
        boolean beginTransactionAndCommitIsNeeded = !g.isInTransaction();
        if(beginTransactionAndCommitIsNeeded) {
            g.begin(ReadWrite.WRITE);
        }
        for (final var analogValue : analogValues) {
            final var subject = NodeFactory.createURI(analogValue.uuid());

            final var tAnalogValueValue = g.find(subject, MEASData.AnalogValueValue.asNode(), Node.ANY).next();
            if(analogValue.value() != (Float)tAnalogValueValue.getObject().getLiteralValue()) {
                g.add(Triple.create(subject, tAnalogValueValue.getPredicate(), NodeFactory.createLiteralByValue(analogValue.value(), XSDDatatype.XSDfloat)));
                g.delete(tAnalogValueValue);
                updates++;
            }


            final var tMeasurementValueTimeStamp = g.find(subject, MEASData.MeasurementValueTimeStamp.asNode(), Node.ANY).next();
            final var newTimeStamp = NodeFactory.createLiteral(DateTimeFormatter.ISO_INSTANT.format(analogValue.timeStamp()), XSDDatatype.XSDdateTimeStamp);
            if(!tMeasurementValueTimeStamp.getObject().equals(newTimeStamp)) {
                g.add(Triple.create(subject, tMeasurementValueTimeStamp.getPredicate(), newTimeStamp));
                g.delete(tMeasurementValueTimeStamp);
                updates++;
            }


            final var tMeasurementValueStatus = g.find(subject, MEASData.MeasurementValueStatus.asNode(), Node.ANY).next();
            if(analogValue.status() != (Integer)tMeasurementValueStatus.getObject().getLiteralValue()) {
                g.add(Triple.create(subject, tMeasurementValueStatus.getPredicate(), NodeFactory.createLiteralByValue(analogValue.status(), XSDDatatype.XSDinteger)));
                g.delete(tMeasurementValueStatus);
                updates++;
            }
        }
        for (final var discreteValue : discreteValues) {
            final var subject = NodeFactory.createURI(discreteValue.uuid());

            final var tDiscreteValueValue = g.find(subject, MEASData.DiscreteValueValue.asNode(), Node.ANY).next();
            if(discreteValue.value() != (Integer)tDiscreteValueValue.getObject().getLiteralValue()) {
                g.add(Triple.create(subject, tDiscreteValueValue.getPredicate(), NodeFactory.createLiteralByValue(discreteValue.value(), XSDDatatype.XSDinteger)));
                g.delete(tDiscreteValueValue);
                updates++;
            }

            final var tMeasurementValueTimeStamp = g.find(subject, MEASData.MeasurementValueTimeStamp.asNode(), Node.ANY).next();
            final var newTimeStamp = NodeFactory.createLiteral(DateTimeFormatter.ISO_INSTANT.format(discreteValue.timeStamp()), XSDDatatype.XSDdateTimeStamp);
            if(!tMeasurementValueTimeStamp.getObject().equals(newTimeStamp)) {
                g.add(Triple.create(subject, tMeasurementValueTimeStamp.getPredicate(), newTimeStamp));
                g.delete(tMeasurementValueTimeStamp);
                updates++;
            }

            final var tMeasurementValueStatus = g.find(subject, MEASData.MeasurementValueStatus.asNode(), Node.ANY).next();
            if(discreteValue.status() != (Integer)tMeasurementValueStatus.getObject().getLiteralValue()) {
                g.add(Triple.create(subject, tMeasurementValueStatus.getPredicate(), NodeFactory.createLiteralByValue(discreteValue.status(), XSDDatatype.XSDinteger)));
                g.delete(tMeasurementValueStatus);
                updates++;
            }
        }
        if(beginTransactionAndCommitIsNeeded) {
            g.commit();
        }
        return updates;
    }

    /**
     * This method is used to get the memory consumption of the current JVM.
     *
     * @return the memory consumption in MB
     */
    private static double runGcAndGetUsedMemoryInMB() {
        System.gc();
        Runtime.getRuntime().gc();
        return BigDecimal.valueOf(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()).divide(BigDecimal.valueOf(1024L)).divide(BigDecimal.valueOf(1024L)).doubleValue();
    }

    @Test
    public void testSerializeAndDeserializeMeasurements() throws InterruptedException {
        final var analogValues = MEASData.generateRandomAnalogValues(100000);
        final var discreteValues = MEASData.generateRandomDiscreteValues(25000);

        final StopWatch stopwatch;
        final Serialization.SerializedData serialized;
        {
            final var memoryBefore = runGcAndGetUsedMemoryInMB();
            final var g = createGraph();
            g.begin(ReadWrite.WRITE);
            MEASData.addAnalogValuesToGraph(g, analogValues);
            MEASData.addDiscreteValuesToGraph(g, discreteValues);
            g.commit();
            final var memoryAfter = runGcAndGetUsedMemoryInMB();
            //print additional memory needed to fill graph
            System.out.printf("Additional memory needed to fill graph with %d analog values and %d discrete values: %,.2f MB%n",
                    analogValues.size(),
                    discreteValues.size(),
                    (memoryAfter - memoryBefore));

            stopwatch = StopWatch.createStarted();
            g.begin(ReadWrite.READ);
            serialized = Serialization.serialize(g, RDFFormat.RDF_THRIFT, Serialization.LZ4_FASTEST);
            g.end();
            g.close();
        }
        stopwatch.stop();
        // print size of serialized graph
        System.out.printf("Serialized graph with %d analog values and %d discrete values: %,.2f MB%n",
                analogValues.size(),
                discreteValues.size(),
                serialized.bytes().length / 1024.0 / 1024.0);
        //print time to serialize graph
        System.out.printf("Serializing graph with %d analog values and %d discrete values took %s%n",
                analogValues.size(),
                discreteValues.size(),
                stopwatch);

        stopwatch.reset();
        final var memoryBefore = runGcAndGetUsedMemoryInMB();
        stopwatch.start();
        var deserialized = Serialization.deserialize(serialized, false);
        stopwatch.stop();
        final var memoryAfter = runGcAndGetUsedMemoryInMB();
        //print time to deserialize graph
        System.out.printf("Deserializing graph with %d analog values and %d discrete values took %s%n",
                analogValues.size(),
                discreteValues.size(),
                stopwatch);
        //print additional memory needed to deserialize graph
        System.out.printf("Additional memory needed to deserialize graph with %d analog values and %d discrete values: %,.2f MB%n",
                analogValues.size(),
                discreteValues.size(),
                (memoryAfter - memoryBefore));
        Assert.assertEquals(analogValues.size(), deserialized.stream(Node.ANY, MEASData.AnalogValueValue.asNode(), Node.ANY).count());
        Assert.assertEquals(discreteValues.size(), deserialized.stream(Node.ANY, MEASData.DiscreteValueValue.asNode(), Node.ANY).count());
    }


    @Test
    public void testMultipleThreadsUpdatingWithSeveralThreadsReading() throws InterruptedException {
        final var bulkUpdateRateInSeconds = 3;
        final var spontaneousUpdateRateInSeconds = 1;
        final var queryRateInSeconds = 1;
        final var numberOfSpontaneousUpdateThreads = 4;
        final var numberOfQueryThreads = 8;
        final var numberOfSpontaneousUpdatesPerSecond = 100;

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
                if(stopwatch.getTime(TimeUnit.MILLISECONDS) > bulkUpdateRateInSeconds*1000) {
                    System.out.printf("Bulk-Update from version %d to %d in %s (total time: %s)%n", verTriple.getObject().getLiteralValue(), ver, stopwatch, overallStopwatch);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 0, bulkUpdateRateInSeconds, TimeUnit.SECONDS);


        final var spontaneousUpdateScheduler = Executors.newScheduledThreadPool(numberOfSpontaneousUpdateThreads);
        final var spontaneousUpdateFutures = new ArrayList<ScheduledFuture>(numberOfSpontaneousUpdatesPerSecond);

        for (int i = 0; i < numberOfSpontaneousUpdatesPerSecond; i++) {
            final var threadNumber = Integer.toString(i);
            final var future = spontaneousUpdateScheduler.scheduleAtFixedRate(() -> {
                try {
                    final var stopwatch = StopWatch.createStarted();
                    final var updatedAnalogValues = MEASData.getRandomlyUpdatedAnalogValues(analogValues, 80);
                    final var updatedDiscreteValues = MEASData.getRandomlyUpdatedDiscreteValues(discreteValues, 20);
                    g.begin(ReadWrite.WRITE);
                    var verTriple = g.find(versionTriple.getSubject(), versionTriple.getPredicate(), Node.ANY).next();
                    final var ver = version.incrementAndGet();
                    g.add(versionTriple.getSubject(), versionTriple.getPredicate(), NodeFactory.createLiteralByValue(ver));
                    g.delete(verTriple);
                    updateAnalogAndDiscreteValues(g, updatedAnalogValues, updatedDiscreteValues);
                    g.commit();
                    stopwatch.stop();
                    if(stopwatch.getTime(TimeUnit.MILLISECONDS) > bulkUpdateRateInSeconds*1000) {
                        System.out.printf("Spontaneous-Update-Thread %s from version %d to %d in %s (total time: %s)%n", threadNumber, version.get(), ver, stopwatch, overallStopwatch);
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
                        System.out.printf("Thread %s reading version %d in %s (total time: %s)%n", threadNumber, ver, stopwatch, overallStopwatch);
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

    private record UpdatesToApply(List<MEASData.AnalogValue> analogValues, List<MEASData.DiscreteValue> discreteValues) {}

    @Test
    public void testOneThreadUpdatingWithSeveralThreadsReading() throws InterruptedException {
        final var bulkUpdateRateInSeconds = 3;
        final var spontaneousUpdateRateInSeconds = 1;
        final var queryRateInSeconds = 1;
        final var numberOfSpontaneousUpdateThreads = 6;
        final var numberOfQueryThreads = 6;
        final var numberOfSpontaneousUpdatesPerSecond = 500;

        final var updateCounter = new AtomicLong(0);

        final var version = new AtomicInteger(0);
        final var versionTriple = Triple.create(NodeFactory.createURI("_" + UUID.randomUUID().toString()), NodeFactory.createLiteralByValue("jena.apache.org/jena-jmh-benchmarks#version"), NodeFactory.createLiteralByValue(version.intValue()));

        final var analogValues = MEASData.generateRandomAnalogValues(100000);
        final var discreteValues = MEASData.generateRandomDiscreteValues(25000);

        final var g = new GraphMem2Txn(2);
        g.begin(ReadWrite.WRITE);
        g.add(versionTriple);
        MEASData.addAnalogValuesToGraph(g, analogValues);
        MEASData.addDiscreteValuesToGraph(g, discreteValues);
        g.commit();

        final ConcurrentLinkedQueue<UpdatesToApply> updateQueue = new ConcurrentLinkedQueue<>();

        final var overallStopwatch = StopWatch.createStarted();
        final var updatesRunning = new AtomicBoolean(true);

        final var updateThread = new Thread(() -> {
            try {
                while(updatesRunning.get()) {
                    while(!updateQueue.isEmpty()) {
                        var update = updateQueue.poll();
                        g.begin(ReadWrite.WRITE);
                        var verTriple = g.find(versionTriple.getSubject(), versionTriple.getPredicate(), Node.ANY).next();
                        final var ver = (Integer)version.incrementAndGet();
                        g.add(versionTriple.getSubject(), versionTriple.getPredicate(), NodeFactory.createLiteralByValue(ver));
                        g.delete(verTriple);
                        final var numOfUpdates = updateAnalogAndDiscreteValues(g, update.analogValues(), update.discreteValues()) + 1;
                        g.commit();
                        updateCounter.getAndAdd(numOfUpdates);
                    }
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
        updateThread.start();

        final var updateScheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory());
        var scheduledFutureForBulkUpdates = updateScheduler.scheduleAtFixedRate(() -> {
            try {
                updateQueue.add(
                        new UpdatesToApply(
                                MEASData.getRandomlyUpdatedAnalogValues(analogValues),
                                MEASData.getRandomlyUpdatedDiscreteValues(discreteValues)));

            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 0, bulkUpdateRateInSeconds, TimeUnit.SECONDS);


        final var spontaneousUpdateScheduler = Executors.newScheduledThreadPool(numberOfSpontaneousUpdateThreads, Thread.ofVirtual().factory());
        final var spontaneousUpdateFutures = new ArrayList<ScheduledFuture>(numberOfSpontaneousUpdatesPerSecond);

        for (int i = 0; i < numberOfSpontaneousUpdatesPerSecond; i++) {
            final var future = spontaneousUpdateScheduler.scheduleAtFixedRate(() -> {
                try {
                    updateQueue.add(
                            new UpdatesToApply(
                                    MEASData.getRandomlyUpdatedAnalogValues(analogValues, 80),
                                    MEASData.getRandomlyUpdatedDiscreteValues(discreteValues, 20)));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, new Random().nextInt(0, spontaneousUpdateRateInSeconds*1000), spontaneousUpdateRateInSeconds*1000, TimeUnit.MILLISECONDS);
            spontaneousUpdateFutures.add(future);
        }

        final var queryFutures = new ArrayList<ScheduledFuture>(numberOfQueryThreads);
        final var queryScheduler = Executors.newScheduledThreadPool(numberOfQueryThreads, Thread.ofVirtual().factory());

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
                        System.out.printf("Thread %s reading version %d in %s (total time: %s, update-queue-length: %d, updated-rate: %d t/s)%n", threadNumber, ver, stopwatch, overallStopwatch, updateQueue.size(), (int)((updateCounter.get()/(double)overallStopwatch.getTime(TimeUnit.MILLISECONDS)*1000.0)));
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

        updatesRunning.set(false);
        updateThread.join(3000L);

        if(updateThread.isAlive()) {
            updateThread.interrupt();
        }

        scheduledFutureForBulkUpdates.cancel(true);
        updateScheduler.shutdown();

        spontaneousUpdateFutures.forEach(future -> future.cancel(true));
        spontaneousUpdateScheduler.shutdown();

        queryFutures.forEach(future -> future.cancel(true));
        queryScheduler.shutdown();

        g.close();

//        return g;
    }


    public GraphMem2Txn createGraph() {
        return new GraphMem2Txn();
    }

    private static Pair<List<MEASData.AnalogValue>, List<MEASData.DiscreteValue>> fillListsByGraph(GraphMem2Txn g, int totalAnalogValues, int totalDiscreteValues) {

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
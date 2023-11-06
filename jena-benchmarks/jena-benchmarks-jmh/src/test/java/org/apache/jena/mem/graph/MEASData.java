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

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Graph;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.vocabulary.RDF;

import java.time.Clock;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class MEASData {

    public static final String MEAS_NS = "http://www.fancyTSO.org/OurCIMModel/MEASv1#";

    private static final Model m = ModelFactory.createDefaultModel();

    public static final Property AnalogValueAnalog = m.createProperty(MEAS_NS + "AnalogValue.analog");
    public static final Property DiscreteValueDiscrete = m.createProperty(MEAS_NS + "AnalogValue.discrete");

    public static Random random = new Random();

    public static List<AnalogValue> generateRandomAnalogValues(int numberOfAnalogValues) {
        final var list = new ArrayList<AnalogValue>(numberOfAnalogValues);
        for (int i = 0; i < numberOfAnalogValues; i++) {
            list.add(new AnalogValue("_" + UUID.randomUUID(), random.nextFloat(), Clock.systemUTC().instant(), random.nextInt()));
        }
        return list;
    }

    public static List<DiscreteValue> generateRandomDiscreteValues(int numberOfDiscreteValues) {
        final var list = new ArrayList<DiscreteValue>(numberOfDiscreteValues);
        for (int i = 0; i < numberOfDiscreteValues; i++) {
            list.add(new DiscreteValue("_" + UUID.randomUUID(), random.nextInt(), Clock.systemUTC().instant(), random.nextInt()));
        }
        return list;
    }

    public static float getRandomUnequalFloat(float value) {
        float newValue;
        do {
            newValue = random.nextFloat();
        } while (newValue == value);
        return newValue;
    }

    public static int getRandomUnequalInt(int value) {
        int newValue;
        do {
            newValue = random.nextInt();
        } while (newValue == value);
        return newValue;
    }

    private static Set<Integer> getRandomIndices(Random random, int size, int numberOfIndices) {
        final var indices = new HashSet<Integer>(numberOfIndices);
        while (indices.size() < numberOfIndices) {
            indices.add(random.nextInt(size));
        }
        return indices;
    }

    public static List<AnalogValue> getRandomlyUpdatedAnalogValues(List<AnalogValue> analogValues, final int numberOfUpdates) {
        final var random = new Random();
        final var newValues = new ArrayList<AnalogValue>(numberOfUpdates);
        final var indices = getRandomIndices(random, analogValues.size(), numberOfUpdates);
        for (final var index : indices) {
            final var analogValue = analogValues.get(index);
            newValues.add(new AnalogValue(analogValue.uuid(), getRandomUnequalFloat(analogValue.value), Clock.systemUTC().instant(), getRandomUnequalInt(analogValue.status)));
        }
        // Shuffle is important because the order might play a role. We want to test the performance of the
        // contains method regardless of the order
        Collections.shuffle(newValues, random);
        return newValues;
    }

    public static List<AnalogValue> getRandomlyUpdatedAnalogValues(List<AnalogValue> analogValues) {
        final var random = new Random();
        final var newValues = new ArrayList<AnalogValue>(analogValues.size());
        for (final var analogValue : analogValues) {
            newValues.add(new AnalogValue(analogValue.uuid(), getRandomUnequalFloat(analogValue.value), Clock.systemUTC().instant(), getRandomUnequalInt(analogValue.status)));
        }
        // Shuffle is important because the order might play a role. We want to test the performance of the
        // contains method regardless of the order
        Collections.shuffle(newValues, random);
        return newValues;
    }

    public static List<DiscreteValue> getRandomlyUpdatedDiscreteValues(List<DiscreteValue> discreteValues, final int numberOfUpdates) {
        final var random = new Random();
        final var newValues = new ArrayList<DiscreteValue>(numberOfUpdates);
        final var indices = getRandomIndices(random, discreteValues.size(), numberOfUpdates);
        for (final var index : indices) {
            final var discreteValue = discreteValues.get(index);
            newValues.add(new DiscreteValue(discreteValue.uuid(), getRandomUnequalInt(discreteValue.value), Clock.systemUTC().instant(), getRandomUnequalInt(discreteValue.status)));
        }
        // Shuffle is important because the order might play a role. We want to test the performance of the
        // contains method regardless of the order
        Collections.shuffle(newValues, random);
        return newValues;
    }

    public static List<DiscreteValue> getRandomlyUpdatedDiscreteValues(List<DiscreteValue> discreteValues) {
        final var newValues = new ArrayList<DiscreteValue>(discreteValues.size());
        for (final var discreteValue : discreteValues) {
            newValues.add(new DiscreteValue(discreteValue.uuid(), getRandomUnequalInt(discreteValue.value), Clock.systemUTC().instant(), getRandomUnequalInt(discreteValue.status)));
        }
        // Shuffle is important because the order might play a role. We want to test the performance of the
        // contains method regardless of the order
        Collections.shuffle(newValues, random);
        return newValues;
    }

    public static void addAnalogValuesToGraph(final Graph graph, final List<AnalogValue> analogValues) {
        final var model = ModelFactory.createModelForGraph(graph);

        final var analogTypeProvider = new NextStringInArrayProvider(AnalogTypes);
        for (final var analogValue : analogValues) {
            model.createResource(analogValue.uuid())
                    .addProperty(RDF.type, model.createResource(MEAS_NS + analogTypeProvider.next()))
                    .addProperty(AnalogValueAnalog, model.createResource("_" + UUID.randomUUID()))
                    .addProperty(MeasurementValueTimeStamp, DateTimeFormatter.ISO_INSTANT.format(analogValue.timeStamp()), XSDDatatype.XSDdateTimeStamp)
                    .addProperty(MeasurementValueStatus, Integer.toString(analogValue.status()), XSDDatatype.XSDinteger)
                    .addProperty(AnalogValueValue, Float.toString(analogValue.value()), XSDDatatype.XSDfloat);
        }
    }
    public static final Property AnalogValueValue = m.createProperty(MEAS_NS + "AnalogValue.value");

    public static void addDiscreteValuesToGraph(final Graph graph, final List<DiscreteValue> discreteValues) {
        final var model = ModelFactory.createModelForGraph(graph);

        final var discreteTypeProvider = new NextStringInArrayProvider(DiscreteTypes);
        for (final var discreteValue : discreteValues) {
            model.createResource(discreteValue.uuid())
                    .addProperty(RDF.type, model.createResource(MEAS_NS + discreteTypeProvider.next()))
                    .addProperty(DiscreteValueDiscrete, model.createResource("_" + UUID.randomUUID()))
                    .addProperty(MeasurementValueTimeStamp, DateTimeFormatter.ISO_INSTANT.format(discreteValue.timeStamp()), XSDDatatype.XSDdateTimeStamp)
                    .addProperty(MeasurementValueStatus, Integer.toString(discreteValue.status()), XSDDatatype.XSDinteger)
                    .addProperty(DiscreteValueValue, Integer.toString(discreteValue.value()), XSDDatatype.XSDinteger);
        }
    }

    public record AnalogValue(String uuid, float value, Instant timeStamp, int status) {
    }

    public record DiscreteValue(String uuid, int value, Instant timeStamp, int status) {
    }
    public static final Property DiscreteValueValue = m.createProperty(MEAS_NS + "DiscreteValue.value");
    public static final Property MeasurementValueTimeStamp = m.createProperty(MEAS_NS + "MeasurementValue.timeStamp");
    public static final Property MeasurementValueStatus = m.createProperty(MEAS_NS + "MeasurementValue.status");
    private static final String[] AnalogTypes = {"SomeAnalog", "ActivePowerAnalog", "ReactivePowerAnalog", "VoltageAnalog", "PhaseAngleAnalog", "GlobalRadiationAnalog", "HumidityAnalog", "TemperatureAnalog", "WindSpeedAnalog", "WindDirectionAnalog", "FrequencyAnalog", "PowerFactorAnalog", "CurrentAnalog"};
    private static final String[] DiscreteTypes = {"PhaseChangerStep", "BreakerStatus", "SwitchStatus", "TapChangerStatus", "TapChangerStep", "TapChangerControlMode", "TapChangerNeutralStatus", "TapChangerKind", "TapChangerMode", "TapChangerControlKind"};

//    public static void fillGraphWithMEASData(final Graph graph, final int numberOfAnalogValues, final int numberOfDiscreteValues) {
//        final var model = ModelFactory.createModelForGraph(graph);
//
//        final var random = new Random(4711);
//
//        final var analogTypeProvider = new NextStringInArrayProvider(AnalogTypes);
//        for (int i = 0; i < numberOfAnalogValues; i++) {
//            model.createResource("_" + UUID.randomUUID())
//                    .addProperty(RDF.type, model.createResource(MEAS_NS + analogTypeProvider.next()))
//                    .addProperty(MeasurementValueTimeStamp, DateTimeFormatter.ISO_INSTANT.format(Clock.systemUTC().instant()), XSDDatatype.XSDdateTimeStamp)
//                    .addProperty(MeasurementValueStatus, Integer.toString(random.nextInt()), XSDDatatype.XSDinteger)
//                    .addProperty(AnalogValueValue, Float.toString(random.nextFloat()), XSDDatatype.XSDfloat);
//        }
//
//        final var discreteTypeProvider = new NextStringInArrayProvider(DiscreteTypes);
//        for (int i = 0; i < numberOfDiscreteValues; i++) {
//            model.createResource("_" + UUID.randomUUID())
//                    .addProperty(RDF.type, model.createResource(MEAS_NS + discreteTypeProvider.next()))
//                    .addProperty(MeasurementValueTimeStamp, DateTimeFormatter.ISO_INSTANT.format(Clock.systemUTC().instant()), XSDDatatype.XSDdateTimeStamp)
//                    .addProperty(MeasurementValueStatus, Integer.toString(random.nextInt()), XSDDatatype.XSDinteger)
//                    .addProperty(DiscreteValueValue, Integer.toString(random.nextInt()), XSDDatatype.XSDinteger);
//        }
//    }


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
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *   SPDX-License-Identifier: Apache-2.0
 */

package org.apache.jena.sparql.core.mem.txn.helper;

import java.time.Clock;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.TxnType;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.vocabulary.RDF;

/**
 * Generates synthetic CIM-style measurement data for the transactional-graph
 * JMH benchmarks under {@code sparql.core.mem.txn}.
 * <p>
 * The data model is a deliberately simplified slice of the IEC CIM
 * {@code MEAS} package: every {@link AnalogValue} or {@link DiscreteValue}
 * becomes a small fan-out of triples (rdf:type, link to the parent
 * Analog/Discrete resource, timestamp, status, numeric value) — five triples
 * per value when added through {@link #addAnalogValuesToGraph} or
 * {@link #addDiscreteValuesToGraph}, and the three mutable triples
 * (timestamp, status, value) when produced by the {@code *ToTriplesForUpdate}
 * helpers.
 * <p>
 * The helper deliberately uses a single shared {@link Random} so a benchmark
 * run is internally consistent. The state is not thread-safe; benchmarks
 * generate their data on a single setup thread before forking workers.
 */
public final class MEASData {

    /** Namespace for the synthetic CIM MEAS vocabulary used by these benchmarks. */
    public static final String MEAS_NS = "http://www.fancyTSO.org/OurCIMModel/MEASv1#";

    public static final Property AnalogValueAnalog        = property("AnalogValue.analog");
    public static final Property AnalogValueValue         = property("AnalogValue.value");
    public static final Property DiscreteValueDiscrete    = property("DiscreteValue.discrete");
    public static final Property DiscreteValueValue       = property("DiscreteValue.value");
    public static final Property MeasurementValueTimeStamp = property("MeasurementValue.timeStamp");
    public static final Property MeasurementValueStatus   = property("MeasurementValue.status");

    /** Rotated through when assigning rdf:type to generated AnalogValue resources. */
    private static final String[] ANALOG_TYPES = {
            "SomeAnalog", "ActivePowerAnalog", "ReactivePowerAnalog", "VoltageAnalog",
            "PhaseAngleAnalog", "GlobalRadiationAnalog", "HumidityAnalog", "TemperatureAnalog",
            "WindSpeedAnalog", "WindDirectionAnalog", "FrequencyAnalog", "PowerFactorAnalog",
            "CurrentAnalog"
    };

    /** Rotated through when assigning rdf:type to generated DiscreteValue resources. */
    private static final String[] DISCRETE_TYPES = {
            "PhaseChangerStep", "BreakerStatus", "SwitchStatus", "TapChangerStatus",
            "TapChangerStep", "TapChangerControlMode", "TapChangerNeutralStatus",
            "TapChangerKind", "TapChangerMode", "TapChangerControlKind"
    };

    private static final Random RANDOM = new Random();

    private MEASData() {}

    private static Property property(String localName) {
        return ResourceFactory.createProperty(MEAS_NS + localName);
    }

    /** A single analog measurement: continuous (float) value with a status flag and timestamp. */
    public record AnalogValue(String uuid, float value, Instant timeStamp, int status) {}

    /** A single discrete measurement: integer-coded value with a status flag and timestamp. */
    public record DiscreteValue(String uuid, int value, Instant timeStamp, int status) {}

    // ----- Random generation --------------------------------------------------

    /** Build {@code n} random AnalogValues with fresh UUIDs and the current instant. */
    public static List<AnalogValue> generateRandomAnalogValues(int numberOfAnalogValues) {
        final var list = new ArrayList<AnalogValue>(numberOfAnalogValues);
        for (int i = 0; i < numberOfAnalogValues; i++) {
            list.add(new AnalogValue(
                    UUID.randomUUID().toString(),
                    RANDOM.nextFloat(),
                    Clock.systemUTC().instant(),
                    RANDOM.nextInt()));
        }
        return list;
    }

    /** Build {@code n} random DiscreteValues with fresh UUIDs and the current instant. */
    public static List<DiscreteValue> generateRandomDiscreteValues(int numberOfDiscreteValues) {
        final var list = new ArrayList<DiscreteValue>(numberOfDiscreteValues);
        for (int i = 0; i < numberOfDiscreteValues; i++) {
            list.add(new DiscreteValue(
                    UUID.randomUUID().toString(),
                    RANDOM.nextInt(),
                    Clock.systemUTC().instant(),
                    RANDOM.nextInt()));
        }
        return list;
    }

    /**
     * Picks {@code numberOfUpdates} random AnalogValues from {@code analogValues}
     * (without replacement) and returns updated copies — same UUID, but
     * value, status and timestamp replaced with new, distinct values.
     * <p>
     * The returned list is shuffled so callers cannot rely on it being in
     * the original order; benchmark numbers that depend on key locality
     * should not creep in.
     */
    public static List<AnalogValue> getRandomlyUpdatedAnalogValues(List<AnalogValue> analogValues,
                                                                   final int numberOfUpdates) {
        final var newValues = new ArrayList<AnalogValue>(numberOfUpdates);
        for (final var index : pickIndices(analogValues.size(), numberOfUpdates)) {
            final var av = analogValues.get(index);
            newValues.add(new AnalogValue(
                    av.uuid(),
                    randomFloatDifferentFrom(av.value()),
                    Clock.systemUTC().instant(),
                    randomIntDifferentFrom(av.status())));
        }
        Collections.shuffle(newValues, RANDOM);
        return newValues;
    }

    /**
     * Picks {@code numberOfUpdates} random DiscreteValues from {@code discreteValues}
     * (without replacement) and returns updated copies — same UUID, but
     * value, status and timestamp replaced with new, distinct values.
     * <p>
     * The returned list is shuffled; see {@link #getRandomlyUpdatedAnalogValues}.
     */
    public static List<DiscreteValue> getRandomlyUpdatedDiscreteValues(List<DiscreteValue> discreteValues,
                                                                      final int numberOfUpdates) {
        final var newValues = new ArrayList<DiscreteValue>(numberOfUpdates);
        for (final var index : pickIndices(discreteValues.size(), numberOfUpdates)) {
            final var dv = discreteValues.get(index);
            newValues.add(new DiscreteValue(
                    dv.uuid(),
                    randomIntDifferentFrom(dv.value()),
                    Clock.systemUTC().instant(),
                    randomIntDifferentFrom(dv.status())));
        }
        Collections.shuffle(newValues, RANDOM);
        return newValues;
    }

    // ----- Triple conversion --------------------------------------------------

    /**
     * Convert AnalogValues to the three triples (timestamp, status, value) that
     * a periodic refresh would rewrite. The static graph shape — rdf:type and
     * link to the parent Analog resource — is produced once by
     * {@link #addAnalogValuesToGraph} and not repeated here.
     */
    public static List<Triple> analogValuesToTriplesForUpdate(final List<AnalogValue> analogValues) {
        final var triples = new ArrayList<Triple>(analogValues.size() * 3);
        for (final var av : analogValues) {
            final var subject = NodeFactory.createURI("urn:uuid:" + av.uuid());
            triples.add(Triple.create(subject, MeasurementValueTimeStamp.asNode(),
                    NodeFactory.createLiteralDT(DateTimeFormatter.ISO_INSTANT.format(av.timeStamp()),
                            XSDDatatype.XSDdateTimeStamp)));
            triples.add(Triple.create(subject, MeasurementValueStatus.asNode(),
                    NodeFactory.createLiteralByValue(av.status(), XSDDatatype.XSDinteger)));
            triples.add(Triple.create(subject, AnalogValueValue.asNode(),
                    NodeFactory.createLiteralByValue(av.value(), XSDDatatype.XSDfloat)));
        }
        return triples;
    }

    /**
     * Convert DiscreteValues to the three triples (timestamp, status, value) that
     * a periodic refresh would rewrite. Analogue of
     * {@link #analogValuesToTriplesForUpdate}.
     */
    public static List<Triple> discreteValuesToTriplesForUpdate(final List<DiscreteValue> discreteValues) {
        final var triples = new ArrayList<Triple>(discreteValues.size() * 3);
        for (final var dv : discreteValues) {
            final var subject = NodeFactory.createURI("urn:uuid:" + dv.uuid());
            triples.add(Triple.create(subject, MeasurementValueTimeStamp.asNode(),
                    NodeFactory.createLiteralDT(DateTimeFormatter.ISO_INSTANT.format(dv.timeStamp()),
                            XSDDatatype.XSDdateTimeStamp)));
            triples.add(Triple.create(subject, MeasurementValueStatus.asNode(),
                    NodeFactory.createLiteralByValue(dv.status(), XSDDatatype.XSDinteger)));
            triples.add(Triple.create(subject, DiscreteValueValue.asNode(),
                    NodeFactory.createLiteralByValue(dv.value(), XSDDatatype.XSDinteger)));
        }
        return triples;
    }

    // ----- Bulk-load helpers --------------------------------------------------

    /**
     * Bulk-load {@code analogValues} into {@code graph}. For each value five
     * triples are written: rdf:type, link to a freshly minted Analog resource,
     * timestamp, status and the float value. If {@code graph} is
     * {@link Transactional} the load runs inside a single {@code WRITE}
     * transaction; otherwise the triples are added directly.
     */
    public static void addAnalogValuesToGraph(final Graph graph, final List<AnalogValue> analogValues) {
        final Transactional txn = (graph instanceof Transactional t) ? t : null;
        if (txn != null) {
            txn.begin(TxnType.WRITE);
        }
        try {
            final var model = ModelFactory.createModelForGraph(graph);
            final var analogTypeProvider = new RoundRobinStrings(ANALOG_TYPES);
            for (final var av : analogValues) {
                model.createResource("urn:uuid:" + av.uuid())
                        .addProperty(RDF.type, model.createResource(MEAS_NS + analogTypeProvider.next()))
                        .addProperty(AnalogValueAnalog, model.createResource("urn:uuid:" + UUID.randomUUID()))
                        .addProperty(MeasurementValueTimeStamp,
                                DateTimeFormatter.ISO_INSTANT.format(av.timeStamp()), XSDDatatype.XSDdateTimeStamp)
                        .addProperty(MeasurementValueStatus,
                                Integer.toString(av.status()), XSDDatatype.XSDinteger)
                        .addProperty(AnalogValueValue,
                                Float.toString(av.value()), XSDDatatype.XSDfloat);
            }
            if (txn != null) {
                txn.commit();
            }
        } finally {
            if (txn != null) {
                txn.end();
            }
        }
    }

    /**
     * Bulk-load {@code discreteValues} into {@code graph}. Symmetric to
     * {@link #addAnalogValuesToGraph} but using the discrete vocabulary and
     * integer values.
     */
    public static void addDiscreteValuesToGraph(final Graph graph, final List<DiscreteValue> discreteValues) {
        final Transactional txn = (graph instanceof Transactional t) ? t : null;
        if (txn != null) {
            txn.begin(TxnType.WRITE);
        }
        try {
            final var model = ModelFactory.createModelForGraph(graph);
            final var discreteTypeProvider = new RoundRobinStrings(DISCRETE_TYPES);
            for (final var dv : discreteValues) {
                model.createResource("urn:uuid:" + dv.uuid())
                        .addProperty(RDF.type, model.createResource(MEAS_NS + discreteTypeProvider.next()))
                        .addProperty(DiscreteValueDiscrete, model.createResource("urn:uuid:" + UUID.randomUUID()))
                        .addProperty(MeasurementValueTimeStamp,
                                DateTimeFormatter.ISO_INSTANT.format(dv.timeStamp()), XSDDatatype.XSDdateTimeStamp)
                        .addProperty(MeasurementValueStatus,
                                Integer.toString(dv.status()), XSDDatatype.XSDinteger)
                        .addProperty(DiscreteValueValue,
                                Integer.toString(dv.value()), XSDDatatype.XSDinteger);
            }
            if (txn != null) {
                txn.commit();
            }
        } finally {
            if (txn != null) {
                txn.end();
            }
        }
    }

    // ----- Internal helpers ---------------------------------------------------

    /** Returns {@code count} distinct indices uniformly drawn from {@code [0, size)}. */
    private static Set<Integer> pickIndices(int size, int count) {
        final var indices = new HashSet<Integer>(count);
        while (indices.size() < count) {
            indices.add(RANDOM.nextInt(size));
        }
        return indices;
    }

    private static float randomFloatDifferentFrom(float value) {
        float candidate;
        do {
            candidate = RANDOM.nextFloat();
        } while (candidate == value);
        return candidate;
    }

    private static int randomIntDifferentFrom(int value) {
        int candidate;
        do {
            candidate = RANDOM.nextInt();
        } while (candidate == value);
        return candidate;
    }

    /** Round-robin iterator over a fixed string array; wraps from the end back to index 0. */
    private static final class RoundRobinStrings {
        private final String[] values;
        private int index = 0;

        RoundRobinStrings(String[] values) {
            this.values = values;
        }

        String next() {
            if (index == values.length) {
                index = 0;
            }
            return values[index++];
        }
    }
}

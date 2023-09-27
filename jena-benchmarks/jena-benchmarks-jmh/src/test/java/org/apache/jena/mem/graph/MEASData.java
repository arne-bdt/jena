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
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.UUID;

public class MEASData {

    public static final String MEAS_NS = "http://www.fancyTSO.org/OurCIMModel/MEASv1#";
    private static final Model m = ModelFactory.createDefaultModel();
    public static final Property AnalogValueValue = m.createProperty(MEAS_NS + "AnalogValue.value");
    public static final Property DiscreteValueValue = m.createProperty(MEAS_NS + "DiscreteValue.value");
    public static final Property MeasurementValueTimeStamp = m.createProperty(MEAS_NS + "MeasurementValue.timeStamp");
    public static final Property MeasurementValueStatus = m.createProperty(MEAS_NS + "MeasurementValue.status");
    private static final String[] AnalogTypes = {"SomeAnalog", "ActivePowerAnalog", "ReactivePowerAnalog", "VoltageAnalog", "PhaseAngleAnalog", "GlobalRadiationAnalog", "HumidityAnalog", "TemperatureAnalog", "WindSpeedAnalog", "WindDirectionAnalog", "FrequencyAnalog", "PowerFactorAnalog", "CurrentAnalog"};
    private static final String[] DiscreteTypes = {"PhaseChangerStep", "BreakerStatus", "SwitchStatus", "TapChangerStatus", "TapChangerStep", "TapChangerControlMode", "TapChangerNeutralStatus", "TapChangerKind", "TapChangerMode", "TapChangerControlKind"};

    public static void fillGraphWithMEASData(final Graph graph, final int numberOfAnalogValues, final int numberOfDiscreteValues) {
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
}

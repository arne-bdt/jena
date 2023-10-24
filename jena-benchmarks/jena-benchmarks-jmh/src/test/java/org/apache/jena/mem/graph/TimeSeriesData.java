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
import java.util.*;

public class TimeSeriesData {

    public static final String TSTS_NS = "http://www.fancyTSO.org/OurCIMModel/TSTSv1#";

    private static final Model m = ModelFactory.createDefaultModel();

    public static final Property ZeitreiheIntervall = m.createProperty(TSTS_NS + "Zeitreihe.intervall");
    public static final Property ZeitreiheHerkunft = m.createProperty(TSTS_NS + "Zeitreihe.herkunft");
    public static final Property ZeitreiheName = m.createProperty(TSTS_NS + "Zeitreihe.name");
    public static final Property ZeitreiheLangname = m.createProperty(TSTS_NS + "Zeitreihe.langname");
    public static final Property ZeitreiheModulId = m.createProperty(TSTS_NS + "Zeitreihe.modulId");
    public static final Property ZeitreiheLokalitaet = m.createProperty(TSTS_NS + "Zeitreihe.lokalitaet");
    public static final Property ZeitreiheEnergieart = m.createProperty(TSTS_NS + "Zeitreihe.energieart");
    public static final Property ZeitreiheZeitreihenGruppe = m.createProperty(TSTS_NS + "Zeitreihe.zeitreihenGruppe");
    public static final Property ZeitreiheZeitreihenTyp = m.createProperty(TSTS_NS + "Zeitreihe.zeitreihenTyp");
    public static final Property ZeitreiheVermarktungsart = m.createProperty(TSTS_NS + "Zeitreihe.vermarktungsart");
    public static final Property ZeitreiheWertetyp = m.createProperty(TSTS_NS + "Zeitreihe.wertetyp");
    public static final Property ZeitreiheTSTSIntervall = m.createProperty(TSTS_NS + "Zeitreihe.TSTSIntervall");
    public static final Property ZeitreiheDefaultValue = m.createProperty(TSTS_NS + "Zeitreihe.defaultValue");
    public static final Property ZeitreiheDefaultStatus = m.createProperty(TSTS_NS + "Zeitreihe.defaultStatus");
    public static final Property ZeitreiheEinheit = m.createProperty(TSTS_NS + "Zeitreihe.Einheit");
    public static final Property ZeitreiheFormel = m.createProperty(TSTS_NS + "Zeitreihe.formel");

    public static final Integer[] Intervalle = new Integer[]{1, 5, 15, 60, 1440};

    public static final String[] Herkuenfte = generateUUIDs(512);

    public static final String[] modulIDs = generateUUIDs(64);

    public static final String[] prognosemodelle = generateUUIDs(1024);

    public static final String[] energiearten = generateUUIDs(255);

    public static final String[] zeitreihenGruppen = generateUUIDs(1024);

    public static final String[] zeitreihenTypen = generateUUIDs(512);

    public static final String[] vermarktungsarten = generateUUIDs(128);

    public static final String[] wertetypen = generateUUIDs(512);

    public static final String[] einheiten = generateUUIDs(64);

    public static final Double[] defaultValues = new Double[] { 0.0, 1.0, 100.0, -1.0 };

    public static final String[] defaultStatusValues = generateUUIDs(255);

    private static String[] generateUUIDs(int lenght) {
        var array = new String[lenght];
        for (int i = 0; i < lenght; i++) {
            array[i] = "_" + UUID.randomUUID();
        }
        return array;
    }

    public static void fillGraphWithTimeSeries(final Graph graph, final int totalNumberOfTimeSeries) {
        final var model = ModelFactory.createModelForGraph(graph);

        final String[] lokalitaeten = generateUUIDs(totalNumberOfTimeSeries/3);

        final var random = new Random();
        final var intervalProvider = new NextElementInArrayProvider<>(Intervalle);
        final var herkunftProvider = new NextElementInArrayProvider<>(Herkuenfte);
        final var modulIDProvider = new NextElementInArrayOrNullProvider<>(modulIDs, 0.9, random);
        final var lokalitaetProvider = new NextElementInArrayProvider<>(lokalitaeten);
        final var prognosemodellProvider = new NextElementInArrayOrNullProvider<>(prognosemodelle, 0.2, random);
        final var energieartProvider = new NextElementInArrayOrNullProvider<>(energiearten, 0.05, random);
        final var zeitreihenGruppeProvider = new NextElementInArrayOrNullProvider<>(zeitreihenGruppen, 0.5, random);
        final var zeitreihenTypProvider = new NextElementInArrayOrNullProvider<>(zeitreihenTypen, 0.05, random);
        final var vermarktungsartProvider = new NextElementInArrayOrNullProvider<>(vermarktungsarten, 0.2, random);
        final var wertetypProvider = new NextElementInArrayOrNullProvider<>(wertetypen, 0.05, random);
        final var defaultValueProvider = new NextElementInArrayOrNullProvider<>(defaultValues, 0.5, random);
        final var defaultStatusProvider = new NextElementInArrayOrNullProvider<>(defaultStatusValues, 0.1, random);
        final var einheitProvider = new NextElementInArrayProvider<>(einheiten);
        final var formelProvider = new UUIDOrNullProvider(0.85, random);

        for(int i=0; i<totalNumberOfTimeSeries; i++) {
            final var uuid = UUID.randomUUID().toString();
            final var resource = model.createResource("_" + uuid)
                    .addProperty(RDF.type, model.createResource(TSTS_NS + "Zeitreihe"))
                    .addProperty(ZeitreiheIntervall, Integer.toString(intervalProvider.next()), XSDDatatype.XSDinteger)
                    .addProperty(ZeitreiheHerkunft, herkunftProvider.next())
                    .addProperty(ZeitreiheName, "Zeitreihe " + i)
                    .addProperty(ZeitreiheLangname, "Zeitreihe " + i + ": " + uuid)
                    .addProperty(ZeitreiheLokalitaet, lokalitaetProvider.next())
                    .addProperty(ZeitreiheTSTSIntervall, Integer.toString(intervalProvider.next()), XSDDatatype.XSDinteger)
                    .addProperty(ZeitreiheEinheit, einheitProvider.next());

            final var modulID = modulIDProvider.next();
            if (modulID != null) {
                resource.addProperty(ZeitreiheModulId, modulID);
            }
            final var prognosemodell = prognosemodellProvider.next();
            if (prognosemodell != null) {
                resource.addProperty(ZeitreiheModulId, prognosemodell);
            }
            final var energieart = energieartProvider.next();
            if (energieart != null) {
                resource.addProperty(ZeitreiheEnergieart, energieart);
            }
            final var zeitreihenGruppe = zeitreihenGruppeProvider.next();
            if (zeitreihenGruppe != null) {
                resource.addProperty(ZeitreiheZeitreihenGruppe, zeitreihenGruppe);
            }
            final var zeitreihenTyp = zeitreihenTypProvider.next();
            if (zeitreihenTyp != null) {
                resource.addProperty(ZeitreiheZeitreihenTyp, zeitreihenTyp);
            }
            final var vermarktungsart = vermarktungsartProvider.next();
            if (vermarktungsart != null) {
                resource.addProperty(ZeitreiheVermarktungsart, vermarktungsart);
            }
            final var wertetyp = wertetypProvider.next();
            if (wertetyp != null) {
                resource.addProperty(ZeitreiheWertetyp, wertetyp);
            }
            final var defaultValue = defaultValueProvider.next();
            if (defaultValue != null) {
                resource.addProperty(ZeitreiheDefaultValue, Double.toString(defaultValue), XSDDatatype.XSDdouble);
            }
            final var defaultStatus = defaultStatusProvider.next();
            if (defaultStatus != null) {
                resource.addProperty(ZeitreiheDefaultStatus, defaultStatus);
            }
            final var formel = formelProvider.next();
            if (formel != null) {
                resource.addProperty(ZeitreiheFormel, formel);
            }
        }
    }




    private static class NextElementInArrayProvider<T> {
        private final T[] array;
        private int index = 0;

        public NextElementInArrayProvider(T[] array) {
            this.array = array;
        }

        public T next() {
            if (index == array.length) {
                index = 0;
            }
            return array[index++];
        }
    }

    private static class NextElementInArrayOrNullProvider<T> {
        private final T[] array;
        private int index = 0;
        private final double nullProbability;
        private final Random random;

        public NextElementInArrayOrNullProvider(T[] array, double nullProbability, Random random) {
            this.array = array;
            this.nullProbability = nullProbability;
            this.random = random;
        }

        public T next() {
            if (random.nextDouble() < nullProbability) {
                return null;
            }
            if (index == array.length) {
                index = 0;
            }
            return array[index++];
        }
    }

    private static class UUIDOrNullProvider {
        private final double nullProbability;
        private final Random random;

        public UUIDOrNullProvider(double nullProbability, Random random) {
            this.nullProbability = nullProbability;
            this.random = random;
        }

        public String next() {
            if (random.nextDouble() < nullProbability) {
                return null;
            }
            return "_" + UUID.randomUUID();
        }
    }
}

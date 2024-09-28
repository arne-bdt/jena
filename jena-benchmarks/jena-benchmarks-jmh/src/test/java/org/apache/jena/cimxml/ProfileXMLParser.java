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

import org.apache.commons.io.input.BufferedFileChannelInputStream;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.cimxml.schema.BaseURI;
import org.apache.jena.graph.Graph;
import org.apache.jena.jmh.helper.TestFileInventory;
import org.apache.jena.mem2.GraphMem2Fast;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RDFWriter;
import org.apache.jena.riot.lang.rdfxml.RRX;
import org.apache.jena.sparql.graph.GraphFactory;
import org.junit.Test;

import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ProfileXMLParser {


    @Test
    public void parseXML() throws Exception {
        var stopWatch = StopWatch.createStarted();
        var sink = new GraphMem2Fast();
        try(final var is = new BufferedFileChannelInputStream.Builder()
                .setFile(TestFileInventory.getFilePath(TestFileInventory.XML_REAL_GRID_V2_EQ))
                .setOpenOptions(StandardOpenOption.READ)
                .setBufferSize(64*4096)
                .get()) {
            RDFParser.source(is)
                    .base(BaseURI.DEFAULT_BASE_URI)  // base URI for the model and thus for al mRID's in the model
                    .forceLang(RRX.RDFXML_SAX)
                    .checking(false)
                    .parse(sink);
        }
        stopWatch.stop();
        System.out.println("Triples in graph: " + sink.size());
        System.out.println(stopWatch);
    }

    @Test
    public void testCompare() throws Exception {
        var files = List.of(
                TestFileInventory.XML_XXX_CGMES_SSH,
                TestFileInventory.XML_XXX_CGMES_EQ,
                TestFileInventory.XML_XXX_CGMES_TP,
                TestFileInventory.XML_REAL_GRID_V2_EQ,
                TestFileInventory.XML_REAL_GRID_V2_TP,
                TestFileInventory.XML_REAL_GRID_V2_SSH,
                TestFileInventory.XML_REAL_GRID_V2_SV,
                TestFileInventory.XML_BSBM_5M
//                TestFileInventory.RDF_CITATIONS
        );
        var langs = List.of(
                RRX.RDFXML_SAX
//                RRX.RDFXML_StAX2_ev_aalto,
//                RRX.RDFXML_StAX_sr,
//                RRX.RDFXML_StAX_ev
        );
        for(var file : files) {
            final var referenceGraph = read(file, RRX.RDFXML_StAX2_ev_aalto);
            for(var lang : langs) {
                System.out.println(file + " -> " + lang);
                final var graphToTest = read(file, lang);

                referenceGraph.isIsomorphicWith(graphToTest);
                referenceGraph.getPrefixMapping().getNsPrefixMap().entrySet().forEach(e ->
                        assertEquals(e.getValue(), graphToTest.getPrefixMapping().getNsPrefixURI(e.getKey()))
                );
                graphToTest.getPrefixMapping().getNsPrefixMap().entrySet().forEach(e ->
                        assertEquals(e.getValue(), referenceGraph.getPrefixMapping().getNsPrefixURI(e.getKey()))
                );
                referenceGraph.find().forEachRemaining(t ->
                    assertTrue(graphToTest.contains(t))
                );
                graphToTest.find().forEachRemaining(t ->
                        assertTrue(referenceGraph.contains(t))
                );
            }
        }
    }

    private static Graph read(String inventoryFile, Lang lang) throws Exception {
        var graph = new GraphMem2Fast();
        try(final var is = new BufferedFileChannelInputStream.Builder()
                .setFile(TestFileInventory.getFilePath(inventoryFile))
                .setOpenOptions(StandardOpenOption.READ)
                .setBufferSize(64*4096)
                .get()) {
            RDFParser.source(is)
                    .base(BaseURI.DEFAULT_BASE_URI)  // base URI for the model and thus for al mRID's in the model
                    .forceLang(lang)
                    .checking(false)
                    .parse(graph);
        }
        return  graph;
    }


    public void convertBSBMToRDFXML() {
        var sink = GraphFactory.createGraphMem();

        RDFParser.source("C:\\temp\\bsbm-25m.nt.gz")
                .base(BaseURI.DEFAULT_BASE_URI)  // base URI for the model and thus for al mRID's in the model
                .checking(false)
                .parse(sink);

        RDFWriter.source(sink)
                .format(RDFFormat.RDFXML)
                .output("C:\\temp\\bsbm-25m.xml");
    }
}

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
import org.apache.jena.jmh.helper.TestFileInventory;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RDFWriter;
import org.apache.jena.riot.lang.rdfxml.RRX;
import org.apache.jena.sparql.graph.GraphFactory;
import org.junit.Test;

import java.nio.file.StandardOpenOption;

public class ProfileXMLParser {


    @Test
    public void parseXML() throws Exception {
        var stopWatch = StopWatch.createStarted();
        var sink = GraphFactory.createGraphMem();
        try(final var is = new BufferedFileChannelInputStream.Builder()
                .setFile(TestFileInventory.getFilePath(TestFileInventory.XML_REAL_GRID_V2_EQ))
                .setOpenOptions(StandardOpenOption.READ)
                .setBufferSize(64*4096)
                .get()) {
            RDFParser.source(is)
                    .base(BaseURI.DEFAULT_BASE_URI)  // base URI for the model and thus for al mRID's in the model
                    .forceLang(RRX.RDFXML_StAX2_sr_aalto)
                    .checking(false)
                    .parse(sink);
        }
        stopWatch.stop();
        System.out.println("Triples in graph: " + sink.size());
        System.out.println(stopWatch);
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

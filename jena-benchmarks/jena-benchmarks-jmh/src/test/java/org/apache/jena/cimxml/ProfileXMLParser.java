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

import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.cimxml.schema.BaseURI;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.lang.rdfxml.RRX;
import org.apache.jena.sparql.graph.GraphFactory;
import org.junit.Test;

public class ProfileXMLParser {


    @Test
    public void parseXML() throws Exception {
        var stopWatch = StopWatch.createStarted();
        var sink = GraphFactory.createGraphMem();
        RDFParser.source("C:\\rd\\bewegungsdaten-demo\\shared\\ENTSOE_RDF\\src\\main\\resources\\CGMES\\v2.4.15\\TestConfigurations_packageCASv2.0\\RealGrid\\CGMES_v2.4.15_RealGridTestConfiguration_v2\\CGMES_v2.4.15_RealGridTestConfiguration_EQ_V2.xml")
                .base(BaseURI.DEFAULT_BASE_URI)  // base URI for the model and thus for al mRID's in the model
                .forceLang(RRX.RDFXML_StAX2_ev)
                .checking(false)
                .parse(sink);
        stopWatch.stop();
        System.out.println("Triples in graph: " + sink.size());
        System.out.println(stopWatch);
    }
}

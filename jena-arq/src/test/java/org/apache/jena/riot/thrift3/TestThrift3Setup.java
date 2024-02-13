/**
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

package org.apache.jena.riot.thrift3;

import org.apache.jena.riot.*;
import org.junit.Test;

import static org.apache.jena.riot.RDFLanguages.RDFTHRIFT3;
import static org.junit.Assert.*;

public class TestThrift3Setup {

    @Test public void setup_01() {
        assertTrue(RDFLanguages.isRegistered(RDFTHRIFT3)) ;
    }

    @Test public void setup_02() {
        Lang lang = RDFLanguages.filenameToLang("data.rt3") ;
        assertEquals(lang, RDFTHRIFT3) ;
    }

    @Test public void setup_03() {
        assertTrue(RDFParserRegistry.isQuads(RDFTHRIFT3)) ;
        assertTrue(RDFParserRegistry.isTriples(RDFTHRIFT3)) ;
        assertTrue(RDFParserRegistry.isRegistered(RDFTHRIFT3));
        assertNotNull(RDFParserRegistry.getFactory(RDFTHRIFT3)) ;
    }

    @Test public void setup_04() {
        assertTrue(RDFWriterRegistry.contains(RDFTHRIFT3)) ;
        assertNotNull(RDFWriterRegistry.getWriterDatasetFactory(RDFTHRIFT3)) ;
        assertTrue(RDFWriterRegistry.contains(RDFFormat.RDF_THRIFT3)) ;
        assertNotNull(RDFWriterRegistry.getWriterDatasetFactory(RDFFormat.RDF_THRIFT3)) ;
    }
}


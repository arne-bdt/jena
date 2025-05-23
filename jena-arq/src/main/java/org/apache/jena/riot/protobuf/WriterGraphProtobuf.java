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

package org.apache.jena.riot.protobuf;

import static org.apache.jena.riot.RDFFormat.RDF_PROTO_VALUES;
import static org.apache.jena.riot.RDFLanguages.RDFPROTO;

import java.io.OutputStream;
import java.io.Writer;

import org.apache.jena.atlas.lib.NotImplemented;
import org.apache.jena.graph.Graph;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.WriterGraphRIOT;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.StreamRDFOps;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.util.Context;

/** Write a graph as RDF Protobuf */
public class WriterGraphProtobuf implements WriterGraphRIOT
{
    private final boolean withValues;
    public WriterGraphProtobuf(RDFFormat fmt) {
        this.withValues = RDF_PROTO_VALUES.equals(fmt);
    }
    @Override
    public Lang getLang() {
        return RDFPROTO;
    }
    @Override
    public void write(Writer out, Graph graph, PrefixMap prefixMap, String baseURI, Context context) {
        throw new NotImplemented("Writing binary data to a java.io.Writer is not supported. Please use an OutputStream");
    }

    @Override
    public void write(OutputStream out, Graph graph, PrefixMap prefixMap, String baseURI, Context context) {
        StreamRDF stream = ProtobufRDF.streamToOutputStream(out, withValues);
        stream.start();
        StreamRDFOps.graphToStream(graph, stream);
        stream.finish();
    }
}

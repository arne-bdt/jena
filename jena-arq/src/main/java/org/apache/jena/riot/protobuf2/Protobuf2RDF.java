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

package org.apache.jena.riot.protobuf2;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.riot.protobuf2.wire.PB2_RDF.RDF_StreamRow;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.thrift.ThriftRDF;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.exec.RowSetStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Operations on binary RDF using <a href="https://developers.google.com/protocol-buffers">Google Protobuf</a>.
 * <p>
 * An encoding use Apache Thrift is available in {@link ThriftRDF}.
 */
public class Protobuf2RDF {

    /**
     * Create an {@link StreamRDF} for output.  A filenames ending {@code .gz} or {@code .bz2} will have
     * the respective compressor added to the output path. A filename of "-" is {@code System.out}.
     * The file is closed when {@link StreamRDF#finish()} is called unless it is {@code System.out}.
     * Call {@link StreamRDF#start()}...{@link StreamRDF#finish()}.
     *
     * @param filename The file
     * @return StreamRDF A stream to send to.
     */
    public static StreamRDF streamToFile(String filename) {
        OutputStream out = IO.openOutputFile(filename) ;
        out = IO.ensureBuffered(out);
        return streamToOutputStream(out);
    }

    /**
     * An {@link StreamRDF} that writes to an output stream.
     * The output stream is written with "writeDelimitedTo".
     */
    public static StreamRDF streamToOutputStream(OutputStream output) {
        output = IO.ensureBuffered(output);
        try {
            return StreamRDF2Protobuf2.createDelimited(output);
        } finally { IO.flush(output); }
    }

    /**
     * Decode the contents of the file and send to the {@link StreamRDF}.
     * A filename ending {@code .gz} will have a gzip decompressor added.
     * A filename of "-" is {@code System.in}.
     * @param filename The file.
     * @param dest Sink
     */
    public static void fileToStream(String filename, StreamRDF dest) {
        InputStream in = IO.openFileBuffered(filename) ;
        inputStreamToStreamRDF(in, dest);
    }

    /**
     * Read an input stream and send item to the {@link StreamRDF}. The input stream
     * will be read with "parseDelimitedTo".
     * @param input InputStream
     * @param stream StreamRDF
     */
    public static void inputStreamToStreamRDF(InputStream input, StreamRDF stream) {
        Protobuf2StreamRDF visitor = new Protobuf2StreamRDF(PrefixMapFactory.create(), stream);
        stream.start();
        try {
            apply(input, visitor);
        }
        finally { stream.finish(); }
    }

    /**
     * Send the contents of a RDF-encoded protobuf file to an "action"
     * @param input   InputStream
     * @param action  Code to act on the row.
     */
    public static void apply(InputStream input, VisitorStreamRowProto2RDF action) {
        input = IO.ensureBuffered(input);
        try {
            while(true) {
                RDF_StreamRow x = RDF_StreamRow.parseDelimitedFrom(input);
                if(!PBuf2RDF.visit(x, action)) {
                    return;
                }
            }
        } catch(IOException ex) { IO.exception(ex); }
    }

    /**
     * Return a streaming {@link RowSet} read from an input stream (with delimiters per row)
     */
    public static RowSet readRowSet(InputStream input) {
        Protobuf2Binding p2b = new Protobuf2Binding(input);
        return RowSetStream.create(p2b.getVars(), p2b);
    }

    /**
     * Write a {@link RowSet} to an output stream (with delimiters per row)
     */
    public static void writeRowSet(OutputStream out, RowSet rowSet) {
        out = IO.ensureBuffered(out);
        try {
            List<Var> vars = rowSet.getResultVars();
            try ( Binding2Protobuf2 b2p = new Binding2Protobuf2(out, vars) ) {
                rowSet.forEachRemaining(b2p::output);
            }
        } finally { IO.flush(out); }
    }

}


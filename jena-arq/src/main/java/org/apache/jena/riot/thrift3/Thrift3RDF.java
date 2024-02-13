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

package org.apache.jena.riot.thrift3;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.riot.protobuf.ProtobufRDF;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.thrift3.wire.RDF_StreamRow;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.exec.RowSetStream;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransportException;

import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.function.Consumer;

/**
 * Operations on binary RDF (which uses <a href="http://thrift.apache.org/">Apache Thrift</a>).
 * See also {@link Thrift3Convert}, for specific functions on binary RDF.
 * <p>
 * Encoding use Protobuf is available in {@link ProtobufRDF}.
 */
public class Thrift3RDF {

    private static int BUFSIZE_OUT  = 128*1024 ;

    /**
     * Create an {@link StreamRDF} for output.  A filename ending {@code .gz} will have
     * a gzip compressor added to the output path. A filename of "-" is {@code System.out}.
     * The file is closed when {@link StreamRDF#finish()} is called unless it is {@code System.out}.
     * Call {@link StreamRDF#start()}...{@link StreamRDF#finish()}.
     *
     * @param filename The file
     * @return StreamRDF A stream to send to.
     */
    public static StreamRDF streamToFile(String filename) {
        OutputStream out = IO.openOutputFile(filename) ;
        BufferedOutputStream bout = new BufferedOutputStream(out, BUFSIZE_OUT) ;
        TProtocol protocol = T3RDF.protocol(bout) ;
        return new StreamRDF2Thrift3(protocol) ;
    }

    /**
     * Create an {@link StreamRDF} for output.
     * The {@code OutputStream} is closed when {@link StreamRDF#finish()} is called unless it is {@code System.out}.
     * Call {@link StreamRDF#start()}...{@link StreamRDF#finish()}.
     * @param out OutputStream
     * @return StreamRDF A stream to send to.
     */
    public static StreamRDF streamToOutputStream(OutputStream out) {
        return new StreamRDF2Thrift3(out) ;
    }

    /**
     * Create an {@link StreamRDF} for output.
     * The {@code OutputStream} is closed when {@link StreamRDF#finish()} is called unless it is {@code System.out}.
     * Call {@link StreamRDF#start()}...{@link StreamRDF#finish()}.
     * @param protocol Output and encoding.
     * @return StreamRDF A stream to send to.
     */
    public static StreamRDF streamToTProtocol(TProtocol protocol) {
        return new StreamRDF2Thrift3(protocol) ;
    }

    /**
     * Decode the contents of the file and send to the {@link StreamRDF}.
     * A filename ending {@code .gz} will have a gzip decompressor added.
     * A filename of "-" is {@code System.in}.
     * @param filename The file.
     * @param dest Sink
     */
    public static void fileToStream(String filename, StreamRDF dest) {
        InputStream in = IO.openFile(filename) ;
        TProtocol protocol = T3RDF.protocol(in) ;
        protocolToStream(protocol, dest) ;
    }

    /**
     * Decode the contents of the input stream and send to the {@link StreamRDF}.
     * @param in InputStream
     * @param dest StreamRDF
     */
    public static void inputStreamToStream(InputStream in, StreamRDF dest) {
        TProtocol protocol = T3RDF.protocol(in) ;
        protocolToStream(protocol, dest) ;
    }

    /**
     * Decode the contents of the TProtocol and send to the {@link StreamRDF}.
     * @param protocol TProtocol
     * @param dest Sink
     */
    public static void protocolToStream(TProtocol protocol, StreamRDF dest) {
        final PrefixMap pmap = PrefixMapFactory.create() ;
        final var readerDict = new StringDictionaryReader();
        final Thrift2StreamRDF s = new Thrift2StreamRDF(pmap, dest, readerDict) ;

        dest.start() ;
        apply(protocol, z -> T3RDF.visit(z, s, readerDict)) ;
        // Includes flushing the protocol.
        dest.finish() ;
    }

    /**
     * Send the contents of a RDF-encoded Thrift file to an "action"
     * @param protocol TProtocol
     * @param action   Code to act on the row.
     */
    public static void apply(TProtocol protocol, Consumer<RDF_StreamRow> action) {
        RDF_StreamRow row = new RDF_StreamRow() ;
        // Bug in 0.13.0 / TIOStreamTransport.isOpen / THRIFT-5022
        //while(protocol.getTransport().isOpen()) {
        while(true) {
            try { row.read(protocol) ; }
            catch (TTransportException e) {
                if ( e.getType() == TTransportException.END_OF_FILE )
                    // THRIFT-5022 // break;
                    return;
            }
            catch (TException ex) { T3RDF.exception(ex) ; }
            action.accept(row) ;
            row.clear() ;
        }
    }

    /** Debug help - print details of a Thrift stream.
     * Destructive on the InputStream.
     * @param out OutputStream
     * @param in InputStream
     */
    public static void dump(OutputStream out, InputStream in) {
        IndentedWriter iOut = new IndentedWriter(out) ;
        final var readerDict = new StringDictionaryReader();
        StreamRowT3RDFPrinter printer = new StreamRowT3RDFPrinter(iOut, readerDict) ;
        TProtocol protocol = T3RDF.protocol(in) ;
        apply(protocol, z -> T3RDF.visit(z, printer, readerDict));
        iOut.flush() ;
    }

    public static RowSet readRowSet(InputStream in) {
        return readRowSet(T3RDF.protocol(in)) ;
    }

    public static RowSet readRowSet(TProtocol protocol) {
        Thift2Binding t2b = new Thift2Binding(protocol) ;
        return RowSetStream.create(t2b.getVars(), t2b) ;
    }

    public static void writeRowSet(OutputStream out, RowSet rowSet) {
        out = T3RDF.ensureBuffered(out);
        writeRowSet(T3RDF.protocol(out), rowSet) ;
        IO.flush(out) ;
    }

    public static void writeRowSet(TProtocol protocol, RowSet rowSet) {
        List<Var> vars = rowSet.getResultVars();
        try ( Binding2Thrift3 b2t = new Binding2Thrift3(protocol, vars) ) {
            rowSet.forEachRemaining(b2t::output);
        }
        //Done by Binding2Thrift.close() -- LibThriftRDF.flush(protocol) ;
    }
}

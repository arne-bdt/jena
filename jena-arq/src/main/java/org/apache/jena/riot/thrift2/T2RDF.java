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

package org.apache.jena.riot.thrift2;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.thrift2.wire.RDF_ANY;
import org.apache.jena.riot.thrift2.wire.RDF_StreamRow;
import org.apache.jena.riot.thrift2.wire.RDF_Term;
import org.apache.jena.riot.thrift2.wire.RDF_UNDEF;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

import java.io.*;

/** Support operations for RDF Thrift */
public class T2RDF {
    private static final int BUFSIZE_IN   = 128*1024 ;
    private static final int BUFSIZE_OUT  = 128*1024; ;

    public static InputStream ensureBuffered(InputStream input) {
        if ( input instanceof BufferedInputStream )
            return input;
        if ( input instanceof ByteArrayInputStream )
            return input;
        return new BufferedInputStream(input, BUFSIZE_IN);
    }

    public static OutputStream ensureBuffered(OutputStream output) {
        if ( output instanceof BufferedOutputStream )
            return output;
        if ( output instanceof ByteArrayOutputStream )
            return output;
        return new BufferedOutputStream(output, BUFSIZE_OUT);
    }

    /**
     * Create Thrift protocol for the InputStream.
     * @param in InputStream
     */
    public static TProtocol protocol(InputStream in) {
        in = ensureBuffered(in);
        try {
            TTransport transport = new TIOStreamTransport(in) ;
            transport.open() ;
            TProtocol protocol = protocol(transport) ;
            return protocol ;
        } catch (TException ex) { T2RDF.exception(ex) ; return null ; }
    }

    /**
     * Create Thrift protocol for the OutputStream.
     * The caller must call {@link T2RDF#flush(TProtocol)}
     * which will flush the underlying (internally buffered) output stream.
     * @param out OutputStream
     */
    public static TProtocol protocol(OutputStream out) {
        out = ensureBuffered(out);
        try {
            // Flushing the protocol will flush the BufferedOutputStream
            TTransport transport = new TIOStreamTransport(out) ;
            transport.open() ;
            TProtocol protocol = protocol(transport) ;
            return protocol ;
        } catch (TException ex) { T2RDF.exception(ex) ; return null ; }
    }

    /**
     * Decode the contents of the input stream and send to the {@link StreamRDF}.
     * @param filename
     */
    public static TProtocol protocol(String filename) {
        InputStream in = IO.openFile(filename) ;
        TProtocol protocol = protocol(in) ;
        return protocol ;
    }

    public static TProtocol protocol(TTransport transport) {
        if ( true ) return new TCompactProtocol(transport) ;

        // Keep the warnings down.
        if ( false ) return new TTupleProtocol(transport) ;
        if ( false ) return new TJSONProtocol(transport) ;
        throw new RiotThrift2Exception("No protocol impl choosen") ;
    }

    /** Flush a TProtocol; exceptions converted to {@link RiotException} */
    public static void flush(TProtocol protocol) {
        flush(protocol.getTransport()) ;
    }

    /** Flush a TTransport; exceptions converted to {@link RiotException} */
    public static void flush(TTransport transport) {
        try { transport.flush() ; }
        catch (TException ex) { T2RDF.exception(ex) ; }
    }

    public static final RDF_ANY ANY = new RDF_ANY() ;
    /** The Thrift RDF Term 'ANY' */
    public static final RDF_Term tANY = new RDF_Term() ;
    /** The Thrift RDF Term 'UNDEF' */
    public static final RDF_UNDEF UNDEF = new RDF_UNDEF() ;
    public static final RDF_Term tUNDEF = new RDF_Term();

    static { tANY.setAny(new RDF_ANY()) ; }

    static { tUNDEF.setUndefined(new RDF_UNDEF()) ; }

    public static void visit(RDF_StreamRow row, VisitorStreamRowT2RDF visitor, StringDictionaryReader readerDict) {
        readerDict.addAll(row.getStrings());
        final var r = row.getRow();
        if ( r.isSetTriple() ) {
            visitor.visit(r.getTriple()) ;
        } else if ( r.isSetQuad() ) {
            visitor.visit(r.getQuad()) ;
        } else if ( r.isSetPrefixDecl() ) {
            visitor.visit(r.getPrefixDecl()) ;
        } else {
            Log.warn(Thrift2Convert.class, "visit: Unrecognized: "+row) ;
        }
    }

    public static void exception(TException ex) { throw new RiotThrift2Exception(ex) ; }

}


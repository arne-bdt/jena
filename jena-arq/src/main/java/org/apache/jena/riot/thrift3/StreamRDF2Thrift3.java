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

import org.apache.jena.atlas.logging.Log;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.thrift.TRDF;
import org.apache.jena.riot.thrift3.wire.*;
import org.apache.jena.sparql.core.Quad;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

import java.io.OutputStream;

/** Encode StreamRDF in Thrift.
 *
 * @see Thrift2StreamRDF (for each RDF_StreamRow) for the reverse process.
 */
public class StreamRDF2Thrift3 implements StreamRDF, AutoCloseable
{
    // No REPEAT support.
    private final OutputStream out;
    private final TProtocol protocol;
    private PrefixMap pmap = PrefixMapFactory.create();

    private final StringDictionaryWriter writerDict = new StringDictionaryWriter();

    public StreamRDF2Thrift3(OutputStream out) {
        this(TRDF.protocol(out));
    }

    public StreamRDF2Thrift3(TProtocol out) {
        this.out = null;
        this.protocol = out;
        this.pmap = PrefixMapFactory.create();
    }

    @Override
    public void start() { }

    private final RDF_PrefixDecl tprefix = new RDF_PrefixDecl();
    private final RDF_StreamRow tStreamRow   = new RDF_StreamRow();
    private final RDF_StreamUnion tStreamUnion = new RDF_StreamUnion();

    private final RDF_Triple ttriple    = new RDF_Triple();
    private final RDF_Quad tquad      = new RDF_Quad();

    private final RDF_Term tsubject   = new RDF_Term();
    private final RDF_Term tpredicate = new RDF_Term();
    private final RDF_Term tobject    = new RDF_Term();
    private final RDF_Term tgraph     = new RDF_Term();

    @Override
    public void triple(Triple triple) {
        doTriple(triple.getSubject(), triple.getPredicate(), triple.getObject());
    }

    private void doTriple(Node subject, Node predicate, Node object) {
        Thrift3Convert.toThrift(subject, pmap, tsubject, writerDict);
        Thrift3Convert.toThrift(predicate, pmap, tpredicate, writerDict);
        Thrift3Convert.toThrift(object, pmap, tobject, writerDict);
        ttriple.setS(tsubject);
        ttriple.setP(tpredicate);
        ttriple.setO(tobject);

        tStreamUnion.setTriple(ttriple);
        tStreamRow.setRow(tStreamUnion);
        if ( writerDict.hasStringsToFlush()) {
            tStreamRow.setStrings(writerDict.flush());
        }
        try { tStreamRow.write(protocol); }
        catch (TException e) { T3RDF.exception(e); }
        finally {
            tStreamRow.clear();
            tStreamUnion.clear();
            ttriple.clear();
            tsubject.clear();
            tpredicate.clear();
            tobject.clear();
        }
    }

    @Override
    public void quad(Quad quad) {
        if ( quad.getGraph() == null || quad.isDefaultGraph() ) {
            doTriple(quad.getSubject(), quad.getPredicate(), quad.getObject());
            return;
        }

        Thrift3Convert.toThrift(quad.getGraph(), pmap, tgraph, writerDict);
        Thrift3Convert.toThrift(quad.getSubject(), pmap, tsubject, writerDict);
        Thrift3Convert.toThrift(quad.getPredicate(), pmap, tpredicate, writerDict);
        Thrift3Convert.toThrift(quad.getObject(), pmap, tobject, writerDict);

        tquad.setG(tgraph);
        tquad.setS(tsubject);
        tquad.setP(tpredicate);
        tquad.setO(tobject);
        tStreamUnion.setQuad(tquad);
        tStreamRow.setRow(tStreamUnion);
        if ( writerDict.hasStringsToFlush()) {
            tStreamRow.setStrings(writerDict.flush());
        }

        try { tStreamRow.write(protocol); }
        catch (TException e) { T3RDF.exception(e); }
        finally {
            tStreamRow.clear();
            tStreamUnion.clear();
            tquad.clear();
            tgraph.clear();
            tsubject.clear();
            tpredicate.clear();
            tobject.clear();
        }
    }

    @Override
    public void base(String base) {
        // Ignore.
    }

    @Override
    public void prefix(String prefix, String iri) {
        try { pmap.add(prefix, iri); }
        catch ( RiotException ex) {
            Log.warn(this, "Prefix mapping error", ex);
        }
        tprefix.setPrefix(writerDict.getIndex(prefix));
        tprefix.setUri(writerDict.getIndex(iri));
        tStreamUnion.setPrefixDecl(tprefix);
        tStreamRow.setRow(tStreamUnion);
        if(writerDict.hasStringsToFlush()) {
            tStreamRow.setStrings(writerDict.flush());
        }
        try { tStreamRow.write(protocol); }
        catch (TException e) { T3RDF.exception(e); }
        finally {
            tStreamRow.clear();
            tStreamUnion.clear();
            tprefix.clear();
        }
    }

    @Override
    public void close() {
        finish();
    }

    @Override
    public void finish() {
        T3RDF.flush(protocol);
        writerDict.clear();
    }
}

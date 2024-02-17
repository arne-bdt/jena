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

import org.apache.jena.graph.Node;
import org.apache.jena.riot.thrift3.wire.RDF_DataTuple;
import org.apache.jena.riot.thrift3.wire.RDF_Term;
import org.apache.jena.riot.thrift3.wire.RDF_VAR;
import org.apache.jena.riot.thrift3.wire.RDF_VarTuple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/** Converted from Bindings to SPARQL result set encoded in Thrift */
public class Binding2Thrift3 implements AutoCloseable {
    private final RDF_DataTuple row = new RDF_DataTuple() ;
    private final Collection<Var> vars ;
    private final TProtocol protocol ;

    private final RDF_Term[] terms;

    private final StringDictionaryWriter writerDict = new StringDictionaryWriter() ;

    public Binding2Thrift3(OutputStream out, Collection<Var> vars) {
        this.vars = vars ;
        this.terms = initTerms(vars.size());
        try {
            TIOStreamTransport transport = new TIOStreamTransport(out) ;
            this.protocol = T3RDF.protocol(transport) ;
        } catch (TTransportException ex) { throw new RiotThrift3Exception(ex); }
        varsRow() ;
    }

    private static RDF_Term[] initTerms(int size) {
        RDF_Term[] terms = new RDF_Term[size];
        for(int i = 0; i < size; i++) {
            terms[i] = new RDF_Term();
        }
        return terms;
    }

    private void varsRow() {
        RDF_VarTuple vrow = new RDF_VarTuple(new ArrayList<>(vars.size())) ;
        for ( Var v : vars ) {
            RDF_VAR rv = new RDF_VAR() ;
            rv.setName(writerDict.getIndex(v.getName())) ;
            vrow.addToVars(rv) ;
        }
        if(writerDict.hasStringsToFlush()) {
            vrow.setStrings(writerDict.flush());
        }
        try { vrow.write(protocol) ; }
        catch (TException e) { T3RDF.exception(e) ; }
    }

    public Binding2Thrift3(TProtocol out, Collection<Var> vars) {
        this.vars = vars ;
        this.protocol = out ;
        this.terms = initTerms(vars.size());
        varsRow() ;
    }

    public void output(Binding binding) {
        Iterator<Var> vIter = (vars == null ? null : vars.iterator()) ;
        if ( vIter == null )
            vIter = binding.vars() ;

        row.clear() ;
        for(var t: terms) {
            t.clear();
        }

        final int[] i = {0};
        vIter.forEachRemaining(v -> {
            final Node n = binding.get(v) ;
            if(n != null) {
                Thrift3Convert.toThrift(n, terms[i[0]], writerDict);
                row.addToRow(terms[i[0]++]);
            } else {
                row.addToRow(T3RDF.tUNDEF);
            }
        }) ;
        if(writerDict.hasStringsToFlush()) {
            row.setStrings(writerDict.flush()) ;
        }
        try { row.write(protocol) ; }
        catch (TException e) { T3RDF.exception(e) ; }
    }

    @Override
    public void close() {
        T3RDF.flush(protocol) ;
    }
}

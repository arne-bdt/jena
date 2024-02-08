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

package org.apache.jena.riot.protobuf2;

import org.apache.jena.graph.Node;
import org.apache.jena.riot.protobuf2.wire.PB2_RDF.RDF_DataTuple;
import org.apache.jena.riot.protobuf2.wire.PB2_RDF.RDF_Term;
import org.apache.jena.riot.protobuf2.wire.PB2_RDF.RDF_Var;
import org.apache.jena.riot.protobuf2.wire.PB2_RDF.RDF_VarTuple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;

import java.io.OutputStream;
import java.util.Collection;
import java.util.Iterator;

/** Converted from Bindings to SPARQL result set encoded in Protobuf */
public class Binding2Protobuf2 implements AutoCloseable {
    private final RDF_DataTuple.Builder row = RDF_DataTuple.newBuilder();
    private final RDF_Term.Builder term = RDF_Term.newBuilder();
    private final Collection<Var> vars ;
    private final OutputStream out ;

    private final StringDictionaryWriter writerDict = new StringDictionaryWriter() ;

    public Binding2Protobuf2(OutputStream out, Collection<Var> vars) {
        this.out = out ;
        this.vars = vars ;
        varsRow() ;
    }

    private void varsRow() {
        RDF_VarTuple.Builder vrow = RDF_VarTuple.newBuilder();
        RDF_Var.Builder var = RDF_Var.newBuilder();
        vars.forEach(v->{
            var.clear();
            var.setName(writerDict.getIndex(v.getVarName()));
            vrow.addVars(var);
        });
        if(writerDict.hasStringsToFlush()) {
            vrow.addAllStrings(writerDict.flush());
        }
        PBuf2RDF.writeDelimitedTo(vrow.build(), out);
    }

    public void output(Binding binding) {
        row.clear();
        Iterator<Var> vIter = (vars == null ? null : vars.iterator()) ;
        if ( vIter == null )
            vIter = binding.vars() ;
        vIter.forEachRemaining(v -> {
            term.clear();
            Node n = binding.get(v) ;
            RDF_Term rt = Protobuf2Convert.toProtobuf(n, term, writerDict);
            row.addRow(rt);
        }) ;
        if(writerDict.hasStringsToFlush()) {
            row.addAllStrings(writerDict.flush());
        }
        PBuf2RDF.writeDelimitedTo(row.build(), out);
    }

    @Override
    public void close() {}
}

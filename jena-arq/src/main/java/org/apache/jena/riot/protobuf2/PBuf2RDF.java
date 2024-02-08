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

import com.google.protobuf.GeneratedMessageV3;
import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.lib.InternalErrorException;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.protobuf2.wire.PB2_RDF.RDF_Quad;
import org.apache.jena.riot.protobuf2.wire.PB2_RDF.RDF_StreamRow;
import org.apache.jena.riot.protobuf2.wire.PB2_RDF.RDF_Term;
import org.apache.jena.riot.protobuf2.wire.PB2_RDF.RDF_Triple;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.sparql.core.Quad;

import java.io.IOException;
import java.io.OutputStream;

/** Package support functions */
class PBuf2RDF {

    private static PrefixMap PMAP0 = PrefixMapFactory.emptyPrefixMap();

    public static void writeDelimitedTo(GeneratedMessageV3 gmv3, OutputStream output) {
        try {
            gmv3.writeDelimitedTo(output);
        } catch (IOException e) { IO.exception(e); }
    }

    public static RDF_Triple rdfTriple(Triple triple, RDF_Triple.Builder tripleBuilder, RDF_Term.Builder termBuilder,
                                       StringDictionaryWriter writerDict) {
        tripleBuilder.clear();
        tripleBuilder.setS(rdfTerm(triple.getSubject(), termBuilder, writerDict));
        tripleBuilder.setP(rdfTerm(triple.getPredicate(), termBuilder, writerDict));
        tripleBuilder.setO(rdfTerm(triple.getObject(), termBuilder, writerDict));
        return tripleBuilder.build();
    }

    public static RDF_Quad rdfQuad(Quad quad, RDF_Quad.Builder quadBuilder, RDF_Term.Builder termBuilder,
                                   StringDictionaryWriter writerDict) {
        quadBuilder.clear();
        if ( quad.getGraph() != null )
            quadBuilder.setG(rdfTerm(quad.getGraph(), termBuilder, writerDict));
        quadBuilder.setS(rdfTerm(quad.getSubject(), termBuilder, writerDict));
        quadBuilder.setP(rdfTerm(quad.getPredicate(), termBuilder, writerDict));
        quadBuilder.setO(rdfTerm(quad.getObject(), termBuilder, writerDict));
        return quadBuilder.build();
    }

    static RDF_Term rdfTerm(Node node, RDF_Term.Builder termBuilder, StringDictionaryWriter writerDict) {
        termBuilder.clear();
        return Protobuf2Convert.toProtobuf(node, PMAP0, termBuilder, writerDict);
    }


    /** Visitor dispatch for {@link RDF_StreamRow} */
    static boolean visit(RDF_StreamRow x, VisitorStreamRowProto2RDF visitor) {
        if ( x == null )
            return false;
        visitor.visit(x.getStringsList());
        switch(x.getRowCase()) {
            case BASE :
                visitor.visit(x.getBase());
                return true;
            case PREFIXDECL :
                visitor.visit(x.getPrefixDecl());
                return true;
            case QUAD :
                visitor.visit(x.getQuad());
                return true;
            case TRIPLE :
                visitor.visit(x.getTriple());
                return true;
            case ROW_NOT_SET :
                throw new InternalErrorException();
        }
        return false;
    }
}

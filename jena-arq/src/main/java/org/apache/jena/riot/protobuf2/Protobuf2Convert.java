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


import org.apache.jena.atlas.lib.Cache;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.datatypes.xsd.impl.RDFLangString;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.protobuf2.wire.PB2_RDF.*;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.riot.system.RiotLib;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;

/** Convert to and from Protobuf wire objects.
 * <p>
 * See {@link StreamRDF2Protobuf2} and {@link Protobuf2StreamRDF}
 * for ways to convert as streams (they recycle intermediate objects).
 * <p>
 * Many operations have available with a cache.
 * The cache is used for creating URis from strings. It interns the node
 * leading to significant savings of space, especially in the property position.
 *
 * @see StreamRDF2Protobuf2
 * @see Protobuf2StreamRDF
 */
public class Protobuf2Convert
{
    private static RDF_UNDEF rdfUNDEF = RDF_UNDEF.newBuilder().build();
    private static RDF_Term UNDEF = RDF_Term.newBuilder().setUndefined(rdfUNDEF).build();

    private static RDF_ANY rdfANY = RDF_ANY.newBuilder().build();
    private static RDF_Term ANY = RDF_Term.newBuilder().setAny(rdfANY).build();

    private static String dtXSDString = XSDDatatype.XSDstring.getURI();

    private static final PrefixMap emptyPrefixMap = PrefixMapFactory.emptyPrefixMap() ;

    // ---- From Protobuf

    /** Build a {@link Node} from an {@link RDF_Term}. */
    public static Node convert(RDF_Term term, StringDictionaryReader readerDict) {
        return convert(null, term, readerDict) ;
    }

    /** Build a {@link Node} from an {@link RDF_Term}. */
    public static Node convert(Cache<String, Node> uriCache, RDF_Term term, StringDictionaryReader readerDict) {
        return convert(uriCache, term, null, readerDict) ;
    }

    // Create URI, using a cached copy if possible.
    private static Node uri(Cache<String, Node> uriCache, String uriStr) {
        if ( uriCache != null )
            return uriCache.get(uriStr, RiotLib::createIRIorBNode);
        return RiotLib.createIRIorBNode(uriStr);
    }


    /**
     * Build a {@link Node} from an {@link RDF_Term} using a prefix map which must agree
     * with the map used to create the {@code RDF_Term} in the first place.
     */
    public static Node convert(RDF_Term term, PrefixMap pmap, StringDictionaryReader readerDict) {
        return convert(null, term, pmap, readerDict);
    }

    /**
     * Build a {@link Node} from an {@link RDF_Term} using a prefix map which must agree
     * with the map used to create the {@code RDF_Term} in the first place.
     */
    public static Node convert(Cache<String, Node> uriCache, RDF_Term term, PrefixMap pmap,
                               StringDictionaryReader readerDict) {
        switch (term.getTermCase()) {
            case IRI :
                return uri(uriCache, readerDict.get(term.getIri().getIri())) ;
            case BNODE :
                return NodeFactory.createBlankNode(readerDict.get(term.getBnode().getLabel())) ;
            case LITERAL : {
                if ( term.hasLiteral() ) {
                    RDF_Literal lit = term.getLiteral() ;
                    String lex = readerDict.get(lit.getLex()) ;
                    switch ( lit.getLiteralKindCase() ) {
                        case SIMPLE :
                            return NodeFactory.createLiteralString(lex) ;
                        case LANGTAG : {
                            String lang = readerDict.get(lit.getLangtag());
                            return NodeFactory.createLiteralLang(lex, lang) ;
                        }
                        case DATATYPE : {
                            String dtString = readerDict.get(lit.getDatatype());
                            RDFDatatype dt = NodeFactory.getType(dtString) ;
                            return NodeFactory.createLiteral(lex, dt) ;
                        }
                        case DTPREFIX : {
                            String x = expand(lit.getDtPrefix(), pmap, readerDict);
                            if ( x == null )
                                throw new RiotProtobuf2Exception("Failed to expand datatype prefix name: "+lit.getDtPrefix()) ;
                            RDFDatatype dt = NodeFactory.getType(x) ;
                            return NodeFactory.createLiteral(lex, dt) ;
                        }
                        case LITERALKIND_NOT_SET :
                            throw new RiotProtobuf2Exception("Literal kind not set.");
                    }
                }
            }
            case PREFIXNAME : {
                String x = expand(term.getPrefixName(), pmap, readerDict) ;
                if ( x != null )
                    return uri(uriCache, x) ;
                throw new RiotProtobuf2Exception("Failed to expand "+term) ;
            }
            case VARIABLE :
                return Var.alloc(readerDict.get(term.getVariable().getName())) ;
            case TRIPLETERM : {
                RDF_Triple rt = term.getTripleTerm();
                Triple t = convert(rt, pmap, readerDict);
                return NodeFactory.createTripleNode(t);
            }
            case ANY :
                return Node.ANY ;
            case REPEAT :
                throw new RiotProtobuf2Exception("REPEAT not implemented") ;
            case UNDEFINED :
                return null;
            case TERM_NOT_SET :
                throw new RiotProtobuf2Exception("RDF_Term not set") ;
            default:
                throw new RiotProtobuf2Exception("No conversion to a Node: "+term.toString()) ;
        }
    }

    public static Triple convert(RDF_Triple triple, StringDictionaryReader readerDict) {
        return convert(null, triple, null, readerDict);
    }

    public static Triple convert(Cache<String, Node> uriCache, RDF_Triple triple, StringDictionaryReader readerDict) {
        return convert(uriCache, triple, null, readerDict) ;
    }

    public static Triple convert(RDF_Triple rt, PrefixMap pmap, StringDictionaryReader readerDict) {
        return convert(null, rt, pmap, readerDict);
    }

    public static Triple convert(Cache<String, Node> uriCache, RDF_Triple rt, PrefixMap pmap,
                                 StringDictionaryReader readerDict) {
        Node s = convert(uriCache, rt.getS(), pmap, readerDict) ;
        Node p = convert(uriCache, rt.getP(), pmap, readerDict) ;
        Node o = convert(uriCache, rt.getO(), pmap, readerDict) ;
        return Triple.create(s, p, o) ;
    }

    public static Quad convert(RDF_Quad rq, PrefixMap pmap, StringDictionaryReader readerDict) {
        return convert(null, rq, pmap, readerDict);
    }

    public static Quad convert(Cache<String, Node> uriCache, RDF_Quad rq, PrefixMap pmap,
                               StringDictionaryReader readerDict) {
        Node g = (rq.hasG() ? convert(uriCache,rq.getG(), pmap, readerDict) : null ) ;
        Node s = convert(uriCache,rq.getS(), pmap, readerDict) ;
        Node p = convert(uriCache,rq.getP(), pmap, readerDict) ;
        Node o = convert(uriCache,rq.getO(), pmap, readerDict) ;
        return Quad.create(g, s, p, o) ;
    }

    /**
     * Encode a {@link Node} into an {@link RDF_Term},
     * using values (integer, decimal, double) if possible.
     */
    public static RDF_Term toProtobuf(Node node, RDF_Term.Builder term, StringDictionaryWriter writerDict) {
        return toProtobuf(node, emptyPrefixMap, term, writerDict);
    }

    /** Encode a {@link Node} into an {@link RDF_Term} */
    public static RDF_Term toProtobuf(Node node, PrefixMap pmap, RDF_Term.Builder termBuilder,
                                      StringDictionaryWriter writerDict) {
        if ( node == null)
            return UNDEF;

        if ( node.isURI() ) {
            RDF_PrefixName prefixName = abbrev(node.getURI(), pmap, writerDict) ;
            if ( prefixName != null ) {
                termBuilder.setPrefixName(prefixName) ;
                return termBuilder.build();
            }
            RDF_IRI iri = RDF_IRI.newBuilder()
                    .setIri(writerDict.getIndex(node.getURI()))
                    .build() ;
            return termBuilder.setIri(iri).build();
        }

        if ( node.isBlank() ) {
            RDF_BNode b = RDF_BNode.newBuilder()
                    .setLabel(writerDict.getIndex(node.getBlankNodeLabel()))
                    .build();
            return termBuilder.setBnode(b).build();
        }

        if ( node.isLiteral() ) {
            RDF_Literal.Builder literal = RDF_Literal.newBuilder();
            String lex = node.getLiteralLexicalForm() ;
            literal.setLex(writerDict.getIndex(lex));

            // Protobuf default string is ""

            String dt = node.getLiteralDatatypeURI() ;
            String lang = node.getLiteralLanguage() ;
            if ( lang.isEmpty() )
                lang = null;

            // General encoding.
            if ( node.getLiteralDatatype().equals(XSDDatatype.XSDstring) ||
                    node.getLiteralDatatype().equals(RDFLangString.rdfLangString) ) {
                dt = null ;
            }

            if ( dt == null && lang == null ) {
                literal.setSimple(true);
            } else if ( dt != null ) {
                RDF_PrefixName dtPrefixName = abbrev(dt, pmap, writerDict) ;
                if ( dtPrefixName != null )
                    literal.setDtPrefix(dtPrefixName) ;
                else {
                    literal.setDatatype(writerDict.getIndex(dt)) ;
                }
            } else {
                //if ( lang != null && ! lang.isEmpty() )
                literal.setLangtag(writerDict.getIndex(lang));
            }
            termBuilder.setLiteral(literal) ;
            return termBuilder.build();
        }

        if ( node.isVariable() ) {
            RDF_Var var = RDF_Var.newBuilder()
                    .setName(writerDict.getIndex(node.getName()))
                    .build();
            return termBuilder.setVariable(var).build();
        }

        if ( node.isNodeTriple() ) {
            Triple triple = node.getTriple();

            RDF_Term sTerm = toProtobuf(triple.getSubject(), pmap, termBuilder, writerDict);
            termBuilder.clear();

            RDF_Term pTerm = toProtobuf(triple.getPredicate(), pmap, termBuilder, writerDict);
            termBuilder.clear();

            RDF_Term oTerm = toProtobuf(triple.getObject(), pmap, termBuilder, writerDict);
            termBuilder.clear();

            RDF_Triple tripleTerm = RDF_Triple.newBuilder().setS(sTerm).setP(pTerm).setO(oTerm).build();
            termBuilder.setTripleTerm(tripleTerm);
            return termBuilder.build();
        }

        if ( Node.ANY.equals(node))
            return ANY;

        throw new RiotProtobuf2Exception("Node conversion not supported: "+node) ;
    }

    private static String expand(RDF_PrefixName prefixName, PrefixMap pmap, StringDictionaryReader readerDict) {
        if ( pmap == null )
            return null ;

        String prefix = readerDict.get(prefixName.getPrefix()) ;
        String localname  = readerDict.get(prefixName.getLocalName()) ;
        String x = pmap.expand(prefix, localname) ;
        if ( x == null )
            throw new RiotProtobuf2Exception("Failed to expand "+prefixName) ;
        return x ;
    }


    public static RDF_Term convert(Node node, StringDictionaryWriter writerDict) {
        return convert(node, null, writerDict) ;
    }

    public static RDF_Term convert(Node node, PrefixMap pmap, StringDictionaryWriter writerDict) {
        RDF_Term.Builder n = RDF_Term.newBuilder();
        return toProtobuf(node, pmap, n, writerDict) ;
    }

    /** Produce a {@link RDF_PrefixName} is possible. */
    private static RDF_PrefixName abbrev(String uriStr, PrefixMap pmap, StringDictionaryWriter writerDict) {
        if ( pmap == null )
            return null ;
        Pair<String, String> p = pmap.abbrev(uriStr) ;
        if ( p == null )
            return null ;
        return RDF_PrefixName.newBuilder()
                .setPrefix(writerDict.getIndex(p.getLeft()))
                .setLocalName(writerDict.getIndex(p.getRight()))
                .build();
    }

    public static RDF_Triple convert(Triple triple, StringDictionaryWriter writerDict) {
        return convert(triple, null, writerDict) ;
    }

    public static RDF_Triple convert(Triple triple, PrefixMap pmap, StringDictionaryWriter writerDict) {
        RDF_Triple.Builder t = RDF_Triple.newBuilder();
        RDF_Term s = convert(triple.getSubject(), pmap, writerDict) ;
        RDF_Term p = convert(triple.getPredicate(), pmap, writerDict) ;
        RDF_Term o = convert(triple.getObject(), pmap, writerDict) ;
        t.setS(s) ;
        t.setP(p) ;
        t.setO(o) ;
        return t.build() ;
    }

    public static Quad convert(RDF_Quad quad, StringDictionaryReader readerDict) {
        return convert(quad, null, readerDict) ;
    }

    public static RDF_Quad convert(Quad quad, StringDictionaryWriter writerDict) {

        return convert(quad, null, writerDict) ;
    }

    public static RDF_Quad convert(Quad quad, PrefixMap pmap, StringDictionaryWriter writerDict) {
        RDF_Quad.Builder q = RDF_Quad.newBuilder();
        RDF_Term g = null ;
        if ( quad.getGraph() != null )
            g = convert(quad.getGraph(), pmap, writerDict) ;
        RDF_Term s = convert(quad.getSubject(), pmap, writerDict) ;
        RDF_Term p = convert(quad.getPredicate(), pmap, writerDict) ;
        RDF_Term o = convert(quad.getObject(), pmap, writerDict) ;
        if ( g != null )
            q.setG(g) ;
        q.setS(s) ;
        q.setP(p) ;
        q.setO(o) ;
        return q.build() ;
    }

//    /**
//     * Serialize the {@link RDF_Term} into a byte array.
//     * <p>
//     * Where possible, to is better to serialize into a stream, directly using {@code term.write(TProtocol)}.
//     */
//    public static byte[] termToBytes(RDF_Term term) {
//        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
//        try {
//            return serializer.serialize(term);
//        }
//        catch (TException e) {
//            throw new RiotProtobufException(e);
//        }
//    }
//
//    /**
//     * Deserialize from a byte array into an {@link RDF_Term}.
//     * <p>
//     * Where possible, to is better to deserialize from a stream, directly using {@code term.read(TProtocol)}.
//     */
//    public static RDF_Term termFromBytes(byte[] bytes) {
//        RDF_Term term = new RDF_Term();
//        termFromBytes(term, bytes);
//        return term;
//    }
//
//    /**
//     * Deserialize from a byte array into an {@link RDF_Term}.
//     * <p>
//     * Where possible, to is better to deserialize from a stream, directly using {@code term.read(TProtocol)}.
//     */
//    public static void termFromBytes(RDF_Term term, byte[] bytes) {
//        TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
//        try {
//            deserializer.deserialize(term, bytes);
//        }
//        catch (TException e) { throw new RiotProtobufException(e); }
//    }

    // RDF_Tuple => RDF_row (for result sets) or List<RDFTerm>

//    public static Tuple<Node> convert(RDF_Tuple row) {
//        return convert(row, null) ;
//    }
//
//    public static Tuple<Node> convert(RDF_Tuple row, PrefixMap pmap) {
//        List<RDF_Term> terms = row.getTerms() ;
//        Node[] tuple = new Node[terms.size()] ;
//        int idx = 0 ;
//        for ( RDF_Term rt : terms ) {
//            tuple[idx] = convert(rt, pmap) ;
//            idx ++ ;
//        }

//        return Tuple.create(tuple) ;
//    }
//
//    public static RDF_Tuple convert(Tuple<Node> tuple) {
//        return convert(tuple, null) ;
//    }
//
//    public static RDF_Tuple convert(Tuple<Node> tuple, PrefixMap pmap) {
//        RDF_Tuple rTuple = new RDF_Tuple() ;
//        for ( Node n : tuple ) {
//            RDF_Term rt = convert(n, pmap) ;
//            rTuple.addToTerms(rt) ;
//        }
//        return rTuple ;
//    }
}

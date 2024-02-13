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

import org.apache.jena.atlas.lib.Cache;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.datatypes.xsd.impl.RDFLangString;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.riot.system.RiotLib;
import org.apache.jena.riot.thrift3.wire.*;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import java.math.BigInteger;

import static org.apache.jena.riot.thrift3.T3RDF.ANY;

/** Convert to and from Thrift wire objects.
 * See {@link StreamRDF2Thrift3} and {@link Thrift2StreamRDF}
 * for ways to convert as streams (they recycle intermediate objects).
 * <p>
 * Many operations have available with a cache.
 * The cache is used for creating URis from strings. It interns the node
 * leading to significant savings of space, especially in the property position.
 * <p>
 * @see StreamRDF2Thrift3
 * @see Thrift2StreamRDF
 */
public class Thrift3Convert
{

    /**
     * Encode a {@link Node} into an {@link RDF_Term},
     * using values (integer, decimal, double) if possible.
     */
    public static void toThrift(Node node, RDF_Term term, StringDictionaryWriter writerDict) {
        toThrift(node, emptyPrefixMap, term, writerDict);
    }

    /** Encode a {@link Node} into an {@link RDF_Term} */
    public static void toThrift(Node node, PrefixMap pmap, RDF_Term term, StringDictionaryWriter writerDict) {
        if ( node == null) {
            term.setUndefined(T3RDF.UNDEF);
            return;
        }

        if ( node.isURI() ) {
            RDF_PrefixName prefixName = abbrev(node.getURI(), pmap, writerDict);
            if ( prefixName != null ) {
                term.setPrefixName(prefixName);
                return;
            }
        }

        if ( node.isBlank() ) {
            RDF_BNode b = new RDF_BNode(writerDict.getIndex(node.getBlankNodeLabel()));
            term.setBnode(b);
            return;
        }

        if ( node.isURI() ) {
            RDF_IRI iri = new RDF_IRI(writerDict.getIndex(node.getURI()));
            term.setIri(iri);
            return;
        }

        if ( node.isLiteral() ) {

            String lex = node.getLiteralLexicalForm();
            String dt = node.getLiteralDatatypeURI();
            String lang = node.getLiteralLanguage();

            // General encoding.
            RDF_Literal literal = new RDF_Literal(writerDict.getIndex(lex));
            if ( node.getLiteralDatatype().equals(XSDDatatype.XSDstring) ||
                    node.getLiteralDatatype().equals(RDFLangString.rdfLangString) ) {
                dt = null;
            }

            if ( dt != null ) {
                RDF_PrefixName dtPrefixName = abbrev(dt, pmap, writerDict);
                if ( dtPrefixName != null )
                    literal.setDtPrefix(dtPrefixName);
                else
                    literal.setDatatype(writerDict.getIndex(dt));
            }
            if ( lang != null && ! lang.isEmpty() )
                literal.setLangtag(writerDict.getIndex(lang));
            term.setLiteral(literal);
            return;
        }

        if ( node.isVariable() ) {
            RDF_VAR var = new RDF_VAR(writerDict.getIndex(node.getName()));
            term.setVariable(var);
            return;
        }

        if ( node.isNodeTriple() ) {
            Triple triple = node.getTriple();

            RDF_Term sTerm = new RDF_Term();
            toThrift(triple.getSubject(), pmap, sTerm, writerDict);

            RDF_Term pTerm = new RDF_Term();
            toThrift(triple.getPredicate(), pmap, pTerm, writerDict);

            RDF_Term oTerm = new RDF_Term();
            toThrift(triple.getObject(), pmap, oTerm, writerDict);

            RDF_Triple tripleTerm = new RDF_Triple(sTerm, pTerm, oTerm);
            term.setTripleTerm(tripleTerm);
            return;
        }

        if ( Node.ANY.equals(node)) {
            term.setAny(ANY);
            return;
        }
        throw new RiotThrift3Exception("Node conversion not supported: "+node);
    }

    private static final PrefixMap emptyPrefixMap = PrefixMapFactory.emptyPrefixMap();

    /** Build a {@link Node} from an {@link RDF_Term}. */
    public static Node convert(RDF_Term term, StringDictionaryReader dict) {
        return convert(null, term, null, dict);
    }

    /** Build a {@link Node} from an {@link RDF_Term}. */
    public static Node convert(Cache<String, Node> uriCache, RDF_Term term, StringDictionaryReader dict) {
        return convert(uriCache, term, null, dict);
    }

    /** Build a {@link Node} from an {@link RDF_Term}. */
    public static Node convert(RDF_Term term, PrefixMap pmap, StringDictionaryReader dict) {
        return convert(null, term, pmap, dict);
    }

    /**
     * Build a {@link Node} from an {@link RDF_Term} using a prefix map which must agree
     * with the map used to create the {@code RDF_Term} in the first place.
     */
    public static Node convert(Cache<String, Node> uriCache, RDF_Term term, PrefixMap pmap, StringDictionaryReader dict) {
        if ( term.isSetPrefixName() ) {
            String x = expand(term.getPrefixName(), pmap, dict);
            if ( x != null )
                return uri(uriCache, x);
            throw new RiotThrift3Exception("Failed to expand "+term);
        }

        if ( term.isSetIri() ) {
            String s = dict.get(term.getIri().getIri());
            return uri(uriCache, s);
        }

        if ( term.isSetBnode() )
            return NodeFactory.createBlankNode(dict.get(term.getBnode().getLabel()));

        if ( term.isSetLiteral() ) {
            RDF_Literal lit = term.getLiteral();
            String lex = dict.get(lit.getLex());
            String dtString = null;
            if ( lit.isSetDatatype() )
                dtString = dict.get(lit.getDatatype());
            else if ( lit.isSetDtPrefix()) {
                String x = expand(lit.getDtPrefix(), pmap, dict);
                if ( x == null )
                    throw new RiotThrift3Exception("Failed to expand datatype prefix name:"+term);
                dtString = x;
            }
            RDFDatatype dt = NodeFactory.getType(dtString);

            String lang = lit.isSetLangtag() ? dict.get(lit.getLangtag()) : null;
            return NodeFactory.createLiteral(lex, lang, dt);
        }

        if ( term.isSetTripleTerm() ) {
            RDF_Triple rt = term.getTripleTerm();
            Triple t = convert(rt, pmap, dict);
            return NodeFactory.createTripleNode(t);
        }

        if ( term.isSetVariable() )
            return Var.alloc(dict.get(term.getVariable().getName()));

        if ( term.isSetAny() )
            return Node.ANY;

        if ( term.isSetUndefined() )
            return null;

        throw new RiotThrift3Exception("No conversion to a Node: "+term.toString());
    }

    // Create URI, using a cached copy if possible.
    private static Node uri(Cache<String, Node> uriCache, String uriStr) {
        if ( uriCache != null )
            return uriCache.get(uriStr, RiotLib::createIRIorBNode);
        return RiotLib.createIRIorBNode(uriStr);
    }

    private static String expand(RDF_PrefixName prefixName, PrefixMap pmap, StringDictionaryReader dict) {
        if ( pmap == null )
            return null;

        String prefix = dict.get(prefixName.getPrefix());
        String localname = dict.get(prefixName.getLocalName());
        String x = pmap.expand(prefix, localname);
        if ( x == null )
            throw new RiotThrift3Exception("Failed to expand "+prefixName);
        return x;
    }

    public static RDF_Term convert(Node node, StringDictionaryWriter dict) {
        return convert(node, null, dict);
    }

    public static RDF_Term convert(Node node, PrefixMap pmap, StringDictionaryWriter dict) {
        RDF_Term n = new RDF_Term();
        toThrift(node, pmap, n, dict);
        return n;
    }

    static final BigInteger MAX_I = BigInteger.valueOf(Long.MAX_VALUE);
    static final BigInteger MIN_I = BigInteger.valueOf(Long.MIN_VALUE);

    /** Produce a {@link RDF_PrefixName} is possible. */
    private static RDF_PrefixName abbrev(String uriStr, PrefixMap pmap, StringDictionaryWriter dictionaryWriter) {
        if ( pmap == null )
            return null;
        Pair<String, String> p = pmap.abbrev(uriStr);
        if ( p == null )
            return null;
        return new RDF_PrefixName(dictionaryWriter.getIndex(p.getLeft()), dictionaryWriter.getIndex(p.getRight()));
    }

    public static Triple convert(RDF_Triple triple, StringDictionaryReader dict) {
        return convert(triple, null, dict);
    }

    public static Triple convert(RDF_Triple rt, PrefixMap pmap, StringDictionaryReader dict) {
        return convert(null, rt, pmap, dict);
    }

    public static Triple convert(Cache<String, Node> uriCache, RDF_Triple rt, PrefixMap pmap, StringDictionaryReader dict) {
        Node s = convert(uriCache, rt.getS(), pmap, dict);
        Node p = convert(uriCache, rt.getP(), pmap, dict);
        Node o = convert(uriCache, rt.getO(), pmap, dict);
        return Triple.create(s, p, o);
    }

    public static RDF_Triple convert(Triple triple, StringDictionaryWriter dictionaryWriter) {
        return convert(triple, dictionaryWriter);
    }

    public static RDF_Triple convert(Triple triple, PrefixMap pmap, StringDictionaryWriter dictionaryWriter) {
        RDF_Triple t = new RDF_Triple();
        RDF_Term s = convert(triple.getSubject(), pmap, dictionaryWriter);
        RDF_Term p = convert(triple.getPredicate(), pmap, dictionaryWriter);
        RDF_Term o = convert(triple.getObject(), pmap, dictionaryWriter);
        t.setS(s);
        t.setP(p);
        t.setO(o);
        return t;
    }

    public static Quad convert(RDF_Quad quad, StringDictionaryReader dict) {
        return convert(quad, null, dict);
    }

    public static Quad convert(RDF_Quad rq, PrefixMap pmap, StringDictionaryReader dict) {
        return convert(null, rq, pmap, dict);
    }

    public static Quad convert(Cache<String, Node> uriCache, RDF_Quad rq, PrefixMap pmap, StringDictionaryReader dict) {
        Node g = (rq.isSetG() ? convert(uriCache, rq.getG(), pmap, dict) : null );
        Node s = convert(uriCache, rq.getS(), pmap, dict);
        Node p = convert(uriCache, rq.getP(), pmap, dict);
        Node o = convert(uriCache, rq.getO(), pmap, dict);
        return Quad.create(g, s, p, o);
    }

    public static RDF_Quad convert(Quad quad, StringDictionaryWriter writerDict) {
        return convert(quad, null, writerDict);
    }

    public static RDF_Quad convert(Quad quad, PrefixMap pmap, StringDictionaryWriter writerDict) {
        RDF_Quad q = new RDF_Quad();
        RDF_Term g = null;
        if ( quad.getGraph() != null )
            g = convert(quad.getGraph(), pmap, writerDict);
        RDF_Term s = convert(quad.getSubject(), pmap, writerDict);
        RDF_Term p = convert(quad.getPredicate(), pmap, writerDict);
        RDF_Term o = convert(quad.getObject(), pmap, writerDict);
        if ( g != null )
            q.setG(g);
        q.setS(s);
        q.setP(p);
        q.setO(o);
        return q;
    }

    /**
     * Serialize the {@link RDF_Term} into a byte array.
     * <p>
     * Where possible, to is better to serialize into a stream, directly using {@code term.write(TProtocol)}.
     */
    public static byte[] termToBytes(RDF_Term term) {
        try {
            TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
            return serializer.serialize(term);
        }
        catch (TException e) {
            throw new RiotThrift3Exception(e);
        }
    }

    /**
     * Deserialize from a byte array into an {@link RDF_Term}.
     * <p>
     * Where possible, to is better to deserialize from a stream, directly using {@code term.read(TProtocol)}.
     */
    public static RDF_Term termFromBytes(byte[] bytes) {
        RDF_Term term = new RDF_Term();
        termFromBytes(term, bytes);
        return term;
    }

    /**
     * Deserialize from a byte array into an {@link RDF_Term}.
     * <p>
     * Where possible, to is better to deserialize from a stream, directly using {@code term.read(TProtocol)}.
     */
    public static void termFromBytes(RDF_Term term, byte[] bytes) {
        try {
            TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
            deserializer.deserialize(term, bytes);
        }
        catch (TException e) { throw new RiotThrift3Exception(e); }
    }
}

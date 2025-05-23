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

package org.apache.jena.graph;


import static org.apache.jena.atlas.lib.Lib.isEmpty;
import static org.apache.jena.langtagx.LangTagX.formatLanguageTag;

import java.util.Objects;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.datatypes.xsd.impl.RDFDirLangString;
import org.apache.jena.datatypes.xsd.impl.RDFLangString;
import org.apache.jena.graph.impl.LiteralLabel;
import org.apache.jena.graph.impl.LiteralLabelFactory;
import org.apache.jena.shared.JenaException;
import org.apache.jena.sys.JenaSystem;

public class NodeFactory {

    static { JenaSystem.init(); }
    private NodeFactory() {}

    public static RDFDatatype getType(String s) {
        if ( s == null )
            return null;
        return TypeMapper.getInstance().getSafeTypeByName(s);
    }

    /** Make a fresh blank node */
    public static Node createBlankNode() {
        return createBlankNode(BlankNodeId.createFreshId());
    }

    /** make a blank node with the specified label */
    public static Node createBlankNode(String string) {
        return new Node_Blank(string);
    }

    /** make a URI node with the specified URIref string */
    public static Node createURI(String uri) {
        Objects.requireNonNull(uri, "Argument to NodeFactory.createURI is null");
        return new Node_URI(uri);
    }

    /** make a variable node with a given name */
    public static Node createVariable(String name) {
        Objects.requireNonNull(name, "Argument to NodeFactory.createVariable is null");
        return new Node_Variable(name);
    }

    /** make an extension node based on a string. */
    public static Node createExt(String name) {
        Objects.requireNonNull(name, "Argument to NodeFactory.createExt is null");
        return new Node_Marker(name);
    }

    /**
     * Make a literal node with the specified literal value
     * @deprecated Making nodes directly from {@link LiteralLabel} may be removed.
     */
    @Deprecated
    public static Node createLiteral(LiteralLabel lit) {
        Objects.requireNonNull(lit, "Argument to NodeFactory.createLiteral is null");
        return new Node_Literal( lit );
    }

    /** @deprecated Use {@link #createLiteralString} */
    @Deprecated(forRemoval = true)
    public static Node createLiteral(String string) {
        return createLiteralString(string);
    }

    /*
     * Make literal which is a string (xsd:string)
     */
    public static Node createLiteralString(String string) {
        Objects.requireNonNull(string, "Argument to NodeFactory.createLiteralString is null");
        return new Node_Literal(string);
    }

    /**
     * Make a literal with specified language. The lexical form must not be null.
     *
     * @param string  the lexical form of the literal
     * @param lang    the optional language tag
     *
     * @deprecated Use {@link #createLiteralLang(String, String)}.
     */
    @Deprecated(forRemoval = true)
    public static Node createLiteral(String string, String lang) {
        return createLiteralLang(string, lang);
    }

    /**
     * Make a literal with specified language. The lexical form must not be null.
     * <p>
     * If the {@code lang} contains "--" it is interpreted as including a base direction.
     *
     * @param string  the lexical form of the literal
     * @param lang    the optional language tag
     */
    public static Node createLiteralLang(String string, String lang) {
        Objects.requireNonNull(string, "null lexical form for literal");
        if ( isEmpty(lang) )
            return new Node_Literal(string);

        int idx = lang.indexOf("--");
        if ( idx >= 0 ) {
            String textDir = lang.substring(idx+2);
            if ( textDir.isEmpty() )
                throw new JenaException("Empty base direction after '--'");
            lang = lang.substring(0, idx);
            return createLiteralDirLang(string, lang, textDir);
        }
        String langFmt = formatLanguageTag(lang);
        return new Node_Literal(string, langFmt);
    }

    /**
     * Make a literal with specified language and language direction.
     * The lexical form must not be null.
     * The language must not be null or "" if a non-direction is provided.
     *
     * @param string  the lexical form of the literal
     * @param lang    the optional language tag
     * @param textDir the optional language direction
     */
    public static Node createLiteralDirLang(String string, String lang, String textDir) {
        TextDirection textDirEnum = initialTextDirection(textDir);
        return createLiteralDirLang(string, lang, textDirEnum);
    }

    private static boolean noTextDir(TextDirection textDir) {
        return textDir == Node.noTextDirection;
    }

    public static Node createLiteralDirLang(String string, String lang, TextDirection textDir) {
        Objects.requireNonNull(string, "null lexical form for literal");
        if ( isEmpty(lang) ) {
            if ( textDir != null )
                throw new JenaException("The language must be given for a language direction literal");
            return new Node_Literal(string);
        }
        if ( noTextDir(textDir) )
            return new Node_Literal(string, lang);
        String langFmt = formatLanguageTag(lang);
        return new Node_Literal(string, langFmt, textDir);
    }

    /**
     * Build a literal node.
     * <p>
     * This is a convenience operation for passing in language and datatype without
     * needing the caller to differentiate between the xsd:string, rdf:langString, rdf:dirLangString
     * and other datatype cases.
     * <p>
     * It calls {@link #createLiteralString(String)},
     * {@link #createLiteralDirLang(String, String, String)} or
     * {@link #createLiteralDT(String, RDFDatatype)}
     * as appropriate.
     *
     * @param lex the lexical form of the literal
     * @param lang the optional language tag or null or ""
     * @param dtype the type of the literal or null.
     */
    public static Node createLiteral(String lex, String lang, RDFDatatype dtype) {
        return createLiteralInternal(lex, lang, Node.noTextDirection, dtype);
    }

    /**
     * Build a literal node.
     * <p>
     * This is a convenience operation for passing in language and datatype without
     * needing the caller to differentiate between the xsd:string, rdf:langString, and other
     * datatype cases.
     * <p>
     * It calls {@link #createLiteralString(String)},
     * {@link #createLiteralDirLang(String, String, String)} or
     * {@link #createLiteralDT(String, RDFDatatype)}
     * as appropriate.
     *
     * @param lex the lexical form of the literal
     * @param lang the optional language tag or null or ""
     * @param textDir the optional language direction or null
     * @param dtype the type of the literal or null.
     */
    public static Node createLiteral(String lex, String lang, String textDir, RDFDatatype dtype) {
        TextDirection textDirEnum = initialTextDirection(textDir);
        return createLiteralInternal(lex, lang, textDirEnum, dtype);
    }

    /**
     * Build a literal node.
     * <p>
     * This is a convenience operation for passing in language and datatype without
     * needing the caller to differentiate between the xsd:string, rdf:langString, and other
     * datatype cases.
     * It calls {@link #createLiteralString(String)},
     * {@link #createLiteralLang(String, String)} or
     * {@link #createLiteralDirLang(String, String, String)} or
     * {@link #createLiteralDT(String, RDFDatatype)}
     * as appropriate.
     *
     * @param lex the lexical form of the literal
     * @param lang the optional language tag or null or ""
     * @param textDir the optional language direction or null
     * @param dtype the type of the literal or null.
     */
    public static Node createLiteral(String lex, String lang, TextDirection textDir, RDFDatatype dtype) {
        return createLiteralInternal(lex, lang, textDir, dtype);
    }

    /**
     * Make a literal from any legal combination language tag, base direction and datatype.
     * Any of these can be null when not needed.
     */
    private static Node createLiteralInternal(String lex, String lang, TextDirection textDir, RDFDatatype dtype) {
        Objects.requireNonNull(lex, "null lexical form for literal");
        boolean hasLang = ! isEmpty(lang);
        boolean hasTextDirLang = ! noTextDir(textDir);

        if ( hasTextDirLang && ! hasLang )
            throw new JenaException("The language must be given for a language direction literal");

        // Datatype check when lang present.
        if ( hasLang ) {
            if ( ! hasTextDirLang ) {
                if ( dtype != null && ! dtype.equals(RDFLangString.rdfLangString) )
                    throw new JenaException("Datatype is not rdf:langString but a language was given");
                return createLiteralLang(lex, lang);
            }
            // hasLang && hasTextDirLang
            if ( dtype != null && ! dtype.equals(RDFDirLangString.rdfDirLangString) )
                throw new JenaException("Datatype is not rdf:dirLangString but a language and initial text direction was given");
            return createLiteralDirLang(lex, lang, textDir);
        }

        // no language tag, no text direction, no datatype.
        if ( dtype == null )
            // No datatype, no lang (it is null or "") => xsd:string.
            return createLiteralString(lex);

        // Datatype. No language, no initial text direction.
        // Allow "abc"^^rdf:langString
        // Allow "abc"^^rdf:dirLangString

        // To disallow.
//        if ( dtype.equals(RDFLangString.rdfLangString) )
//            throw new JenaException("Datatype is rdf:langString but no language given");
//        if ( dtype.equals(RDFDirLangString.rdfDirLangString) && noTextDir(textDir) )
//            throw new JenaException("Datatype is rdf:dirLangString but no ibase direction given");

        Node n = createLiteralDT(lex, dtype);
        return n;
    }

    /** Prepare the initial text direction - apply formatting normalization */
    private static TextDirection initialTextDirection(String input) {
        if ( isEmpty(input) )
            return Node.noTextDirection;
        // Throws JenaException on bad input.
        // If there is formatting normalization, it happens here.
        // RDF 1.2 strictly is 'ltr', 'rtl' only.
        TextDirection textDir = TextDirection.create(input);
        return textDir;
    }

    /** @deprecated Use {@link #createLiteralDT(String, RDFDatatype)} */
    @Deprecated
    public static Node createLiteral(String lex, RDFDatatype dtype) {
        return createLiteralDT(lex, dtype);
    }

    /**
     * Build a typed literal node from its lexical form.
     *
     * @param lex
     *            the lexical form of the literal
     * @param dtype
     *            the type of the literal
     */
    public static Node createLiteralDT(String lex, RDFDatatype dtype) {
        Objects.requireNonNull(lex, "null lexical form for literal");
        if ( dtype == null )
            dtype = XSDDatatype.XSDstring;
        return new Node_Literal(lex, dtype);
    }

    /** Create a Node based on the value
     * If the value is a string we
     * assume this is intended to be a lexical form after all.
     * @param value The value, mapped according to registered types.
     * @return Node
     */
    public static Node createLiteralByValue(Object value) {
        Objects.requireNonNull(value, "Argument 'value' to NodeFactory.createLiteralByValue is null");
        return new Node_Literal(LiteralLabelFactory.createByValue(value));
    }

    /** Create a Node based on the value
     * If the value is a string we
     * assume this is intended to be a lexical form after all.
     * @param value The value, mapped according to registered types.
     * @param dtype RDF Datatype.
     * @return Node
     */
    public static Node createLiteralByValue(Object value, RDFDatatype dtype) {
        Objects.requireNonNull(value, "Argument 'value' to NodeFactory.createLiteralByValue is null");
        return new Node_Literal(LiteralLabelFactory.createByValue(value, dtype));
    }

    /** Create a triple node (RDF-star) */
    public static Node createTripleTerm(Node s, Node p, Node o) {
        return new Node_Triple(s, p, o);
    }

    /** Create a triple term (RDF-star) */
    public static Node createTripleTerm(Triple triple) {
        return new Node_Triple(triple);
    }

    /**
     * Create a triple term (RDF-star)
     * @deprecated Use {@link #createTripleTerm(Node, Node, Node)}
     */
    @Deprecated
    public static Node createTripleNode(Node s, Node p, Node o) {
        return createTripleTerm(s, p, o);
    }

    /**
     * Create a triple node (RDF-star)
     * @deprecated Use {@link #createTripleTerm(Triple)}
     */
    @Deprecated
    public static Node createTripleNode(Triple triple) {
        return createTripleTerm(triple);
    }

    /** Create a graph node. This is an N3-formula; it is not a named graph (see "quad") */
    public static Node createGraphNode(Graph graph) {
        return new Node_Graph(graph);
    }
}

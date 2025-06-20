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

package org.apache.jena.cimxml;

import com.github.jsonldjava.shaded.com.google.common.hash.Hashing;
import org.apache.commons.io.input.BufferedFileChannelInputStream;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.iri3986.provider.IRIProvider3986;
import org.apache.jena.irix.IRIProvider;
import org.apache.jena.irix.IRIx;
import org.apache.jena.mem2.collection.FastHashMap;
import org.apache.jena.mem2.collection.FastHashSet;
import org.apache.jena.riot.system.StreamRDF;

import javax.xml.XMLConstants;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class CIMParser {
    private static final char CHAR_SHARP = '#';
    private static final String STRING_EMPTY = "";
    private static final String STRING_NAMESPACE_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

    private static final int MAX_BUFFER_SIZE = 256*4096; // 256 KB
    private static final Charset UTF_8 = StandardCharsets.UTF_8;
    private static final byte LEFT_ANGLE_BRACKET = (byte)'<';
    private static final byte RIGHT_ANGLE_BRACKET = (byte) '>';
    private static final byte WHITESPACE_SPACE = (byte)' ';
    private static final byte WHITESPACE_TAB = (byte)'\t';
    private static final byte WHITESPACE_NEWLINE = (byte)'\n';
    private static final byte WHITESPACE_CARRIAGE_RETURN = (byte)'\r';
    private static final byte QUESTION_MARK = (byte)'?';
    private static final byte EXCLAMATION_MARK = (byte)'!';
    private static final byte EQUALITY_SIGN = (byte)'=';
    private static final byte DOUBLE_QUOTE = (byte)'"';
    private static final byte SINGLE_QUOTE = (byte)'\'';
    private static final byte SLASH = (byte)'/';
    private static final byte DOUBLE_COLON = (byte)':';
    private static final byte SHARP = (byte)'#';
    private static final byte UNDERSCORE = (byte)'_';
    private static final byte SEMICOLON = (byte)';';
    private static final byte AMPERSAND = (byte)'&';
    private static final byte END_OF_STREAM = -1;


    private static final ByteArrayKey ATTRIBUTE_XMLNS = new ByteArrayKey(XMLConstants.XMLNS_ATTRIBUTE);

    private static final ByteArrayKey ATTRIBUTE_RDF_ID = new ByteArrayKey("rdf:ID");
    private static final ByteArrayKey ATTRIBUTE_RDF_ABOUT = new ByteArrayKey("rdf:about");
    private static final ByteArrayKey ATTRIBUTE_RDF_NODE_ID = new ByteArrayKey("rdf:nodeID");

    private static final ByteArrayKey ATTRIBUTE_RDF_RESOURCE = new ByteArrayKey("rdf:resource");
    private static final ByteArrayKey ATTRIBUTE_RDF_PARSE_TYPE = new ByteArrayKey("rdf:parseType");
    private static final ByteArrayKey ATTRIBUTE_RDF_DATATYPE = new ByteArrayKey("rdf:datatype");

    private static final ByteArrayKey ATTRIBUTE_VALUE_RDF_PARSE_TYPE_COLLECTION = new ByteArrayKey("Collection");
    private static final ByteArrayKey ATTRIBUTE_VALUE_RDF_PARSE_TYPE_LITERAL = new ByteArrayKey("Literal");
    private static final ByteArrayKey ATTRIBUTE_VALUE_RDF_PARSE_TYPE_RESOURCE = new ByteArrayKey("Resource");
    private static final ByteArrayKey ATTRIBUTE_VALUE_RDF_PARSE_TYPE_STATEMENT = new ByteArrayKey("Statement");

    private static final ByteArrayKey ATTRIBUTE_XML_BASE = new ByteArrayKey("xml:base");
    private static final ByteArrayKey ATTRIBUTE_XML_LANG = new ByteArrayKey("xml:lang");

    private static final ByteArrayKey TAG_RDF_RDF = new ByteArrayKey("rdf:RDF");
    private static final ByteArrayKey TAG_RDF_DESCRIPTION = new ByteArrayKey("rdf:Description");
    private static final ByteArrayKey TAG_RDF_LI = new ByteArrayKey("rdf:li");

    private static final ByteArrayKey SEPARATOR_SHARP = new ByteArrayKey(SHARP);

    private static final ByteArrayKey XML_DEFAULT_NS_PREFIX = new ByteArrayKey(XMLConstants.DEFAULT_NS_PREFIX);

    private static final int MAX_LENGTH_OF_TAG_NAME = 1024; // Maximum length for tag names
    private static final int MAX_LENGTH_OF_ATTRIBUTE_NAME = 1024; // Maximum length for attribute names
    private static final int MAX_LENGTH_OF_ATTRIBUTE_VALUE = 1024; // Maximum length for attribute values
    private static final int MAX_LENGTH_OF_TEXT_CONTENT = 1024; // Maximum length for text content

    private static final Node NODE_RDF_TYPE = NodeFactory.createURI(STRING_NAMESPACE_RDF + "type");

    private final Path filePath;
    private final FileChannel fileChannel;
    private final InputStream inputStream;
    private final ByteArrayMap<SpecialByteBuffer, NamespaceIriPair> prefixToNamespace
            = new ByteArrayMap<>(8, 8);
    private final ByteArrayMap<SpecialByteBuffer, Node> tagOrAttributeNameToUriNode
            = new ByteArrayMap<>(256, 8);
    private final StreamRDF streamRDFSink;

    private final StreamBufferRoot root = new StreamBufferRoot();
    private final QNameByteBuffer currentTag = new QNameByteBuffer(root, MAX_LENGTH_OF_TAG_NAME);
    private final AttributeCollection currentAttributes = new AttributeCollection(root);
    private final DecodingTextByteBuffer currentTextContent = new DecodingTextByteBuffer(root, MAX_LENGTH_OF_TEXT_CONTENT);

    private final Deque<Element> elementStack = new ArrayDeque<>();

    //private final Map<SpecialByteBuffer, Node> iriToNode = new HashMap<>();
    private final Map<NamespaceAndQName, RDFDatatype> iriToDatatype = new HashMap<>();
    private final Map<SpecialByteBuffer, Node> blankNodeToNode = new HashMap<>();
    // A map to store langSet and avoid to copy SpecialByteBuffer objects unnecessarily
    private final Map<SpecialByteBuffer, SpecialByteBuffer> langSet = new HashMap<>();
    // A map to store baseSet and avoid to copy NamespaceFixedByteArrayBuffer objects unnecessarily
    private final Map<SpecialByteBuffer, NamespaceIriPair> baseSet = new HashMap<>();
    private final Map<NamespaceAndQName, Node> iriNodeCacheWithNamespace = new HashMap<>();
    private final Map<SpecialByteBuffer, Node> iriNodeCacheWithoutNamespace = new HashMap<>();

    private record NamespaceIriPair(SpecialByteBuffer namespace, IRIx iri) {}
    private record NamespaceAndQName(SpecialByteBuffer namespace, SpecialByteBuffer qname) {}

    private final IRIProvider iriProvider = new IRIProvider3986();

    private NamespaceIriPair baseNamespace = null;
    private NamespaceIriPair defaultNamespace = null;

    public void setBaseNamespace(String base) {
        baseNamespace = new NamespaceIriPair(
                new NamespaceFixedByteArrayBuffer(base),
                iriProvider.create(base));
    }

    private static class Element {
        public Node subject = null;
        public Node predicate = null;
        //public Node graph = null;
        public RDFDatatype datatype = null;
        public NamespaceIriPair xmlBase;
        public SpecialByteBuffer xmlLang;

        public Element(NamespaceIriPair xmlBase, SpecialByteBuffer xmlLang) {
            this.xmlBase = xmlBase;
            this.xmlLang = xmlLang;
        }
    }

    // Parser state
    private enum State {
        LOOKING_FOR_TAG,
        LOOKING_FOR_TAG_NAME,
        LOOKING_FOR_ATTRIBUTE_NAME,
        LOOKING_FOR_ATTRIBUTE_VALUE,
        AT_END_OF_OPENING_TAG,
        AT_END_OF_SELF_CLOSING_TAG,
        IN_CLOSING_TAG,
        IN_TEXT_CONTENT,
        END
    }

    private CIMParser(final Path filePath, final FileChannel fileChannel, final InputStream inputStream, final StreamRDF streamRDFSink) {
        if(filePath == null && fileChannel == null && inputStream == null) {
            throw new IllegalArgumentException("At least one input source must be provided (filePath, fileChannel, or inputStream)");
        }
        this.filePath = filePath;
        this.fileChannel = fileChannel;
        this.inputStream = inputStream;
        this.streamRDFSink = streamRDFSink;
    }

    public CIMParser(final Path filePath, final StreamRDF streamRDFSink) {
        this(filePath, null, null, streamRDFSink);
    }

    public CIMParser(final FileChannel fileChannel, final StreamRDF streamRDFSink) {
        this(null, fileChannel, null, streamRDFSink);
    }

    public CIMParser(final InputStream inputStream, final StreamRDF streamRDFSink) {
        this(null, null, inputStream, streamRDFSink);
    }

    public static class ParserException extends Exception {
        public ParserException(String message) {
            super(message);
        }
        public ParserException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public void parse() throws IOException, ParserException {
        if(inputStream != null)
            this.parse(inputStream);
        else if (fileChannel != null)
            this.parse(fileChannel);
        else if (filePath != null)
            this.parse(filePath);
        else
            throw new IllegalStateException("No input source provided for parsing");

    }

    private void parse(Path filePath) throws IOException, ParserException {
        try (var channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            parse(channel);
        }
    }

    private void parse(FileChannel channel) throws IOException, ParserException {
        final long fileSize = channel.size();
        try(final var is = new BufferedFileChannelInputStream.Builder()
                .setFileChannel(channel)
                .setOpenOptions(StandardOpenOption.READ)
                .setBufferSize((fileSize < MAX_BUFFER_SIZE) ? (int) fileSize : MAX_BUFFER_SIZE)
                .get()) {
            parse(is);
        }
    }

    private void parse(InputStream inputStream) throws IOException, ParserException {
        try (inputStream; var is = new BufferedInputStream(inputStream, MAX_LENGTH_OF_TEXT_CONTENT)) {
            root.setInputStream(is);;
            var state = State.LOOKING_FOR_TAG;
            while (state != State.END) {
                state = switch (state) {
                    case LOOKING_FOR_TAG -> handleLookingForTag();
                    case LOOKING_FOR_TAG_NAME -> handleLookingForTagName();
                    case LOOKING_FOR_ATTRIBUTE_NAME -> handleLookingForAttributeName();
                    case LOOKING_FOR_ATTRIBUTE_VALUE -> handleLookingForAttributeValue();
                    case AT_END_OF_OPENING_TAG -> handleAtEndOfOpeningTag();
                    case AT_END_OF_SELF_CLOSING_TAG -> handleSelfClosingTag();
                    case IN_CLOSING_TAG -> handleClosingTag();
                    case IN_TEXT_CONTENT -> handleTextContent();
                    case END -> State.END;
                };
            }
            if (!this.elementStack.isEmpty()) {
                throw new ParserException("Parser ended with unclosed elements in the stack: "
                        + this.elementStack.size() + " elements left.");
            }
        }
    }

    private State handleTextContent() throws IOException, ParserException {
        var parent = elementStack.peek();
        if (parent == null || parent.subject == null || parent.predicate == null) {
            throw new ParserException("Text content found without subject or predicate: "
                    + currentTextContent.decodeToString());
        }
        currentTextContent.copyRemainingBytesFromPredecessor();
        currentTextContent.setCurrentByteAsStartPositon();
        if(!currentTextContent.tryForwardAndSetEndPositionExclusive(LEFT_ANGLE_BRACKET)) {
            throw new ParserException("Unexpected end of stream while looking for opening tag after text content: "
                    + currentTextContent.decodeToString());
        }
        currentTextContent.skip(); // Skip the LEFT_ANGLE_BRACKET

        final var object = NodeFactory.createLiteral(
                currentTextContent.decodeToString(),
                parent.xmlLang != null ? parent.xmlLang.decodeToString(): null,
                parent.datatype);
        streamRDFSink.triple(Triple.create(parent.subject, parent.predicate, object));

        //refresh currentTag --> this is a shortcut to avoid LOOKING_FOR_TAG status, which would refresh currentTag
        currentTag.copyRemainingBytesFromPredecessor();
        return State.LOOKING_FOR_TAG_NAME;
    }

    private State handleClosingTag() throws IOException, ParserException {
        if (currentTag.tryForwardToByteAfter(RIGHT_ANGLE_BRACKET)) {
            this.elementStack.pop();
            return State.LOOKING_FOR_TAG;
        }
        // If we reach here, it means we didn't find the closing tag properly
        throw new ParserException("Unexpected end of stream while looking for closing tag: "
                + currentTag.decodeToString());
    }

    private State handleSelfClosingTag() throws ParserException, IOException {
        switch (handleAtEndOfOpeningTag()) {
            case LOOKING_FOR_TAG:
                // everything is fine, we are back to looking for a new tag
                break;
            case IN_TEXT_CONTENT:
                // assume empty text context, since self-closing tags do not have text content
                var element = elementStack.peek();
                if (element == null || element.subject == null || element.predicate == null) {
                    throw new ParserException("Self-closing tag without subject or predicate: "
                            + currentTag.decodeToString());
                }
                final var object = NodeFactory.createLiteral(
                        STRING_EMPTY,
                        element.xmlLang != null ? element.xmlLang.decodeToString() : null,
                        element.datatype);
                streamRDFSink.triple(Triple.create(element.subject, element.predicate, object));
                break;
            default:
                throw new ParserException("Unexpected state after self-closing tag: " + currentTag.decodeToString());
        }
        elementStack.pop(); // Remove the current element from the stack
        return State.LOOKING_FOR_TAG;
    }

    private State handleAtEndOfOpeningTag() throws ParserException, IOException {
        State state;
        if(TAG_RDF_RDF.equals(currentTag)) {
            state = handleTagRdfRdf();
        } else if(TAG_RDF_DESCRIPTION.equals(currentTag)) {
            state = handleTagRdfDescription();
        } else if (TAG_RDF_LI.equals(currentTag)) {
            state = handleRdfLi();
        } else {
            state = handleOtherTag();
        }
        if(!currentTag.tryForwardToByteAfter(RIGHT_ANGLE_BRACKET)) {
            // If we reach here, it means we didn't find the closing tag properly
            throw new ParserException("Unexpected end of stream while looking for closing angle bracket in tag: "
                    + currentTag.decodeToString());
        }
        return state;
    }

    private State handleOtherTag() throws ParserException {
        final var parent = elementStack.peek();
        final var current = initElementWithBaseAndLang(parent);

        current.subject = tryFindSubjectNodeInAttributes(current.xmlBase);

        if(current.subject != null) {
            {
                // create type triple for the current tag
                var object = getOrCreateNodeForTagOrAttributeName(currentTag);
                streamRDFSink.triple(Triple.create(current.subject, NODE_RDF_TYPE, object));
            }

            // Now process the remaining attributes, which must be literals
            for(int i = 0; i< currentAttributes.size(); i++) {
                if (currentAttributes.isConsumed(i)) {
                    continue; // skip
                }
                final var attribute = currentAttributes.get(i);
                var predicate = getOrCreateNodeForTagOrAttributeName(attribute.name);
                var object = NodeFactory.createLiteralString(attribute.value.decodeToString());
                streamRDFSink.triple(Triple.create(current.subject, predicate, object));
            }
            elementStack.push(current);
            return State.LOOKING_FOR_TAG;
        }

        if(parent == null || parent.subject == null) {
            throw new ParserException("No subject found for tag: " + currentTag.decodeToString());
        }

        current.subject = parent.subject; // Inherit subject from parent

        // process rdf:resource, rdf:datatype, and rdf:parseType attributes
        for (var i = 0; i < currentAttributes.size(); i++) {
            if (currentAttributes.isConsumed(i)) {
                continue; // Skip already consumed attributes
            }
            final var attribute = currentAttributes.get(i);
            if (ATTRIBUTE_RDF_RESOURCE.equals(attribute.name)) {
                // rdf:resource
                currentAttributes.setAsConsumed(i);
                var predicate = getOrCreateNodeForTagOrAttributeName(currentTag);
                var object = getOrCreateNodeForIri(current.xmlBase, attribute.value);
                streamRDFSink.triple(Triple.create(current.subject, predicate, object));
                elementStack.push(current);
                return State.LOOKING_FOR_TAG;
            }
            if (ATTRIBUTE_RDF_DATATYPE.equals(attribute.name)) {
                // rdf:datatype
                currentAttributes.setAsConsumed(i);
                current.predicate = getOrCreateNodeForTagOrAttributeName(currentTag);
                current.datatype = getOrCreateDatatypeForIri(current.xmlBase, attribute.value);
                elementStack.push(current);
                return State.IN_TEXT_CONTENT;
            }
            if (ATTRIBUTE_RDF_PARSE_TYPE.equals(attribute.name)) {
                // rdf:parseType
                currentAttributes.setAsConsumed(i);
                final var parseType = attribute.value;
                if (ATTRIBUTE_VALUE_RDF_PARSE_TYPE_LITERAL.equals(parseType)) {
                    throw new ParserException("rdf:parseType='Literal' is not supported in CIM/XML");
                }
                if (ATTRIBUTE_VALUE_RDF_PARSE_TYPE_RESOURCE.equals(parseType)) {
                    throw new ParserException("rdf:parseType='Resource' is not supported in CIM/XML");
                }
                if (ATTRIBUTE_VALUE_RDF_PARSE_TYPE_COLLECTION.equals(parseType)) {
                    throw new ParserException("rdf:parseType='Collection' is not supported in CIM/XML");
                }
                if (ATTRIBUTE_VALUE_RDF_PARSE_TYPE_STATEMENT.equals(parseType)) {
                    throw new ParserException("rdf:parseType='Statement' is not yet supported in this parser");
                } else {
                    throw new ParserException("Unknown rdf:parseType: " + parseType.decodeToString());
                }
            }
        }
        // process remaining attributes as literals
        for (var i = 0; i < currentAttributes.size(); i++) {
            if (currentAttributes.isConsumed(i)) {
                continue; // Skip already consumed attributes
            }
            final var attribute = currentAttributes.get(i);
            var predicate = getOrCreateNodeForTagOrAttributeName(attribute.name);
            var object = NodeFactory.createLiteralString(attribute.value.decodeToString());
            streamRDFSink.triple(Triple.create(current.subject, predicate, object));
        }
        // treat as literal if no rdf:resource or rdf:datatype found
        current.predicate = getOrCreateNodeForTagOrAttributeName(currentTag);
        elementStack.push(current);
        return State.IN_TEXT_CONTENT;
    }


    private Element initElementWithBaseAndLang(Element parent) {
        NamespaceIriPair xmlBase = null;
        SpecialByteBuffer xmlLang = null;
        // look for xml:lang and xml:base attributes
        for (var i = 0; i < currentAttributes.size(); i++) {
            final var attribute = currentAttributes.get(i);
            if (ATTRIBUTE_XML_LANG.equals(attribute.name)) {
                currentAttributes.setAsConsumed(i);
                // If the attribute is xml:lang, set the xmlLang for the element
                xmlLang = langSet.get(attribute.value);
                if (xmlLang == null) {
                    xmlLang = attribute.value.copy();
                    langSet.put(xmlLang, xmlLang); // Store the xml:lang value to avoid copying
                }
            } else if (ATTRIBUTE_XML_BASE.equals(attribute.name)) {
                currentAttributes.setAsConsumed(i);
                // If the attribute is xml:base, set the xmlBase for the element
                var namespace = attribute.value.copy();
                xmlBase = baseSet.get(namespace);
                if (xmlBase == null) {
                    var nsCopy = namespace.copy();
                    xmlBase = new NamespaceIriPair(
                            nsCopy,
                            iriProvider.create(nsCopy.decodeToString()));
                    baseSet.put(nsCopy, xmlBase); // Store the xml:base value to avoid copying
                }
            }
        }
        return new Element(
                xmlBase != null ? xmlBase : parent.xmlBase,
                xmlLang != null ? xmlLang : parent.xmlLang);
    }

    private RDFDatatype getOrCreateDatatypeForIri(final NamespaceIriPair xmlBase, final DecodingTextByteBuffer iriQName) {
        if(xmlBase == null) {
            var cacheKey = new NamespaceAndQName(null,iriQName);
            var datatype = iriToDatatype.get(cacheKey);
            if(datatype != null) {
                return datatype; // Return cached datatype if available
            }
            // Use a copy of the QName to ensure immutability in the cache key
            cacheKey = new NamespaceAndQName(null, iriQName.copy());
            final IRIx iri = iriProvider.create(iriQName.decodeToString());
            datatype = TypeMapper.getInstance().getSafeTypeByName(iri.str());
            iriToDatatype.put(cacheKey, datatype);
            return datatype;
        } else {
            var cacheKey = new NamespaceAndQName(xmlBase.namespace, iriQName);
            var datatype = iriToDatatype.get(cacheKey);
            if (datatype != null) {
                return datatype; // Return cached datatype if available
            }
            // Use a copy of the QName to ensure immutability in the cache key
            cacheKey = new NamespaceAndQName(xmlBase.namespace, iriQName.copy());
            final IRIx iri = xmlBase.iri.resolve(iriQName.decodeToString());
            datatype = TypeMapper.getInstance().getSafeTypeByName(iri.str());
            iriToDatatype.put(cacheKey, datatype);
            return datatype;
        }
    }

    private Node getOrCreateNodeForIri(final NamespaceIriPair xmlBase, final DecodingTextByteBuffer iriQName) {
        if(xmlBase == null) {
            var node = iriNodeCacheWithoutNamespace.get(iriQName);
            if(node != null) {
                return node;
            }
            // Use a copy of the QName to ensure immutability in the cache key
            var copy = iriQName.copy();
            final IRIx iri = iriProvider.create(copy.decodeToString());
            node = NodeFactory.createURI(iri.str());
            iriNodeCacheWithoutNamespace.put(copy, node);
            return node;
        } else {
            var cacheKey = new NamespaceAndQName(xmlBase.namespace, iriQName);
            var node = iriNodeCacheWithNamespace.get(cacheKey);
            if(node != null) {
                return node;
            }
            // Use a copy of the QName to ensure immutability in the cache key
            cacheKey = new NamespaceAndQName(xmlBase.namespace, iriQName.copy());
            final IRIx iri = xmlBase.iri.resolve(iriQName.decodeToString());
            node = NodeFactory.createURI(iri.str());
            iriNodeCacheWithNamespace.put(cacheKey, node);
            return node;
        }
    }

    private Node getOrCreateNodeForTagOrAttributeName(final QNameByteBuffer tagOrAttributeName) throws ParserException {
        var uriNode = tagOrAttributeNameToUriNode.get(tagOrAttributeName);
        if(uriNode == null) {
            final NamespaceIriPair namespace;
            if(tagOrAttributeName.hasPrefix()) {
                // If the name has a prefix, resolve it against the prefixToNamespace map
                namespace = prefixToNamespace.get(tagOrAttributeName.getPrefix());
                if(namespace == null) {
                    throw new IllegalArgumentException("Unknown prefix in: " + tagOrAttributeName.decodeToString());
                }
            } else {
                // If no prefix, treat it as a local part and use the default namespace
                if(defaultNamespace == null) {
                    throw new ParserException("No default namespace defined for tag or attribute: "
                            + tagOrAttributeName.decodeToString());
                }
                namespace = defaultNamespace;
            }
            uriNode = NodeFactory.createURI(namespace.namespace.join(tagOrAttributeName.getLocalPart()).decodeToString());
            tagOrAttributeNameToUriNode.put(tagOrAttributeName.copy(), uriNode);
        }
        return uriNode;
    }

    private Node getOrCreateNodeForRdfId(NamespaceIriPair xmlBase, final DecodingTextByteBuffer rdfId) throws ParserException {
        if(xmlBase == null) {
            throw new ParserException("rdf:ID attribute found without base URI");
        }
        var cacheKey = new NamespaceAndQName(xmlBase.namespace, rdfId);
        var node = iriNodeCacheWithNamespace.get(cacheKey);
        if(node != null) {
            return node;
        }
        // Use a copy of the QName to ensure immutability in the cache key
        cacheKey = new NamespaceAndQName(xmlBase.namespace, rdfId.copy());
        final IRIx iri = xmlBase.iri.resolve(CHAR_SHARP + rdfId.decodeToString()); // adding a # here
        node = NodeFactory.createURI(iri.str());
        iriNodeCacheWithNamespace.put(cacheKey, node);
        return node;
    }

    private Node getOrCreateBlankNodeWithIdentifier(final SpecialByteBuffer identifier) {
        var blankNode = blankNodeToNode.get(identifier);
        if(blankNode == null) {
            var copy = identifier.copy();
            blankNode = NodeFactory.createBlankNode(copy.decodeToString());
            blankNodeToNode.put(copy, blankNode);
        }
        return blankNode;
    }

    private Node tryFindSubjectNodeInAttributes(NamespaceIriPair xmlBase) throws ParserException {
        for (int i = 0; i < currentAttributes.size(); i++) {
            if (currentAttributes.isConsumed(i)) {
                continue; // Skip already consumed attributes
            }
            final var attribute = currentAttributes.get(i);
            if (ATTRIBUTE_RDF_ABOUT.equals(attribute.name)) {
                // rdf:about
                currentAttributes.setAsConsumed(i);
                return getOrCreateNodeForIri(xmlBase, attribute.value);
            }
            if (ATTRIBUTE_RDF_ID.equals(attribute.name)) {
                // rdf:ID here the value is relative to the xmlBase
                currentAttributes.setAsConsumed(i);
                return getOrCreateNodeForRdfId(xmlBase, attribute.value);
            }
            if (ATTRIBUTE_RDF_NODE_ID.equals(attribute.name)) {
                // rdf:nodeID
                currentAttributes.setAsConsumed(i);
                return getOrCreateBlankNodeWithIdentifier(attribute.value);
            }
        }
        return null; // No subject found in attributes
    }

    private State handleTagRdfDescription() throws ParserException {
        var current = initElementWithBaseAndLang(elementStack.peek());
        // Handle rdf:Description tag

        // determine current subject by rdf:ID, rdf:about or rdf:nodeID
        current.subject = tryFindSubjectNodeInAttributes(current.xmlBase);

        // If no subject was found, create a blank node as the subject
        if(current.subject == null) {
            current.subject = NodeFactory.createBlankNode();
        }

        // Now process the remaining attributes, which must be literals
        for(int i = 0; i< currentAttributes.size(); i++) {
            if (currentAttributes.isConsumed(i)) {
                continue; // skip
            }
            final var attribute = currentAttributes.get(i);
            var predicate = getOrCreateNodeForTagOrAttributeName(attribute.name);
            var object = NodeFactory.createLiteralString(attribute.value.decodeToString());
            streamRDFSink.triple(Triple.create(current.subject, predicate, object));
        }
        elementStack.push(current);
        return State.LOOKING_FOR_TAG;
    }

    private static SpecialByteBuffer getUriForRdfId(SpecialByteBuffer xmlBas,
                                                    SpecialByteBuffer rdfId) {
        final byte[] combinedData = new byte[xmlBas.length() + 1 + rdfId.length()];
        combinedData[xmlBas.length()] = SHARP; // Use '#' as the separator
        System.arraycopy(xmlBas.getData(), xmlBas.offset(), combinedData, 0, xmlBas.length());
        System.arraycopy(rdfId.getData(), rdfId.offset(), combinedData, xmlBas.length()+1, rdfId.length());
        return new ByteArrayKey(combinedData);
    }

    private State handleTagRdfRdf() throws ParserException {
        // if the tag is rdf:RDF, it contains only the namespaces as attributes and nothing else
        if (!currentAttributes.isEmpty()) {
            // expect all attributes to be namespace attributes
            for(int i = 0; i< currentAttributes.size(); i++) {
                final var attribute = currentAttributes.get(i);
                if(ATTRIBUTE_XML_BASE.equals(attribute.name)) {
                    final var namespace = attribute.value.copy();
                    this.baseNamespace = new NamespaceIriPair(
                            namespace,
                            iriProvider.create(namespace.decodeToString()));
                    this.streamRDFSink.base(baseNamespace.iri.str());
                    continue;
                }
                if(attribute.name.hasPrefix()) {
                    if (!ATTRIBUTE_XMLNS.equals(attribute.name.getPrefix())) {
                        throw new ParserException("Expected attribute 'xmlns:' in rdf:RDF tag but got: "
                                + attribute.name.decodeToString());
                    }
                    final var prefix = attribute.name.getLocalPart().copy();
                    final var namespace = attribute.value.copy();
                    final var iri = iriProvider.create(namespace.decodeToString());
                    // Add the namespace prefix and IRI to the prefixToNamespace map
                    prefixToNamespace.put(
                            prefix,
                            new NamespaceIriPair(namespace, iri));
                    this.streamRDFSink.prefix(
                            prefix.decodeToString(),
                            iri.str());
                } else {
                    if (!ATTRIBUTE_XMLNS.equals(attribute.name)) {
                        throw new ParserException("Expected attribute 'xmlns:' in rdf:RDF tag but got: "
                                + attribute.name.decodeToString());
                    }
                    final var namespace =  attribute.value.copy();
                    final var iri = iriProvider.create(namespace.decodeToString());
                    this.streamRDFSink.prefix(
                            XML_DEFAULT_NS_PREFIX.decodeToString(),
                            iri.str());
                    defaultNamespace = new NamespaceIriPair(namespace, iri);
                    prefixToNamespace.put(
                            XML_DEFAULT_NS_PREFIX,
                            defaultNamespace);

                }
            }
        }
        var current = new Element(this.baseNamespace, null);
        elementStack.push(current);
        return State.LOOKING_FOR_TAG;
    }

    private State handleRdfLi() throws ParserException {
        throw new ParserException("rdf:li tag is not supported yet.");
    }

    private static final byte WHITESPACE_BLOOM_FILTER = WHITESPACE_SPACE | WHITESPACE_TAB | WHITESPACE_NEWLINE | WHITESPACE_CARRIAGE_RETURN;
    private static final byte END_OF_TAG_NAME_BLOOM_FILTER = WHITESPACE_BLOOM_FILTER | RIGHT_ANGLE_BRACKET | SLASH;
    private static final byte IGNORE_TAG_IF_FIRST_CHAR_IN_BOOM_FILTER = EXCLAMATION_MARK | QUESTION_MARK;
    private static final byte ANGLE_BRACKETS_BLOOM_FILTER = LEFT_ANGLE_BRACKET | RIGHT_ANGLE_BRACKET;

    private static boolean isAngleBrackets(byte b) {
        if(b == (ANGLE_BRACKETS_BLOOM_FILTER & b)) {
            switch (b) {
                case LEFT_ANGLE_BRACKET, RIGHT_ANGLE_BRACKET -> {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isWhitespace(byte b) {
        if(b == (WHITESPACE_BLOOM_FILTER & b)) {
            switch (b) {
                case WHITESPACE_SPACE, WHITESPACE_TAB, WHITESPACE_NEWLINE, WHITESPACE_CARRIAGE_RETURN -> {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isEndOfTagName(byte b) {
        if(b == (END_OF_TAG_NAME_BLOOM_FILTER & b)) {
            switch (b) {
                case WHITESPACE_SPACE, RIGHT_ANGLE_BRACKET, SLASH, WHITESPACE_TAB, WHITESPACE_NEWLINE, WHITESPACE_CARRIAGE_RETURN -> {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isTagToBeIgnoredDueToFirstChar(byte b) {
        if(b == (IGNORE_TAG_IF_FIRST_CHAR_IN_BOOM_FILTER & b)) {
            switch (b) {
                case EXCLAMATION_MARK, QUESTION_MARK-> {
                    return true;
                }
            }
        }
        return false;
    }

    public interface SpecialByteBuffer {
        // offset in the buffer where the data starts
        int offset();
        // Number of bytes in the buffer
        int length();
        // Returns the byte array containing the data
        byte[] getData();
        /**
         * Returns a hash code based on the first and last byte of the array.
         */
        default int defaultHashCode() {
            return Hashing.murmur3_32().hashBytes(getData(), offset(), length()).asInt();
        }
        default boolean equals(SpecialByteBuffer other) {
            if (this == other) return true;
            if (other == null) return false;

            if (this.length() != other.length()) return false;

            byte[] thisData = this.getData();
            byte[] otherData = other.getData();
            // Compare in reverse order, since in CIM XML, the last characters are more significant
            for (int i = this.length() - 1; i > -1; i--) {
                if (thisData[offset()+i] != otherData[other.offset()+i]) {
                    return false; // Different content
                }
            }
            return true; // Same content
        }

        default java.nio.ByteBuffer wrapAsByteBuffer() {
            return java.nio.ByteBuffer.wrap(this.getData(), this.offset(), this.length());
        }

        default SpecialByteBuffer copy() {
            return new ByteArrayKey(this.copyToByteArray());
        }

        default byte [] joinedData(SpecialByteBuffer other) {
            if (other == null || other.length() == 0) {
                return this.copyToByteArray();
            }
            final byte[] combinedData = new byte[this.length() + other.length()];
            System.arraycopy(this.getData(), this.offset(), combinedData, 0, this.length());
            System.arraycopy(other.getData(), other.offset(), combinedData, this.length(), other.length());
            return combinedData;
        }

        default SpecialByteBuffer join(SpecialByteBuffer other) {
            return new ByteArrayKey(joinedData(other));
        }

        default SpecialByteBuffer join(SpecialByteBuffer... other) {
            if (other == null || other.length == 0) {
                return this.copy();
            }
            int totalLength = this.length();
            for (SpecialByteBuffer buf : other) {
                if (buf != null) {
                    totalLength += buf.length();
                }
            }
            final byte[] combinedData = new byte[totalLength];
            System.arraycopy(this.getData(), this.offset(), combinedData, 0, this.length());
            int offset = this.length();
            for (SpecialByteBuffer buf : other) {
                if (buf != null && buf.length() > 0) {
                    System.arraycopy(buf.getData(), buf.offset(), combinedData, offset, buf.length());
                    offset += buf.length();
                }
            }
            return new ByteArrayKey(combinedData);
        }

        default byte[] copyToByteArray() {
            if (this.length() == 0) {
                return new byte[0];
            }
            final byte[] dataCopy = new byte[this.length()];
            System.arraycopy(this.getData(), this.offset(), dataCopy, 0, this.length());
            return dataCopy;
        }

        default String decodeToString() {
            return UTF_8.decode(this.wrapAsByteBuffer()).toString();
        }

        /// A heuristic to check if the content is probably a CIM uuid.
        /// These UUIDs start with an underscore or sharp and underscore
        ///  and are 37-38 characters long, with dashes at specific positions.
        /// Format: [#]_8-4-4-4-12 -> [#]_XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
        default boolean isProbablyCimUuid() {
            // A very simple heuristic to check if the content is probably a UUID
            // This is not a strict check, just a quick way to filter out non-UUIDs
            if (this.length() == 38
                && this.getData()[this.offset()] == SHARP
                && this.getData()[this.offset() + 1] == UNDERSCORE
                && this.getData()[this.offset() + 10] == '-'
                && this.getData()[this.offset() + 15] == '-'
                && this.getData()[this.offset() + 20] == '-'
                && this.getData()[this.offset() + 25] == '-') {
                return true;
            }
            return this.length() == 37
                    && this.getData()[this.offset()] == UNDERSCORE
                    && this.getData()[this.offset() + 9] == '-'
                    && this.getData()[this.offset() + 14] == '-'
                    && this.getData()[this.offset() + 19] == '-'
                    && this.getData()[this.offset() + 24] == '-';
        }
    }

    /**
     * A simple key class for byte arrays, using the first and last byte for hash code calculation.
     * This is a simplified version for demonstration purposes.
     */
    public static class ByteArrayKey implements SpecialByteBuffer {
        private final byte[] data;
        private final int length;
        private final int hashCode;
        private String decodedString = null;


        public ByteArrayKey(String string) {
            var buffer = UTF_8.encode(string);
            this.data = buffer.array();
            this.length = buffer.limit();
            this.decodedString = string;
            this.hashCode = this.defaultHashCode();
        }

        public ByteArrayKey(final byte[] data) {
            this.data = data;
            this.length = data.length;
            this.hashCode = this.defaultHashCode();
        }

        public ByteArrayKey(final byte b) {
            this.data = new byte[] { b };
            this.length = 1;
            this.hashCode = b;
        }

        public byte[] getData() {
            return this.data;
        }

        @Override
        public int offset() {
            return 0;
        }

        public int length() {
            return this.length;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof SpecialByteBuffer otherBuffer) {
                return this.equals(otherBuffer);
            }
            return false;
        }

        @Override
        public String decodeToString() {
            if (this.decodedString == null) {
                this.decodedString = UTF_8.decode(this.wrapAsByteBuffer()).toString();
            }
            return this.decodedString;
        }

        @Override
        public String toString() {
            return "ByteArrayKey [" + this.decodeToString()  + "]";
        }
    }

    public static class JenaHashSet<E> extends FastHashSet<E> {

        public JenaHashSet(int initialSize) {
            super(initialSize);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected E[] newKeysArray(int size) {
            return (E[]) new Object[size];
        }

    }

    public static class JenaHashMap<K, V> extends FastHashMap<K, V> {

        public JenaHashMap(int initialSize) {
            super(initialSize);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected K[] newKeysArray(int size) {
            return (K[]) new Object[size];
        }

        @Override
        @SuppressWarnings("unchecked")
        protected V[] newValuesArray(int size) {
            return (V[]) new Object[size];
        }
    }

    public static class ByteArrayMap<K extends SpecialByteBuffer, V> {
        private final int expectedMaxEntriesWithSameLength;
        private JenaHashMap<K, V>[] entriesWithSameLength;

        public ByteArrayMap(int expectedMaxByteLength, int expectedEntriesWithSameLength) {
            var positionsSize = Integer.highestOneBit(expectedMaxByteLength << 1);
            if (positionsSize < expectedMaxByteLength << 1) {
                positionsSize <<= 1;
            }
            this.entriesWithSameLength = new JenaHashMap[positionsSize];
            this.expectedMaxEntriesWithSameLength = expectedEntriesWithSameLength;
        }

        private void grow(final int minimumLength) {
            var newLength = entriesWithSameLength.length << 1;
            while (newLength < minimumLength) {
                newLength = minimumLength << 1;
            }
            final var oldValues = entriesWithSameLength;
            entriesWithSameLength = new JenaHashMap[newLength];
            System.arraycopy(oldValues, 0, entriesWithSameLength, 0, oldValues.length);
        }

        public void put(K key, V value) {
            final JenaHashMap<K, V> map;
            // Ensure the array is large enough
            if (entriesWithSameLength.length < key.length()) {
                grow(key.length());
                map = new JenaHashMap<>(expectedMaxEntriesWithSameLength);
                entriesWithSameLength[key.length()] = map;
                map.put(key, value);
                return;
            }
            if (entriesWithSameLength[key.length()] == null) {
                map = new JenaHashMap<>(expectedMaxEntriesWithSameLength);
                entriesWithSameLength[key.length()] = map;
            } else {
                map = entriesWithSameLength[key.length()];
            }
            map.put(key, value);
        }

        public boolean tryPut(K key, V value) {
            final JenaHashMap<K, V> map;
            // Ensure the array is large enough
            if (entriesWithSameLength.length < key.length()) {
                grow(key.length());
                map = new JenaHashMap<>(expectedMaxEntriesWithSameLength);
                entriesWithSameLength[key.length()] = map;
                map.put(key, value);
                return true;
            }
            if (entriesWithSameLength[key.length()] == null) {
                map = new JenaHashMap<>(expectedMaxEntriesWithSameLength);
                entriesWithSameLength[key.length()] = map;
            } else {
                map = entriesWithSameLength[key.length()];
            }
            return map.tryPut(key, value);
        }

        public V computeIfAbsent(K key, Supplier<V> mappingFunction) {
            final JenaHashMap<K, V> map;
            // Ensure the array is large enough
            if (entriesWithSameLength.length < key.length()) {
                grow(key.length());
                map = new JenaHashMap<>(expectedMaxEntriesWithSameLength);
                entriesWithSameLength[key.length()] = map;
                final var value = mappingFunction.get();
                map.put(key, value);
                return value;
            }
            if (entriesWithSameLength[key.length()] == null) {
                map = new JenaHashMap<>(expectedMaxEntriesWithSameLength);
                entriesWithSameLength[key.length()] = map;
            } else {
                map = entriesWithSameLength[key.length()];
            }
            return map.computeIfAbsent(key, mappingFunction);
        }

        public V get(K key) {
            if (entriesWithSameLength.length < key.length() || entriesWithSameLength[key.length()] == null) {
                return null;
            }
            return entriesWithSameLength[key.length()].get(key);
        }

        public boolean containsKey(K key) {
            if (entriesWithSameLength.length < key.length() || entriesWithSameLength[key.length()] == null) {
                return false;
            }
            return entriesWithSameLength[key.length()].containsKey(key);
        }

    }

    public static class StreamBufferRoot {

        /**
         * Input stream from which the data is read.
         * This stream is expected to be used by the child buffers to read data.
         */
        InputStream inputStream;
        /**
         * The current buffer that is being filled with data.
         * This is used to handover remaining bytes from one child to the next.
         */
        StreamBufferChild lastUsedChildBuffer;

        public StreamBufferRoot() {

        }

        public InputStream getInputStream() {
            return inputStream;
        }

        public void setInputStream(InputStream inputStream) {
            this.inputStream = inputStream;
        }

        public StreamBufferChild getLastUsedChildBuffer() {
            return lastUsedChildBuffer;
        }

        public void setLastUsedChildBuffer(StreamBufferChild lastUsedChildBuffer) {
            this.lastUsedChildBuffer = lastUsedChildBuffer;
        }
    }

    public static class StreamBufferChild implements SpecialByteBuffer {
        /**
         * The root buffer that this child belongs to.
         * This is used to access the input stream for reading data.
         * It also holds the last used buffer, which is used to handover remaining bytes.
         */
        protected final StreamBufferRoot root;
        /**
         * The byte array buffer that holds the data read from the input stream.
         */
        protected final byte[] buffer;
        /**
         * The offset in the buffer where the data starts.
         */
        protected int start = 0;
        /**
         * Marks the end of relevant data in the buffer.
         */
        protected int endExclusive = 0;
        /**
         * This marks the position to which the buffer is filled.
         */
        protected int filledToExclusive = 0;

        /**
         * The position in the buffer where the next byte will be read.
         */
        protected int position = 0;

        protected boolean abort = false;

        public StreamBufferChild(StreamBufferRoot parent, int size) {
            if (parent == null) {
                throw new IllegalArgumentException("Parent buffer cannot be null");
            }
            this.root = parent;
            this.buffer = new byte[size];
        }

        public void reset() {
            this.start = 0;
            this.endExclusive = 0;
            this.filledToExclusive = 0;
            this.position = 0;
        }

        public void abort() {
            this.abort = true;
        }

        public void setCurrentByteAsStartPositon() {
            this.start = this.position;
        }

        public void setNextByteAsStartPositon() {
            this.start = this.position+1;
        }

        public void setEndPositionExclusive() {
            this.endExclusive = this.position;
        }

        public boolean hasRemainingCapacity() {
            return filledToExclusive < buffer.length;
        }

        public boolean tryForwardAndSetStartPositionAfter(byte byteToSeek) throws IOException {
            boolean[] found = {false};
            this.consumeBytes(b -> {
                if (b == byteToSeek) {
                    setNextByteAsStartPositon();
                    abort();
                    found[0] = true;
                }
            });
            return found[0];
        }

        public boolean tryForwardAndSetEndPositionExclusive(byte byteToSeek) throws IOException {
            var abortBefore = this.abort; // memoize the current abort state to avoid side effects
            boolean[] found = {false};
            this.consumeBytes(b -> {
                if (b == byteToSeek) {
                    setEndPositionExclusive();
                    abort();
                    found[0] = true;
                }
            });
            if(abortBefore) {
                this.abort = true; // Restore the abort state if it was set before
            }
            return found[0];
        }

        public boolean tryForwardToByte(byte byteToSeek) throws IOException {
            var abortBefore = this.abort; // memoize the current abort state to avoid side effects
            boolean[] found = {false};
            this.consumeBytes(b -> {
                if (b == byteToSeek) {
                    abort();
                    found[0] = true;
                }
            });
            if(abortBefore) {
                this.abort = true; // Restore the abort state if it was set before
            }
            return found[0];
        }

        public boolean tryForwardToByteAfter(byte byteToSeek) throws IOException {
            boolean found = tryForwardToByte(byteToSeek);
            position++;
            return found;
        }

        /**
         * Copies remaining bytes from the last used child buffer of the parent.
         * This is used to handover remaining bytes from one child buffer to the next.
         * Attention: The predecessor may be identical to this child buffer.
         * In that case, the remaining bytes are copied to the beginning of this buffer
         */
        public void copyRemainingBytesFromPredecessor() {
            if (root.lastUsedChildBuffer == null) {
                root.lastUsedChildBuffer = this;
                reset();
                return; // Nothing to copy
            }
            var predecessor = root.lastUsedChildBuffer;
            var remainingBytes = predecessor.filledToExclusive - predecessor.position;
            if(remainingBytes == 0) {
                root.lastUsedChildBuffer = this;
                reset();
                return; // No remaining bytes to copy
            }
            System.arraycopy(predecessor.buffer, predecessor.position,
                    this.buffer, 0, remainingBytes);
            reset();
            this.filledToExclusive = remainingBytes;
            root.lastUsedChildBuffer = this;
        }

        public byte peek() throws IOException {
            if(position >= filledToExclusive) {
                if (!tryFillFromInputStream()) {
                    return END_OF_STREAM;
                }
            }
            return buffer[position];
        }

        /**
         * Reads the next byte from the buffer and advances the position.
         * @return the next byte in the buffer
         * @throws IOException if an I/O error occurs while reading from the input stream
         */
        public byte next() throws IOException {
            position++;
            return peek();
        }

        /**
         * Skips the current byte and moves to the next one.
         * This does not change the start or end positions.
         * @throws IOException if an I/O error occurs while reading from the input stream
         */
        public void skip() throws IOException {
            position++;
            peek();
        }

        public void consumeBytes(Consumer<Byte> byteConsumer) throws IOException {
            var abortBefore = this.abort; // memoize the current abort state to avoid side effects
            abort = false;
            if(position >= filledToExclusive) {
                if (!tryFillFromInputStream()) {
                    byteConsumer.accept(END_OF_STREAM);
                    return; // No more data to read
                }
            }
            while(position < filledToExclusive) {
                byteConsumer.accept(buffer[position]);
                if(abort) {
                    return;
                }
                if(++position >= filledToExclusive) {
                    if (!tryFillFromInputStream()) {
                        byteConsumer.accept(END_OF_STREAM);
                        return; // No more data to read
                    }
                }
            }
            byteConsumer.accept(END_OF_STREAM);
            if(abortBefore) {
                this.abort = true; // Restore the abort state if it was set before
            }
        }

        private boolean tryFillFromInputStream() throws IOException {
            if (hasRemainingCapacity()) {
                var bytesRead = root.inputStream.read(this.buffer, filledToExclusive,
                        buffer.length - filledToExclusive);
                if (bytesRead == -1) {
                    return false;
                }
                filledToExclusive += bytesRead;
                return true;
            }
            return false;
        }

        @Override
        public int offset() {
            return start;
        }

        @Override
        public int length() {
            return endExclusive-start;
        }

        @Override
        public byte[] getData() {
            return this.buffer;
        }

        @Override
        public String toString() {
            String text;
            if (start == 0 && endExclusive == 0) {
                text = "Start at 0:[" +
                        UTF_8.decode(java.nio.ByteBuffer.wrap(this.buffer, start,
                        position-start+1))
                        + "]--> end not defined yet";
            } else if (start > endExclusive) {
                if(start < position) {
                    text = UTF_8.decode(java.nio.ByteBuffer.wrap(this.buffer, start,
                            position-start+1)) + "][--> end not defined yet";
                } else {
                    text = UTF_8.decode(java.nio.ByteBuffer.wrap(this.buffer, start,
                            1)) + "][--> end not defined yet";
                }
            } else {
                text = this.decodeToString();
            }
            return "StreamBufferChild [" + text + "]";
        }

        public String wholeBufferToString() {
            return UTF_8.decode(java.nio.ByteBuffer.wrap(this.buffer, 0, this.filledToExclusive)).toString();
        }

        public String remainingBufferToString() {
            return UTF_8.decode(java.nio.ByteBuffer.wrap(this.buffer, position, filledToExclusive - position)).toString();
        }
    }

    public static class QNameByteBuffer extends StreamBufferChild {
        private int startOfLocalPart = 0; // Index where the local part starts

        public QNameByteBuffer(StreamBufferRoot parent, int size) {
            super(parent, size);
        }

        @Override
        public void reset() {
            super.reset();
            this.startOfLocalPart = 0;
        }

        public boolean hasPrefix() {
            return startOfLocalPart != 0; // If local part starts after the first byte, it has a prefix
        }

        public ReadonlyByteArrayBuffer getPrefix() {
            return new ReadonlyByteArrayBuffer(buffer, start, startOfLocalPart-start-1); // Exclude the colon
        }

        public ReadonlyByteArrayBuffer getLocalPart() {
            return new ReadonlyByteArrayBuffer(buffer, startOfLocalPart, endExclusive - startOfLocalPart);
        }

        @Override
        public void consumeBytes(Consumer<Byte> byteConsumer) throws IOException {
            var c = byteConsumer.andThen((b) -> {
                if (b == DOUBLE_COLON) {
                    startOfLocalPart = position + 1; // Set the start of local part after the colon
                }
            });
            super.consumeBytes(c);
        }
    }

    public static class DecodingTextByteBuffer extends StreamBufferChild {
        protected int lastAmpersandPosition = -1; // Position of the last '&' character, used for decoding

        public DecodingTextByteBuffer(StreamBufferRoot parent, int size) {
            super(parent, size);
        }

        @Override
        public void reset() {
            super.reset();
            this.lastAmpersandPosition = -1;
        }


        @Override
        public void consumeBytes(Consumer<Byte> byteConsumer) throws IOException {
            var c = byteConsumer.andThen((b) -> {
                switch (b) {
                    case AMPERSAND -> lastAmpersandPosition = position; // Store the position of the last '&'
                    case SEMICOLON -> {
                        var charsBetweenAmpersandAndSemicolon = position - lastAmpersandPosition - 1;
                        switch (charsBetweenAmpersandAndSemicolon) {
                            case 2: {
                                if (buffer[lastAmpersandPosition+2] == 't') {
                                    if (buffer[lastAmpersandPosition+1] == 'l') {
                                        buffer[lastAmpersandPosition] = LEFT_ANGLE_BRACKET; // &lt;

                                        // move remaining data to the left
                                        System.arraycopy(buffer, position+1,
                                                buffer, lastAmpersandPosition + 1,
                                                filledToExclusive-position);

                                        filledToExclusive -= 3; // Reduce filledToExclusive by 3 for &lt;
                                        position = lastAmpersandPosition;
                                        lastAmpersandPosition = -1; // Reset last ampersand position
                                        return;
                                    } else if (buffer[lastAmpersandPosition+1] == 'g') {
                                        buffer[lastAmpersandPosition] = RIGHT_ANGLE_BRACKET; // &gt;

                                        // move remaining data to the left
                                        System.arraycopy(buffer, position+1,
                                                buffer, lastAmpersandPosition + 1,
                                                filledToExclusive-position);

                                        filledToExclusive -= 3; // Reduce filledToExclusive by 3 for &gt;
                                        position = lastAmpersandPosition;
                                        lastAmpersandPosition = -1; // Reset last ampersand position
                                        return;
                                    }
                                }
                                break;
                            }
                            case 3: {
                                if  (buffer[lastAmpersandPosition+3] == 'p'
                                        && buffer[lastAmpersandPosition+2] == 'm'
                                        && buffer[lastAmpersandPosition+1] == 'a') {
                                    buffer[lastAmpersandPosition] = AMPERSAND; // &amp;

                                    // move remaining data to the left
                                    System.arraycopy(buffer, position+1,
                                            buffer, lastAmpersandPosition + 1,
                                            filledToExclusive-position);

                                    filledToExclusive -= 4; // Reduce filledToExclusive by 4 for &amp;
                                    position = lastAmpersandPosition;
                                    lastAmpersandPosition = -1; // Reset last ampersand position
                                    return;
                                }
                                break;
                            }
                            case 4: {
                                if (buffer[lastAmpersandPosition+3] == 'o') {
                                    if(buffer[lastAmpersandPosition+1] == 'q'
                                            && buffer[lastAmpersandPosition+2] == 'u'
                                            && buffer[lastAmpersandPosition+4] == 't') {
                                        buffer[lastAmpersandPosition] = DOUBLE_QUOTE; // &quot;

                                        // move remaining data to the left
                                        System.arraycopy(buffer, position+1,
                                                buffer, lastAmpersandPosition + 1,
                                                filledToExclusive-position);

                                        filledToExclusive -= 5; // Reduce filledToExclusive by 5 for &quot;
                                        position = lastAmpersandPosition;
                                        lastAmpersandPosition = -1; // Reset last ampersand position
                                        return;
                                    } else if (buffer[lastAmpersandPosition+2] == 'p'
                                            && buffer[lastAmpersandPosition+4] == 's'
                                            && buffer[lastAmpersandPosition+1] == 'a') {
                                        buffer[lastAmpersandPosition] = SINGLE_QUOTE; // &apos;

                                        // move remaining data to the left
                                        System.arraycopy(buffer, position+1,
                                                buffer, lastAmpersandPosition + 1,
                                                filledToExclusive-position);

                                        filledToExclusive -= 5; // Reduce filledToExclusive by 5 for &apos;
                                        position = lastAmpersandPosition;
                                        lastAmpersandPosition = -1; // Reset last ampersand position
                                        return;
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
            });
            super.consumeBytes(c);
        }
    }



    private static class FixedByteArrayBuffer implements SpecialByteBuffer {
        protected final byte[] data;
        protected int position;

        public FixedByteArrayBuffer(String text) {
            var buffer = UTF_8.encode(text);
            this.data = buffer.array();
            this.position = buffer.limit();
        }

        public FixedByteArrayBuffer(int size) {
            this.data = new byte[size];
            this.position = 0;
        }

        private FixedByteArrayBuffer(byte[] data, int position) {
            this.data = data;
            this.position = position;
        }

        private FixedByteArrayBuffer(byte[] data) {
            this.data = data;
            this.position = data.length;
        }

        public void reset() {
            position = 0;
        }

        public void append(byte b) {
            if (position < data.length) {
                data[position++] = b;
            } else {
                throw new IllegalStateException("Buffer overflow");
            }
        }

        public NamespaceFixedByteArrayBuffer asNamespace() {
            return new NamespaceFixedByteArrayBuffer(this.data, this.position);
        }

        public NamespaceFixedByteArrayBuffer asNamespaceCopy() {
            return new NamespaceFixedByteArrayBuffer(this.copyToByteArray());
        }

        @Override
        public int hashCode() {
            return this.defaultHashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof SpecialByteBuffer otherBuffer) {
                return this.equals(otherBuffer);
            }
            return false;
        }

        @Override
        public int offset() {
            return 0;
        }

        @Override
        public int length() {
            return position;
        }

        @Override
        public byte[] getData() {
            return this.data;
        }

        @Override
        public String toString() {
            return "FixedByteArrayBuffer [" + this.decodeToString()  + "]";
        }

        public boolean isFull() {
            return position == data.length;
        }
    }

    private static class DecodingTextArrayByteBuffer extends FixedByteArrayBuffer {
        protected int lastAmpersandPosition = -1; // Position of the last '&' character, used for decoding

        public DecodingTextArrayByteBuffer(int size) {
            super(size);
        }

        private DecodingTextArrayByteBuffer(byte[] data, int position) {
            super(data, position);
        }

        private DecodingTextArrayByteBuffer(byte[] data) {
            super(data);
        }

        @Override
        public void reset() {
            super.reset();
            lastAmpersandPosition = -1;
        }

        public int consumeToLeftAngleBracket(int bytesRead) {
            position = 0; // Reset position to start
            if (bytesRead == 0) {
                return 0;
            }
            var bytesConsumed = 0;
            for ( ; position < bytesRead; position++) {
                bytesConsumed++;
                switch (data[position]) {
                    case LEFT_ANGLE_BRACKET -> {
                        return bytesConsumed; // Stop at the first '<'
                    }
                    case AMPERSAND ->
                        lastAmpersandPosition = position; // Store the position of the last '&'

                    case  SEMICOLON -> {
                        var charsBetweenAmpersandAndSemicolon = position - lastAmpersandPosition - 1;
                        switch (charsBetweenAmpersandAndSemicolon) {
                            case 2 -> {
                                if (data[lastAmpersandPosition+2] == 't') {
                                    if (data[lastAmpersandPosition+1] == 'l') {
                                        data[lastAmpersandPosition] = LEFT_ANGLE_BRACKET; // &lt;

                                        bytesRead -= 3; // Reduce bytes read by 3 for &lt;
                                        bytesConsumed++;
                                        // move remaining data to the left
                                        System.arraycopy(data, position+1,
                                                data, lastAmpersandPosition + 1,
                                                bytesRead-position);

                                        position = lastAmpersandPosition + 1;
                                        lastAmpersandPosition = -1; // Reset last ampersand position
                                    } else if (data[lastAmpersandPosition+1] == 'g') {
                                        data[lastAmpersandPosition] = RIGHT_ANGLE_BRACKET; // &gt;

                                        bytesRead -= 3; // Reduce bytes read by 3 for &gt;
                                        bytesConsumed++;
                                        // move remaining data to the left
                                        System.arraycopy(data, position+1,
                                                data, lastAmpersandPosition + 1,
                                                bytesRead-position);

                                        position = lastAmpersandPosition + 1;
                                        lastAmpersandPosition = -1; // Reset last ampersand position
                                    }
                                }
                            }
                            case 3 -> {
                                if  (data[lastAmpersandPosition+3] == 'p'
                                        && data[lastAmpersandPosition+2] == 'm'
                                        && data[lastAmpersandPosition+1] == 'a') {
                                    data[lastAmpersandPosition] = AMPERSAND; // &amp;

                                    bytesRead -= 4; // Reduce bytes read by 4 for &amp;
                                    bytesConsumed++;
                                    // move remaining data to the left
                                    System.arraycopy(data, position+1,
                                            data, lastAmpersandPosition + 1,
                                            bytesRead-position);

                                    position = lastAmpersandPosition + 1;
                                    lastAmpersandPosition = -1; // Reset last ampersand position
                                }
                            }
                            case 4 -> {
                                if (data[lastAmpersandPosition+3] == 'o') {
                                    if(data[lastAmpersandPosition+1] == 'q'
                                            && data[lastAmpersandPosition+2] == 'u'
                                            && data[lastAmpersandPosition+4] == 't') {
                                        data[lastAmpersandPosition] = DOUBLE_QUOTE; // &quot;

                                        bytesRead -= 5; // Reduce bytes read by 5 for &quot;
                                        bytesConsumed++;
                                        // move remaining data to the left
                                        System.arraycopy(data, position+1,
                                                data, lastAmpersandPosition + 1,
                                                bytesRead-position);

                                        position = lastAmpersandPosition + 1;
                                        lastAmpersandPosition = -1; // Reset last ampersand position
                                    } else if (data[lastAmpersandPosition+2] == 'p'
                                            && data[lastAmpersandPosition+4] == 's'
                                            && data[lastAmpersandPosition+1] == 'a') {
                                        data[lastAmpersandPosition] = SINGLE_QUOTE; // &apos;

                                        bytesRead -= 5; // Reduce bytes read by 5 for &apos;
                                        bytesConsumed++;
                                        // move remaining data to the left
                                        System.arraycopy(data, position+1,
                                                data, lastAmpersandPosition + 1,
                                                bytesRead-position);

                                        position = lastAmpersandPosition + 1;
                                        lastAmpersandPosition = -1; // Reset last ampersand position
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return bytesConsumed;
        }

        @Override
        public void append(byte b) {
            if (position < data.length) {
                if (b == AMPERSAND) {
                    lastAmpersandPosition = position; // Store the position of the last '&'
                } else if (b == SEMICOLON) {
                    var charsBetweenAmpersandAndSemicolon = position - lastAmpersandPosition - 1;
                    switch (charsBetweenAmpersandAndSemicolon) {
                        case 2: {
                            if (data[lastAmpersandPosition+2] == 't') {
                                if (data[lastAmpersandPosition+1] == 'l') {
                                    data[lastAmpersandPosition] = LEFT_ANGLE_BRACKET; // &lt;
                                    position = lastAmpersandPosition + 1;
                                    lastAmpersandPosition = -1; // Reset last ampersand position
                                    return;
                                } else if (data[lastAmpersandPosition+1] == 'g') {
                                    data[lastAmpersandPosition] = RIGHT_ANGLE_BRACKET; // &gt;
                                    position = lastAmpersandPosition + 1;
                                    lastAmpersandPosition = -1; // Reset last ampersand position
                                    return;
                                }
                            }
                            break;
                        }
                        case 3: {
                            if  (data[lastAmpersandPosition+3] == 'p'
                                    && data[lastAmpersandPosition+2] == 'm'
                                    && data[lastAmpersandPosition+1] == 'a') {
                                data[lastAmpersandPosition] = AMPERSAND; // &amp;
                                position = lastAmpersandPosition + 1;
                                lastAmpersandPosition = -1; // Reset last ampersand position
                                return;
                            }
                            break;
                        }
                        case 4: {
                            if (data[lastAmpersandPosition+3] == 'o') {
                                if(data[lastAmpersandPosition+1] == 'q'
                                        && data[lastAmpersandPosition+2] == 'u'
                                        && data[lastAmpersandPosition+4] == 't') {
                                    data[lastAmpersandPosition] = DOUBLE_QUOTE; // &quot;
                                    position = lastAmpersandPosition + 1;
                                    lastAmpersandPosition = -1; // Reset last ampersand position
                                    return;
                                } else if (data[lastAmpersandPosition+2] == 'p'
                                        && data[lastAmpersandPosition+4] == 's'
                                        && data[lastAmpersandPosition+1] == 'a') {
                                    data[lastAmpersandPosition] = SINGLE_QUOTE; // &apos;
                                    position = lastAmpersandPosition + 1;
                                    lastAmpersandPosition = -1; // Reset last ampersand position
                                    return;
                                }
                            }
                            break;
                        }
                    }
                }
                data[position++] = b;
            } else {
                throw new IllegalStateException("Buffer overflow");
            }
        }

        @Override
        public String toString() {
            return "DecodingTextArrayByteBuffer [" + this.decodeToString()  + "]";
        }

        @Override
        public DecodingTextArrayByteBuffer copy() {
            return new DecodingTextArrayByteBuffer(this.copyToByteArray());
        }
    }

    public static class ReadonlyByteArrayBuffer implements SpecialByteBuffer {

        private final byte[] data;
        private final int offset;
        private final int length;
        private final int hashCode;

        public ReadonlyByteArrayBuffer(byte[] data, int offset, int length) {
            this.data = data;
            this.offset = offset;
            this.length = length;
            this.hashCode = this.defaultHashCode();
        }

        @Override
        public int offset() {
            return offset;
        }

        @Override
        public int length() {
            return length;
        }

        @Override
        public byte[] getData() {
            return data;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof SpecialByteBuffer otherBuffer) {
                return this.equals(otherBuffer);
            }
            return false;
        }

        @Override
        public String toString() {
            return "ReadonlyByteArrayBuffer [" + this.decodeToString()  + "]";
        }
    }

    private static class NamespaceFixedByteArrayBuffer extends FixedByteArrayBuffer {

        public NamespaceFixedByteArrayBuffer(String namespace) {
           super(namespace);
        }

        private NamespaceFixedByteArrayBuffer(byte[] data, int position) {
            super(data, position);
        }

        private NamespaceFixedByteArrayBuffer(byte[] data) {
            super(data);
        }

        @Override
        public NamespaceFixedByteArrayBuffer copy() {
            return new NamespaceFixedByteArrayBuffer(copyToByteArray());
        }

        @Override
        public String toString() {
            return "NamespaceFixedByteArrayBuffer [" +
                    this.decodeToString() +
                    "]";
        }
    }

    private static class QNameFixedByteArrayBuffer extends FixedByteArrayBuffer {
        private int startOfLocalPart = 0; // Index where the local part starts

        public QNameFixedByteArrayBuffer(int bufferSize) {
            super(bufferSize);
        }

        private QNameFixedByteArrayBuffer(byte[] data) {
            super(data);
        }

        @Override
        public QNameFixedByteArrayBuffer copy() {
            var copy = new QNameFixedByteArrayBuffer(this.copyToByteArray());
            copy.startOfLocalPart = this.startOfLocalPart;
            return copy;
        }

        public boolean hasPrefix() {
            return startOfLocalPart != 0; // If local part starts after the first byte, it has a prefix
        }

        public ReadonlyByteArrayBuffer getPrefix() {
            return new ReadonlyByteArrayBuffer(data, 0, startOfLocalPart-1); // Exclude the colon
        }

        public ReadonlyByteArrayBuffer getLocalPart() {
            return new ReadonlyByteArrayBuffer(data, startOfLocalPart, position - startOfLocalPart);
        }

        public void reset() {
            super.reset();
            startOfLocalPart = 0;
        }

        public void append(byte b) {
            if(DOUBLE_COLON == b) {
                startOfLocalPart = position+1; // Start of local part
            }
            super.append(b);
        }

        @Override
        public String toString() {
            return "QNameFixedByteArrayBuffer [" +
                    this.decodeToString() +
                    "]";
        }
    }

    private record AttributeFixedBuffer(QNameByteBuffer name, DecodingTextByteBuffer value) {}

    private static class AttributeCollection {
        private final StreamBufferRoot streamingBufferRoot;
        private final List<AttributeFixedBuffer> attributeFixedBuffers = new ArrayList<>();
        private final JenaHashSet<Integer> alreadyConsumed = new JenaHashSet<>(16);
        private int currentAttributeIndex = -1;

        private AttributeCollection(StreamBufferRoot streamingBufferRoot) {
            this.streamingBufferRoot = streamingBufferRoot;
        }

        private void newTag() {
            currentAttributeIndex = -1;
            alreadyConsumed.clear();
        }

        private QNameByteBuffer newAttribute() {
            final AttributeFixedBuffer buffer;
            currentAttributeIndex++;
            if (currentAttributeIndex == attributeFixedBuffers.size()) {
                buffer = new AttributeFixedBuffer(
                        new QNameByteBuffer(streamingBufferRoot, MAX_LENGTH_OF_ATTRIBUTE_NAME),
                        new DecodingTextByteBuffer(streamingBufferRoot, MAX_LENGTH_OF_ATTRIBUTE_VALUE));
                attributeFixedBuffers.add(buffer);
            } else {
                buffer = attributeFixedBuffers.get(currentAttributeIndex);
            }
            return buffer.name;
        }

        private void discardCurrentAttribute() {
            currentAttributeIndex--;
        }

        public DecodingTextByteBuffer currentAttributeValue() {
            return attributeFixedBuffers.get(currentAttributeIndex).value;
        }

        public boolean isEmpty() {
            return currentAttributeIndex < 0;
        }

        public int size() {
            return currentAttributeIndex + 1;
        }

        public AttributeFixedBuffer get(int index) {
            return attributeFixedBuffers.get(index);
        }

        public boolean isConsumed(int index) {
            return alreadyConsumed.containsKey(index);
        }

        public void setAsConsumed(int index) {
            this.alreadyConsumed.tryAdd(index);
        }
    }


    private State handleLookingForTagName() throws IOException, ParserException {
        {
            final var b = currentTag.peek();
            if (SLASH == b) {
                // If we encounter a slash right after the left angle bracket, it means we are in a closing tag
                return State.IN_CLOSING_TAG;
            }
            if (isEndOfTagName(b)) {
                // If the first byte is not a valid start of tag name, we throw an exception
                throw new ParserException("Unexpected character at the start of tag name: " + byteToSting(b));
            }
            // If the first char is a '?' or '!', we skip the tag and look for the next one
            // This is typically used for XML declarations or comments
            if (isTagToBeIgnoredDueToFirstChar(b)) {
                // If the first byte is a special character, we skip the tag
                if (currentTag.tryForwardToByteAfter(RIGHT_ANGLE_BRACKET)) {
                    return State.LOOKING_FOR_TAG; // Return to looking for next tag
                }
                throw new ParserException("Unexpected end of stream while skipping tag");
            }
        }
        currentAttributes.newTag(); // Reset attributes for the new tag

        State[] state = {State.END};
        currentTag.setCurrentByteAsStartPositon();
        currentTag.consumeBytes(b -> {
            try {
                if (b == END_OF_STREAM) {
                    throw new RuntimeException(
                            new ParserException("Unexpected end of stream while looking for tag name"));
                }
                if (isEndOfTagName(b)) {
                    currentTag.setEndPositionExclusive();
                    currentTag.abort();
                    switch (b) {
                        case RIGHT_ANGLE_BRACKET -> {
                            state[0] = State.AT_END_OF_OPENING_TAG;
                        }
                        case SLASH -> {
                            if(currentTag.tryForwardToByteAfter(RIGHT_ANGLE_BRACKET)) {
                                state[0] = State.AT_END_OF_SELF_CLOSING_TAG;
                            } else {
                                throw new ParserException("Unexpected end of stream while looking for right angle bracket in self-closing tag");
                            }
                        }
                        default -> state[0] = State.LOOKING_FOR_ATTRIBUTE_NAME;
                    }
                }
            }catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return state[0];
    }

    private State handleLookingForAttributeValue() throws IOException, ParserException {
        State[] state = {State.END};
        final var attributeValue = currentAttributes.currentAttributeValue();
        attributeValue.copyRemainingBytesFromPredecessor();
        attributeValue.consumeBytes(b -> {
            try {
                if (b == DOUBLE_QUOTE) {
                    attributeValue.skip(); // Move to the next byte after the double quote
                    attributeValue.setCurrentByteAsStartPositon();
                    attributeValue.abort();
                    if (attributeValue.tryForwardAndSetEndPositionExclusive(DOUBLE_QUOTE)) {
                        attributeValue.setEndPositionExclusive();
                        attributeValue.skip();
                        state[0] = State.LOOKING_FOR_ATTRIBUTE_NAME;
                        return;
                    }
                    throw new ParserException("Unexpected end of stream while looking for double quote as end of value");
                }
                if (isWhitespace(b)) {
                    return; // Skip whitespace
                }
                if (b == END_OF_STREAM) {
                    throw new ParserException("Unexpected end of stream while looking for attribute value");
                }
                throw new ParserException("Expected '\"' to start attribute value, but found '" + byteToSting(b) + "'");
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return state[0];
    }

    private State handleLookingForAttributeName() throws IOException {
        State[] state = {State.END};

        final var attributeName = currentAttributes.newAttribute();
        attributeName.copyRemainingBytesFromPredecessor();

        // read to the start of the attribute name
        attributeName.consumeBytes(b -> {
            try {
                if (b == END_OF_STREAM) {
                    throw new ParserException("Unexpected end of stream while looking for attribute name");
                }
                if (isWhitespace(b)) {
                    return; // Skip whitespace
                }
                switch (b) {
                    case SLASH -> {
                        if(attributeName.tryForwardToByteAfter(RIGHT_ANGLE_BRACKET)) {
                            state[0] = State.AT_END_OF_SELF_CLOSING_TAG;
                            attributeName.abort();
                            return;
                        } else {
                            throw new ParserException("Unexpected end of stream while looking for right angle bracket in self-closing tag");
                        }
                    }
                    case RIGHT_ANGLE_BRACKET -> {
                        attributeName.skip();
                        state[0] = State.AT_END_OF_OPENING_TAG; // End of tag
                        attributeName.abort();
                        return;

                    }

                    case LEFT_ANGLE_BRACKET, EQUALITY_SIGN ->
                            throw new ParserException("Unexpected character '" + byteToSting(b) + "' while looking for attribute name");
                }
                attributeName.abort();
                state[0] = State.LOOKING_FOR_ATTRIBUTE_NAME;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        // If we are not in the state of looking for attribute name, we return the current state
        if(state[0] != State.LOOKING_FOR_ATTRIBUTE_NAME) {
            currentAttributes.discardCurrentAttribute();
            return state[0];
        }
        state[0] = State.END;
        attributeName.setCurrentByteAsStartPositon();
        // Now we are at the start of the attribute name, we can read it
        attributeName.consumeBytes(b -> {
            try {
                if (isAngleBrackets(b)) {
                    throw new ParserException("Unexpected character in attribute name: " + byteToSting(b));
                }
                if (EQUALITY_SIGN == b) {
                    // Found equality sign, we can return to looking for attribute value
                    attributeName.abort(); // If we encounter whitespace, we stop reading the attribute name
                    attributeName.setEndPositionExclusive();
                    attributeName.skip(); // Move to the next byte after the equality sign
                    state[0] = State.LOOKING_FOR_ATTRIBUTE_VALUE;
                    return;
                }
                if (isWhitespace(b)) {
                    attributeName.setEndPositionExclusive();
                    attributeName.abort(); // If we encounter whitespace, we stop reading the attribute name
                    //look for equality sign, while ignoring whitespace
                    attributeName.consumeBytes(b1 -> {
                        try {
                            if (b1 == EQUALITY_SIGN) {
                                // Found equality sign, we can return to looking for attribute value
                                attributeName.abort();
                                attributeName.skip(); // Move to the next byte after the equality sign
                                state[0] = State.LOOKING_FOR_ATTRIBUTE_VALUE;
                                return;
                            }
                            if (!isWhitespace(b1)) {
                                if (b1 == END_OF_STREAM) {
                                    throw new RuntimeException(
                                            new ParserException("Unexpected end of stream while looking for equality sign after attribute name"));
                                }

                                throw new RuntimeException(
                                        new ParserException("Unexpected character '" + byteToSting(b1) + "' while looking for equality sign after attribute name."));
                            }
                        }
                        catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                }
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return state[0];
    }

    private State handleLookingForTag() throws IOException {
        currentTag.copyRemainingBytesFromPredecessor();
        if(currentTag.tryForwardToByteAfter(LEFT_ANGLE_BRACKET)){
            return State.LOOKING_FOR_TAG_NAME;
        }
        return State.END;
    }

    public static String byteToSting(Byte b) {
        if (b == null) {
            return "null";
        }
        if (b == END_OF_STREAM) {
            return "END_OF_STREAM";
        }
        return UTF_8.decode(ByteBuffer.wrap(new byte[]{b})).toString();
    }
}

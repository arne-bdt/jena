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

import org.apache.commons.io.input.BufferedFileChannelInputStream;
import org.apache.jena.atlas.lib.Cache;
import org.apache.jena.atlas.lib.CacheFactory;
import org.apache.jena.cimxml.collections.JenaHashMap;
import org.apache.jena.cimxml.collections.JenaHashSet;
import org.apache.jena.cimxml.utils.*;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.iri3986.provider.IRIProvider3986;
import org.apache.jena.irix.IRIProvider;
import org.apache.jena.irix.IRIx;
import org.apache.jena.riot.system.StreamRDF;

import javax.xml.XMLConstants;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

import static org.apache.jena.cimxml.utils.ParserConstants.*;
import static org.apache.jena.cimxml.utils.ParserConstants.isEndOfTagName;

public class CIMParser {
    private static final char CHAR_SHARP = '#';
    private static final String STRING_EMPTY = "";
    private static final String STRING_NAMESPACE_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

    private static final int MAX_BUFFER_SIZE = 256*4096; // 256 KB
    private static final int MAX_LENGTH_OF_FRAGMENT = 4096; // Maximum length for tag names

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


    private static final Node NODE_RDF_TYPE = NodeFactory.createURI(STRING_NAMESPACE_RDF + "type");

    private final Path filePath;
    private final FileChannel fileChannel;
    private final InputStream inputStream;
    private final JenaHashMap<SpecialByteBuffer, NamespaceIriPair> prefixToNamespace
            = new JenaHashMap<>(8);
    private final Cache<SpecialByteBuffer, Node> tagOrAttributeNameToUriNode
            = CacheFactory.createSimpleCache(8192);
    private final StreamRDF streamRDFSink;

    private final StreamBufferRoot root = new StreamBufferRoot(MAX_LENGTH_OF_FRAGMENT);
    private final QNameByteBuffer currentTag = new QNameByteBuffer(root);
    private final AttributeCollection currentAttributes = new AttributeCollection(root);
    private final DecodingTextByteBuffer currentTextContent = new DecodingTextByteBuffer(root);

    private final Deque<Element> elementStack = new ArrayDeque<>();

    //private final Map<SpecialByteBuffer, Node> iriToNode = new HashMap<>();
    private final JenaHashMap<NamespaceAndQName, RDFDatatype> iriToDatatype = new JenaHashMap<>();
    private final JenaHashMap<SpecialByteBuffer, Node> blankNodeToNode = new JenaHashMap<>();
    // A map to store langSet and avoid to copy SpecialByteBuffer objects unnecessarily
    private final JenaHashSet<SpecialByteBuffer> langSet = new JenaHashSet<>();
    // A map to store baseSet and avoid to copy NamespaceFixedByteArrayBuffer objects unnecessarily
    private final JenaHashMap<SpecialByteBuffer, NamespaceIriPair> baseSet = new JenaHashMap<>();
    private final Cache<NamespaceAndQName, Node> iriNodeCacheWithNamespace = CacheFactory.createSimpleCache(8192);
    private final Cache<SpecialByteBuffer, Node> iriNodeCacheWithoutNamespace = CacheFactory.createSimpleCache(8192);

    private record NamespaceIriPair(ByteArrayKey namespace, IRIx iri) {}
    private record NamespaceAndQName(ByteArrayKey namespace, SpecialByteBuffer qname) {}

    private final IRIProvider iriProvider = new IRIProvider3986();

    private NamespaceIriPair baseNamespace = null;
    private NamespaceIriPair defaultNamespace = null;
    private int minPrefixLength = 3;

    public void setBaseNamespace(String base) {
        baseNamespace = new NamespaceIriPair(
                new ByteArrayKey(base),
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

    private static class Fragment {
        final StreamBufferRoot root;
        final QNameByteBuffer tag;
        final DecodingTextByteBuffer textContent;
        final AttributeCollection attributes;
        private Fragment(StreamBufferRoot root) {
            this.root = root;
            this.tag = new QNameByteBuffer(root);
            this.textContent = new DecodingTextByteBuffer(root);
            this.attributes = new AttributeCollection(root);
        }
        public void reset() {
            tag.reset();
            textContent.reset();
            attributes.reset();
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
        try (inputStream; var is = new BufferedInputStream(inputStream, MAX_BUFFER_SIZE)) {
            root.setInputStream(is);;
            var state = State.LOOKING_FOR_TAG;
            while (state != State.END) {
                state = switch (state) {
                    case LOOKING_FOR_TAG -> handleLookingForTag();
                    case LOOKING_FOR_TAG_NAME -> handleLookingForTagName();
                    case LOOKING_FOR_ATTRIBUTE_NAME -> handleLookingForAttributeName();
                    case LOOKING_FOR_ATTRIBUTE_VALUE -> handleLookingForAttributeValue();
                    case AT_END_OF_OPENING_TAG -> {
                        var s = handleAtEndOfOpeningTagIncludingRDF_RDF();
                        if (s == State.LOOKING_FOR_TAG && TAG_RDF_RDF.equals(currentTag)) {
                            yield State.END;
                        }
                        yield s;
                    }
                    case AT_END_OF_SELF_CLOSING_TAG -> {
                        var s = handleSelfClosingTagIncludingRDF_RDF();
                        if (s == State.LOOKING_FOR_TAG && TAG_RDF_RDF.equals(currentTag)) {
                            yield State.END;
                        }
                        yield s;
                    }
                    case IN_CLOSING_TAG -> handleClosingTag();
                    case IN_TEXT_CONTENT -> handleTextContent();
                    case END -> State.END;
                };
            }
            state = State.LOOKING_FOR_TAG;
            while (state != State.END) {
                state = switch (state) {
                    case LOOKING_FOR_TAG -> handleLookingForTag();
                    case LOOKING_FOR_TAG_NAME -> handleLookingForTagName();
                    case LOOKING_FOR_ATTRIBUTE_NAME -> handleLookingForAttributeName();
                    case LOOKING_FOR_ATTRIBUTE_VALUE -> handleLookingForAttributeValue();
                    case AT_END_OF_OPENING_TAG -> handleAtEndOfOpeningTagExceptForRDF_RDF();
                    case AT_END_OF_SELF_CLOSING_TAG -> handleSelfClosingTagExceptForRDF_RDF();
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
        currentTextContent.setCurrentByteAsStartPositon();
        if(!currentTextContent.tryConsumeToEndOfTextContent()) {
            throw new ParserException("Unexpected end of stream while looking for opening tag after text content: "
                    + currentTextContent.decodeToString());
        }
        var nextByte = currentTextContent.setEndPositionExclusiveAndSkipAndPeek(); // Skip the LEFT_ANGLE_BRACKET
        if (nextByte == SLASH) {
            // the text content must be a literal as the next tag is a closing tag.
            /*idea for improvements: have cached literals for some datatypes
            --> this is only possible in CIM/XML if we have a schema with the datatypes*/
            final var object = NodeFactory.createLiteral(
                    currentTextContent.decodeToString(),
                    parent.xmlLang != null ? parent.xmlLang.decodeToString(): null,
                    parent.datatype);
            streamRDFSink.triple(Triple.create(parent.subject, parent.predicate, object));
        }
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

    private State handleSelfClosingTagIncludingRDF_RDF() throws ParserException, IOException {
        switch (handleAtEndOfOpeningTagIncludingRDF_RDF()) {
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

    private State handleSelfClosingTagExceptForRDF_RDF() throws ParserException, IOException {
        switch (handleAtEndOfOpeningTagExceptForRDF_RDF()) {
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

    private State handleAtEndOfOpeningTagIncludingRDF_RDF() throws ParserException, IOException {
        final var parent = elementStack.peek();
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
        // if the parent was a predicate, we need to create a triple
        final var current = elementStack.peek();
        if (parent != null && parent.predicate != null && current != null && current.subject != null) {
            streamRDFSink.triple(Triple.create(parent.subject, parent.predicate, current.subject));
        }
        return state;
    }

    private State handleAtEndOfOpeningTagExceptForRDF_RDF() throws ParserException, IOException {
        final var parent = elementStack.peek();
        State state;
        if(TAG_RDF_DESCRIPTION.equals(currentTag)) {
            state = handleTagRdfDescription();
        } else if (TAG_RDF_LI.equals(currentTag)) {
            state = handleRdfLi();
        } else {
            state = handleOtherTag();
        }
        // if the parent was a predicate, we need to create a triple
        final var current = elementStack.peek();
        if (parent != null && parent.predicate != null && current != null && current.subject != null) {
            streamRDFSink.triple(Triple.create(parent.subject, parent.predicate, current.subject));
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

                if (parent.predicate != null) {
                    streamRDFSink.triple(Triple.create(parent.subject, parent.predicate, current.subject));
                }
            }

            // Now process the remaining attributes, which must be literals
            for(int i = 0; i< currentAttributes.size(); i++) {
                final var attribute = currentAttributes.get(i);
                if (attribute.isConsumed()) {
                    continue; // skip
                }
                var predicate = getOrCreateNodeForTagOrAttributeName(attribute.name());
                var object = NodeFactory.createLiteralString(attribute.value().decodeToString());
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
            final var attribute = currentAttributes.get(i);
            if (attribute.isConsumed()) {
                continue; // Skip already consumed attributes
            }
            switch (attribute.name().length()) {
                case 12 -> {
                    if (ATTRIBUTE_RDF_RESOURCE.equals(attribute.name())) {
                        // rdf:resource
                        attribute.setConsumed();
                        var predicate = getOrCreateNodeForTagOrAttributeName(currentTag);
                        var object = getOrCreateNodeForIri(current.xmlBase, attribute.value());
                        streamRDFSink.triple(Triple.create(current.subject, predicate, object));
                        elementStack.push(current);
                        return State.LOOKING_FOR_TAG;
                    }
                    if (ATTRIBUTE_RDF_DATATYPE.equals(attribute.name())) {
                        // rdf:datatype
                        attribute.setConsumed();
                        current.predicate = getOrCreateNodeForTagOrAttributeName(currentTag);
                        current.datatype = getOrCreateDatatypeForIri(current.xmlBase, attribute.value());
                        elementStack.push(current);
                        return State.IN_TEXT_CONTENT;
                    }
                }
                case 13 -> {
                    if (ATTRIBUTE_RDF_PARSE_TYPE.equals(attribute.name())) {
                        // rdf:parseType
                        attribute.setConsumed();
                        final var parseType = attribute.value();
                        if (ATTRIBUTE_VALUE_RDF_PARSE_TYPE_LITERAL.equals(parseType)) {
                            throw new ParserException("rdf:parseType='Literal' is not supported in CIM/XML");
                        } else if (ATTRIBUTE_VALUE_RDF_PARSE_TYPE_RESOURCE.equals(parseType)) {
                            var predicate = getOrCreateNodeForTagOrAttributeName(currentTag);
                            current.subject = NodeFactory.createBlankNode(); // Create a blank node as subject
                            streamRDFSink.triple(Triple.create(parent.subject, predicate, current.subject));
                            elementStack.push(current);
                            return State.LOOKING_FOR_TAG;
                        } else if (ATTRIBUTE_VALUE_RDF_PARSE_TYPE_COLLECTION.equals(parseType)) {
                            throw new ParserException("rdf:parseType='Collection' is not supported in CIM/XML");
                        } else if (ATTRIBUTE_VALUE_RDF_PARSE_TYPE_STATEMENT.equals(parseType)) {
                            throw new ParserException("rdf:parseType='Statement' is not yet supported in this parser");
                        } else {
                            throw new ParserException("Unknown rdf:parseType: " + parseType.decodeToString());
                        }
                    }
                }
            }
        }
        // process remaining attributes as literals
        for (var i = 0; i < currentAttributes.size(); i++) {
            final var attribute = currentAttributes.get(i);
            if (attribute.isConsumed()) {
                continue; // Skip already consumed attributes
            }
            var predicate = getOrCreateNodeForTagOrAttributeName(attribute.name());
            var object = NodeFactory.createLiteralString(attribute.value().decodeToString());
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
            if(attribute.name().length() == 8) { // xml:lang or xml:base
                if (ATTRIBUTE_XML_LANG.equals(attribute.name())) {
                    attribute.setConsumed();
                    // If the attribute is xml:lang, set the xmlLang for the element
                    xmlLang = langSet.getMatchingKey(attribute.value());
                    if (xmlLang == null) {
                        xmlLang = attribute.value().copy();
                        langSet.tryAdd(xmlLang); // Store the xml:lang value to avoid copying
                    }
                } else if (ATTRIBUTE_XML_BASE.equals(attribute.name())) {
                    attribute.setConsumed();
                    // If the attribute is xml:base, set the xmlBase for the element
                    var namespace = attribute.value().copy();
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
            var node = iriNodeCacheWithoutNamespace.getIfPresent(iriQName);
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
            var node = iriNodeCacheWithNamespace.getIfPresent(cacheKey);
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
        var uriNode = tagOrAttributeNameToUriNode.getIfPresent(tagOrAttributeName);
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
            uriNode = NodeFactory.createURI(
                    namespace.namespace.decodeToString() + tagOrAttributeName.getLocalPart().decodeToString());
            tagOrAttributeNameToUriNode.put(tagOrAttributeName.copy(), uriNode);
        }
        return uriNode;
    }

    private Node getOrCreateNodeForRdfId(NamespaceIriPair xmlBase, final DecodingTextByteBuffer rdfId) throws ParserException {
        if(xmlBase == null) {
            throw new ParserException("rdf:ID attribute found without base URI");
        }
        var cacheKey = new NamespaceAndQName(xmlBase.namespace, rdfId);
        var node = iriNodeCacheWithNamespace.getIfPresent(cacheKey);
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
            final var attribute = currentAttributes.get(i);
            if (attribute.isConsumed()) {
                continue; // Skip already consumed attributes
            }
            if (ATTRIBUTE_RDF_ABOUT.equals(attribute.name())) {
                // rdf:about
                attribute.setConsumed();
                return getOrCreateNodeForIri(xmlBase, attribute.value());
            }
            if (ATTRIBUTE_RDF_ID.equals(attribute.name())) {
                // rdf:ID here the value is relative to the xmlBase
                attribute.setConsumed();
                return getOrCreateNodeForRdfId(xmlBase, attribute.value());
            }
            if (ATTRIBUTE_RDF_NODE_ID.equals(attribute.name())) {
                // rdf:nodeID
                attribute.setConsumed();
                return getOrCreateBlankNodeWithIdentifier(attribute.value());
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
            final var attribute = currentAttributes.get(i);
            if (attribute.isConsumed()) {
                continue; // skip
            }
            var predicate = getOrCreateNodeForTagOrAttributeName(attribute.name());
            var object = NodeFactory.createLiteralString(attribute.value().decodeToString());
            streamRDFSink.triple(Triple.create(current.subject, predicate, object));
        }
        elementStack.push(current);
        return State.LOOKING_FOR_TAG;
    }

    private State handleTagRdfRdf() throws ParserException {
        // if the tag is rdf:RDF, it contains only the namespaces as attributes and nothing else
        minPrefixLength = 3; // reset minPrefixLength
        if (!currentAttributes.isEmpty()) {
            // expect all attributes to be namespace attributes
            for(int i = 0; i< currentAttributes.size(); i++) {
                final var attribute = currentAttributes.get(i);
                if(ATTRIBUTE_XML_BASE.equals(attribute.name())) {
                    final var namespace = attribute.value().copy();
                    this.baseNamespace = new NamespaceIriPair(
                            namespace,
                            iriProvider.create(namespace.decodeToString()));
                    this.streamRDFSink.base(baseNamespace.iri.str());
                    continue;
                }
                if(attribute.name().hasPrefix()) {
                    if (!ATTRIBUTE_XMLNS.equals(attribute.name().getPrefix())) {
                        throw new ParserException("Expected attribute 'xmlns:' in rdf:RDF tag but got: "
                                + attribute.name().decodeToString());
                    }
                    final var prefix = attribute.name().getLocalPart().copy();
                    final var namespace = attribute.value().copy();
                    final var iri = iriProvider.create(namespace.decodeToString());
                    // Add the namespace prefix and IRI to the prefixToNamespace map
                    prefixToNamespace.put(
                            prefix,
                            new NamespaceIriPair(namespace, iri));
                    this.streamRDFSink.prefix(
                            prefix.decodeToString(),
                            iri.str());
                    if(prefix.length() < minPrefixLength) {
                        minPrefixLength = prefix.length();
                    }
                } else {
                    if (!ATTRIBUTE_XMLNS.equals(attribute.name())) {
                        throw new ParserException("Expected attribute 'xmlns:' in rdf:RDF tag but got: "
                                + attribute.name().decodeToString());
                    }
                    final var namespace =  attribute.value().copy();
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


    private State handleLookingForTagName() throws IOException, ParserException {
        root.copyRemainingBytesToStart();
        {
            final var b = currentTag.peek();
            if (SLASH == b) {
                // If we encounter a slash right after the left angle bracket, it means we are in a closing tag
                return State.IN_CLOSING_TAG;
            }
            if (isEndOfTagName(b)) {
                // If the first byte is not a valid start of tag name, we throw an exception
                throw new ParserException("Unexpected character at the start of tag name: " + byteToString(b));
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
        currentAttributes.reset(); // Reset attributes for the new tag

        State state;
        currentTag.setCurrentByteAsStartPositon();
        currentTag.skip(minPrefixLength-1);
        if (currentTag.tryConsumeToEndOfTagName()) {
            switch (currentTag.setEndPositionExclusiveAndPeek()) {
                case RIGHT_ANGLE_BRACKET -> {
                    currentTag.skip(); // Move to the next byte after the right angle bracket
                    state = State.AT_END_OF_OPENING_TAG;
                }
                case SLASH -> {
                    if(currentTag.tryForwardToByteAfter(RIGHT_ANGLE_BRACKET)) {
                        state = State.AT_END_OF_SELF_CLOSING_TAG;
                    } else {
                        throw new ParserException("Unexpected end of stream while looking for right angle bracket in self-closing tag");
                    }
                }
                default -> state = State.LOOKING_FOR_ATTRIBUTE_NAME;
            }
        } else {
            throw new ParserException("Unexpected end of stream while looking for tag name");
        }
        return state;
    }

    private State handleLookingForAttributeValue() throws IOException, ParserException {
        final var attributeValue = currentAttributes.currentAttributeValue();
        if(attributeValue.tryForwardToStartOfAttributeValue()) {
            attributeValue.skip(); // Move to the next byte after the double quote
            attributeValue.setCurrentByteAsStartPositon();
            if (attributeValue.tryConsumeToEndOfAttributeValue()) {
                attributeValue.setEndPositionExclusiveAndSkip();
                return State.LOOKING_FOR_ATTRIBUTE_NAME;
            } else {
                throw new ParserException("Unexpected end of stream while looking for double quote as end of value");
            }
        } else {
            throw new ParserException("Unexpected end of stream or non whitespace character while looking for double quote as start of attribute value");
        }
    }

    private State handleLookingForAttributeName() throws IOException, ParserException {
        final var attributeName = currentAttributes.newAttribute();

        // read to the start of the attribute name
        if (attributeName.tryConsumeUntilNonWhitespace()) {
            // let's check the current byte
            switch (attributeName.peek()) {
                case SLASH -> { // self-closing tag
                    if(attributeName.tryForwardToRightAngleBracket()) {
                        attributeName.skip();
                        currentAttributes.discardCurrentAttribute();
                        return State.AT_END_OF_SELF_CLOSING_TAG;
                    } else {
                        throw new ParserException("Unexpected end of stream while looking for right angle bracket in self-closing tag");
                    }
                }
                case RIGHT_ANGLE_BRACKET -> { // end of opening tag
                    attributeName.skip();
                    currentAttributes.discardCurrentAttribute();
                    return State.AT_END_OF_OPENING_TAG; // End of tag
                }

                case LEFT_ANGLE_BRACKET, EQUALITY_SIGN -> // Invalid characters for attribute name
                        throw new ParserException("Unexpected character '" + byteToString(attributeName.peek()) + "' while looking for attribute name");
            }
        } else {
            throw new ParserException("Unexpected end of stream while looking for attribute name");
        }
        attributeName.setCurrentByteAsStartPositon();
        if (attributeName.tryConsumeUntilEndOfQName()) {
            var b = attributeName.setEndPositionExclusiveAndPeek();
            if(b == EQUALITY_SIGN) {
                attributeName.skip(); // Move to the next byte after the equality sign
                return State.LOOKING_FOR_ATTRIBUTE_VALUE;
            }
            if (attributeName.tryForwardToByteAfter(EQUALITY_SIGN)) {
                return State.LOOKING_FOR_ATTRIBUTE_VALUE;
            }
            throw new ParserException("Unexpected end of stream while looking for equality sign after attribute name: "
                    + attributeName);
        }
        throw new ParserException("Unexpected end of stream while looking for end of attribute name");
    }

    private State handleLookingForTag() throws IOException {
        if(currentTag.tryForwardToByteAfter(LEFT_ANGLE_BRACKET)){
            return State.LOOKING_FOR_TAG_NAME;
        }
        return State.END;
    }

    public static String byteToString(Byte b) {
        if (b == null) {
            return "null";
        }
        if (b == END_OF_STREAM) {
            return "END_OF_STREAM";
        }
        return String.valueOf(b);
    }
}

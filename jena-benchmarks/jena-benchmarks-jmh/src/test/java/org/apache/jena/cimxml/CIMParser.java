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
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.collection.FastHashMap;
import org.apache.jena.riot.system.StreamRDF;

import javax.xml.XMLConstants;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.function.Supplier;

public class CIMParser {
    private static final String STRING_NAMESPACE_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

    private static final int MAX_BUFFER_SIZE = 64 * 4096; // 256 KB
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
    private static final byte SLASH = (byte)'\'';
    private static final byte DOUBLE_COLON = (byte)':';
    private static final byte SHARP = (byte)'#';
    private static final byte END_OF_STREAM = -1;

    private static final ByteArrayKey NAMESPACE_PREFIX_RDF = new ByteArrayKey("rdf");
    private static final ByteArrayKey NAMESPACE_PREFIX_XML = new ByteArrayKey("xml");

    private static final ByteArrayKey ATTRIBUTE_XMLNS = new ByteArrayKey(XMLConstants.XMLNS_ATTRIBUTE);
    private static final ByteArrayKey ATTRIBUTE_RDF_ID = new ByteArrayKey("ID");
    private static final ByteArrayKey ATTRIBUTE_RDF_ABOUT = new ByteArrayKey("about");
    private static final ByteArrayKey ATTRIBUTE_RDF_PARSE_TYPE = new ByteArrayKey("parseType");
    private static final ByteArrayKey ATTRIBUTE_RDF_RESOURCE = new ByteArrayKey("resource");
    private static final ByteArrayKey ATTRIBUTE_RDF_NODE_ID = new ByteArrayKey("nodeID");
    private static final ByteArrayKey ATTRIBUTE_RDF_DATATYPE = new ByteArrayKey("datatype");

    private static final ByteArrayKey ATTRIBUTE_VALUE_RDF_PARSE_TYPE_LITERAL = new ByteArrayKey("Literal");

    private static final ByteArrayKey ATTRIBUTE_XML_BASE = new ByteArrayKey("xml:base");
    private static final ByteArrayKey ATTRIBUTE_XML_LANG = new ByteArrayKey("xml:lang");

    private static final ByteArrayKey TAG_RDF_RDF = new ByteArrayKey("rdf:RDF");
    private static final ByteArrayKey TAG_RDF_DESCRIPTION = new ByteArrayKey("rdf:Description");
    private static final ByteArrayKey TAG_RDF_LI = new ByteArrayKey("rdf:li");

    private static final ByteArrayKey XMLNS_BASE = new ByteArrayKey("base");
    private static final ByteArrayKey XML_DEFAULT_NS_PREFIX = new ByteArrayKey(XMLConstants.DEFAULT_NS_PREFIX);

    private static final int TAG_NAME_MAX_LENGTH = 1024; // Maximum length for tag names
    private static final int ATTRIBUTE_NAME_MAX_LENGTH = 1024; // Maximum length for attribute names
    private static final int ATTRIBUTE_VALUE_MAX_LENGTH = 1024; // Maximum length for attribute values
    private static final ByteArrayKey NAMESPACE_RDF =  new ByteArrayKey(STRING_NAMESPACE_RDF);

    private static final String RDF_LI_PROPERTY_START = STRING_NAMESPACE_RDF + "_";

    private static final Node NODE_RDF_TYPE = NodeFactory.createURI(STRING_NAMESPACE_RDF + "type");

    private final Path filePath;
    private final FileChannel fileChannel;
    private final InputStream inputStream;
    private final ByteArrayMap<SpecialByteBuffer> prefixToNamespace = new ByteArrayMap<>(8, 8);
    private final ByteArrayMap<Node> attributeToPredicate = new ByteArrayMap<>(256, 8);
    //private final IRIProvider iriProvider = new IRIProvider3986();
    private final StreamRDF streamRDFSink;

    private final QNameFixedByteArrayBuffer fixedBufferForTagName = new QNameFixedByteArrayBuffer(TAG_NAME_MAX_LENGTH);
    private final AttributeCollection attributeCollection = new AttributeCollection();
    private SpecialByteBuffer baseNamespace = null;
    private SpecialByteBuffer defaultNamespace = null;
    private final Deque<Element> elementStack = new ArrayDeque<>();
    private final Map<SpecialByteBuffer, Node> subjectToNode = new HashMap<>();
    private final Map<SpecialByteBuffer, Node> blankNodeToNode = new HashMap<>();
    private final Map<Node, Integer> subjectToListIndex = new HashMap<>();
    private final Map<Integer, Node> listIndexToProperty = new HashMap<>();

    // A map to store langSet and avoid to copy SpecialByteBuffer objects unnecessarily
    private final Map<SpecialByteBuffer, SpecialByteBuffer> langSet = new HashMap<>();
    // A map to store baseSet and avoid to copy SpecialByteBuffer objects unnecessarily
    private final Map<SpecialByteBuffer, SpecialByteBuffer> baseSet = new HashMap<>();


    private record Element(Node subject, SpecialByteBuffer xmlBase, SpecialByteBuffer xmlLang) {}

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
        prefixToNamespace.put(NAMESPACE_PREFIX_RDF,
                NAMESPACE_RDF);
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

    private class ParserException extends Exception {
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
        if(!inputStream.markSupported()) {
            throw new ParserException("InputStream must support mark/reset operations");
        }
        var state = State.LOOKING_FOR_TAG;
        while (state != State.END) {
            state = switch (state) {
                case LOOKING_FOR_TAG -> handleLookingForTag(inputStream);
                case LOOKING_FOR_TAG_NAME -> handleLookingForTagName(inputStream);
                case LOOKING_FOR_ATTRIBUTE_NAME -> handleLookingForAttributeName(inputStream);
                case LOOKING_FOR_ATTRIBUTE_VALUE -> handleLookingForAttributeValue(inputStream);
                case AT_END_OF_OPENING_TAG -> handleAtEndOfOpeningTag();
                case AT_END_OF_SELF_CLOSING_TAG -> null;
                case IN_CLOSING_TAG -> null;
                case IN_TEXT_CONTENT -> null;
                case END -> null;
            };
        };
    }

    private State handleAtEndOfOpeningTag() throws ParserException {
        if(TAG_RDF_RDF.equals(fixedBufferForTagName)) {
            return handleTagRdfRdf();
        }
        if(TAG_RDF_DESCRIPTION.equals(fixedBufferForTagName)) {
            return handleTagRdfDescription();
        }
        if(TAG_RDF_LI.equals(fixedBufferForTagName)) {
            return handleRdfLi();
        }
        return handleAnyTag();
    }

    private State handleAnyTag() throws ParserException {
        var parent = elementStack.peek();
        SpecialByteBuffer tagNamespace;
        if(fixedBufferForTagName.hasPrefix()) {
            tagNamespace = prefixToNamespace.get(fixedBufferForTagName.getPrefix());
            if(tagNamespace == null) {
                throw new ParserException("Undefined namespace prefix in: "
                        + fixedBufferForTagName.decodeToString());
            }
        } else {
            if(defaultNamespace == null) {
                throw new ParserException("No default namespace defined for tag: "
                        + fixedBufferForTagName.decodeToString());
            }
            tagNamespace = defaultNamespace;
        }
        SpecialByteBuffer xmlBase;
        SpecialByteBuffer xmlLang = null;
        if(parent != null) {
            // If there is a parent element, inherit its xml:base and xml:lang attributes
            xmlBase = parent.xmlBase;
            xmlLang = parent.xmlLang;
        } else {
            // If no parent, use the base namespace defined in rdf:RDF
            xmlBase = this.baseNamespace;
        }
        var indexOfXmlLang = -1;
        var indexOfXmlBase = -1;
        // look for xml:lang and xml:base attributes
        for (var i = 0; i < attributeCollection.size(); i++) {
            final var attribute = attributeCollection.get(i);
            if (ATTRIBUTE_XML_LANG.equals(attribute.name)) {
                // If the attribute is xml:lang, set the xmlLang for the element
                xmlLang = langSet.get(attribute.value);
                if (xmlLang == null) {
                    xmlLang = attribute.value.copy();
                    langSet.put(xmlLang, xmlLang); // Store the xml:lang value to avoid copying
                }
                indexOfXmlLang = i; // Memorize the index of the xml:lang attribute
            } else if (ATTRIBUTE_XML_BASE.equals(attribute.name)) {
                // If the attribute is xml:base, set the xmlBase for the element
                xmlBase = baseSet.get(attribute.value);
                if (xmlBase == null) {
                    xmlBase = attribute.value.copy();
                    baseSet.put(xmlBase, xmlBase); // Store the xml:base value to avoid copying
                }
                indexOfXmlBase = i; // Memorize the index of the xml:base attribute
            }
        }
        // process all other attributes
        for (var i = 0; i < attributeCollection.size(); i++) {
            if (i == indexOfXmlLang || i == indexOfXmlBase) {
                continue; // Skip xml:lang and xml:base attributes
            }
            final var attribute = attributeCollection.get(i);

        }
    }


    private State handleTagRdfDescription() throws ParserException {
        var parent = elementStack.peek();
        SpecialByteBuffer xmlBase;
        SpecialByteBuffer xmlLang = null;
        if(parent != null) {
            // If there is a parent element, inherit its xml:base and xml:lang attributes
            xmlBase = parent.xmlBase;
            xmlLang = parent.xmlLang;
        } else {
            // If no parent, use the base namespace defined in rdf:RDF
            xmlBase = this.baseNamespace;
        }
        var indexOfXmlLang = -1;
        var indexOfXmlBase = -1;
        // look for xml:lang and xml:base attributes
        for (var i = 0; i < attributeCollection.size(); i++) {
            final var attribute = attributeCollection.get(i);
            if (ATTRIBUTE_XML_LANG.equals(attribute.name)) {
                // If the attribute is xml:lang, set the xmlLang for the element
                xmlLang = langSet.get(attribute.value);
                if (xmlLang == null) {
                    xmlLang = attribute.value.copy();
                    langSet.put(xmlLang, xmlLang); // Store the xml:lang value to avoid copying
                }
                indexOfXmlLang = i; // Memorize the index of the xml:lang attribute
            } else if (ATTRIBUTE_XML_BASE.equals(attribute.name)) {
                // If the attribute is xml:base, set the xmlBase for the element
                xmlBase = baseSet.get(attribute.value);
                if (xmlBase == null) {
                    xmlBase = attribute.value.copy();
                    baseSet.put(xmlBase, xmlBase); // Store the xml:base value to avoid copying
                }
                indexOfXmlBase = i; // Memorize the index of the xml:base attribute
            }
        }
        // Handle rdf:Description tag
        Node newSubject = null;
        // first iteration to determine current subject by rdf:ID, rdf:about or rdf:nodeID
        int indexOfRdfAboutIdOrNodeId = -1;
        for(int i=0; i<attributeCollection.size(); i++) {
            final var attribute = attributeCollection.get(i);
            // Check if the attribute belongs to the rdf namespace
            if(attribute.name.hasPrefix() && NAMESPACE_PREFIX_RDF.equals(attribute.name.getPrefix())) {
                final var localPart = attribute.name.getLocalPart();
                if(ATTRIBUTE_RDF_ABOUT.equals(localPart)) {
                    // rdf:about
                    SpecialByteBuffer value;
                    boolean isCopyNeeded = true;
                    if(attribute.value.isRelative()) {
                        // If the value is relative, resolve it against the xmlBase
                        if(xmlBase == null) {
                            throw new ParserException("Relative value found without base URI for attribute: "
                                    + attribute.name.decodeToString());
                        }
                        value = xmlBase.join(attribute.value);
                        isCopyNeeded = false; // No need to copy if we are joining with xmlBase
                    } else {
                        // If the value is absolute, use it as is
                        value = attribute.value;
                    }
                    newSubject = subjectToNode.get(value);
                    if(newSubject == null) {
                        if(isCopyNeeded) {
                            value = value.copy();
                        }
                        newSubject = NodeFactory.createURI(value.decodeToString());
                        subjectToNode.put(value, newSubject); // Store the new subject in the map
                    }
                    indexOfRdfAboutIdOrNodeId = i; // Memorize the index of the attribute to ignore it later
                    break; // No need to check further attributes
                } else if(ATTRIBUTE_RDF_ID.equals(localPart)) {
                    // rdf:ID here the value is relative to the xmlBase
                    if(xmlBase == null) {
                        throw new ParserException("rdf:ID attribute found without base URI");
                    }
                    final SpecialByteBuffer absoluteIri = getAbsoluteIriForRdfId(xmlBase, attribute.value);
                    newSubject = subjectToNode.get(absoluteIri);
                    if(newSubject == null) {
                        newSubject = NodeFactory.createURI(absoluteIri.decodeToString());
                        subjectToNode.put(absoluteIri, newSubject); // Store the new subject in the map
                    }
                    indexOfRdfAboutIdOrNodeId = i; // Memorize the index of the attribute to ignore it later
                    break; // No need to check further attributes
                } else if(ATTRIBUTE_RDF_NODE_ID.equals(localPart)) {
                    // rdf:nodeID
                    newSubject = blankNodeToNode.get(attribute.value);
                    if(newSubject == null) {
                        var copy = attribute.value.copy();
                        newSubject = NodeFactory.createBlankNode(copy.decodeToString());
                        blankNodeToNode.put(copy, newSubject);
                    }
                    indexOfRdfAboutIdOrNodeId = i; // Memorize the index of the attribute to ignore it later
                    break; // No need to check further attributes
                }
            }
        }
        // If no subject was found, create a blank node as the subject
        if(newSubject == null) {
            newSubject = NodeFactory.createBlankNode();
        }
        // Now process the remaining attributes, which must be literals
        for(int i=0; i<attributeCollection.size(); i++) {
            if(i == indexOfRdfAboutIdOrNodeId || i == indexOfXmlLang || i == indexOfXmlBase) {
                continue; // skip
            }
            final var attribute = attributeCollection.get(i);
            var predicate = attributeToPredicate.get(attribute.name);
            if (predicate == null) {
                // If the attribute is not in the map, create a new node
                predicate = NodeFactory.createURI(resolveFullName(attribute.name));
                attributeToPredicate.put(attribute.name.copy(), predicate);
            }
            var object = NodeFactory.createLiteralString(attribute.value.decodeToString());
            streamRDFSink.triple(Triple.create(newSubject, predicate, object));
        }
        elementStack.add(new Element(newSubject, xmlBase, xmlLang));
        return State.LOOKING_FOR_TAG;
    }

    private static SpecialByteBuffer getAbsoluteIriForRdfId(SpecialByteBuffer xmlBas,
                                                            SpecialByteBuffer rdfId) {
        final byte[] combinedData = new byte[xmlBas.length() + 1 + rdfId.length()];
        combinedData[xmlBas.length()] = SHARP; // Use '#' as the separator
        System.arraycopy(xmlBas.getData(), xmlBas.offset(), combinedData, 0, xmlBas.length());
        System.arraycopy(rdfId.getData(), rdfId.offset(), combinedData, xmlBas.length()+1, rdfId.length());
        return new ByteArrayKey(combinedData);
    })

    private State handleTagRdfRdf() throws ParserException {
        // if the tag is rdf:RDF, it contains only the namespaces as attributes and nothing else
        if (!attributeCollection.isEmpty()) {
            // expect all attributes to be namespace attributes
            for(int i=0; i<attributeCollection.size(); i++) {
                final var attribute = attributeCollection.get(i);
                if(attribute.name.hasPrefix()) {
                    if (!ATTRIBUTE_XMLNS.equals(attribute.name.getPrefix())) {
                        throw new ParserException("Expected attribute 'xmlns:' in rdf:RDF tag but got: "
                                + attribute.name.decodeToString());
                    }
                    final var prefix = attribute.name.getLocalPart().copy();
                    // Ignore the rdf namespace prefix, as it is already defined
                    if (!NAMESPACE_PREFIX_RDF.equals(prefix)) {
                        final var namespace = attribute.value.copy();
                        // Add the namespace prefix and IRI to the prefixToNamespace map
                        prefixToNamespace.put(
                                prefix,
                                namespace);
                        this.streamRDFSink.prefix(
                                prefix.decodeToString(),
                                namespace.decodeToString());
                        if(XMLNS_BASE.equals(prefix)) {
                            this.baseNamespace = namespace;
                            this.streamRDFSink.base(baseNamespace.decodeToString());
                        }
                    }
                } else {
                    if (!ATTRIBUTE_XMLNS.equals(attribute.name)) {
                        throw new ParserException("Expected attribute 'xmlns:' in rdf:RDF tag but got: "
                                + attribute.name.decodeToString());
                    }
                    defaultNamespace = XML_DEFAULT_NS_PREFIX; // Default namespace prefix
                }
            }
        }
        return State.LOOKING_FOR_TAG;
    }

    private State handleRdfLi() throws ParserException {
        // Handle rdf:li tag
        final var parent = elementStack.peek();
        if(parent == null) {
            throw new ParserException("rdf:li tag found without a parent in the stack.");
        }
        var listIndex = subjectToListIndex.get(parent);
        if(listIndex == null) {
            listIndex = 1; // Start with index 0 if not found
        } else {
            listIndex++; // Increment the index for the next rdf:li element
        }
        subjectToListIndex.put(parent.subject, listIndex);
        var property = listIndexToProperty.get(listIndex);
        if(property == null) {
            // If the property for this index does not exist, create a new one
            property = NodeFactory.createURI(RDF_LI_PROPERTY_START+ listIndex);
            listIndexToProperty.put(listIndex, property);
        }
        throw new ParserException("rdf:li tag is not supported yet.");
    }

    private String resolveFullName(QNameFixedByteArrayBuffer name) {
        if(name.hasPrefix()) {
            // If the name has a prefix, resolve it against the prefixToNamespace map
            final var localPart = name.getLocalPart();
            final var namespace = prefixToNamespace.get(name.getPrefix());
            if(namespace != null) {
                return namespace.decodeToString() + localPart.decodeToString();
            } else {
                throw new IllegalArgumentException("Unknown prefix: " + name.getPrefix().decodeToString());
            }
        } else {
            // If no prefix, treat it as a local part
            return name.getLocalPart().decodeToString();
        }
    }

//    private SpecialByteBuffer resolveFullName(QNameFixedByteArrayBuffer name) {
//        if(name.hasPrefix()) {
//            // If the name has a prefix, resolve it against the prefixToNamespace map
//            final var localPart = name.getLocalPart();
//            final var namespace = prefixToNamespace.get(name.getPrefix());
//            if(namespace != null) {
//                return namespace.join(localPart);
//            } else {
//                throw new IllegalArgumentException("Unknown prefix: " + name.getPrefix().decodeToString());
//            }
//        } else {
//            // If no prefix, treat it as a local part
//            return name.getLocalPart().copy();
//        }
//    }

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
            return java.nio.ByteBuffer.wrap(this.getData(), 0, this.length());
        }

        default SpecialByteBuffer copy() {
            return new ByteArrayKey(this.copyToByteArray());
        }

        default SpecialByteBuffer join(SpecialByteBuffer other) {
            if (other == null || other.length() == 0) {
                return this.copy();
            }
            final byte[] combinedData = new byte[this.length() + other.length()];
            System.arraycopy(this.getData(), this.offset(), combinedData, 0, this.length());
            System.arraycopy(other.getData(), other.offset(), combinedData, this.length(), other.length());
            return new ByteArrayKey(combinedData);
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
    }

    /**
     * A simple key class for byte arrays, using the first and last byte for hash code calculation.
     * This is a simplified version for demonstration purposes.
     */
    public static class ByteArrayKey implements SpecialByteBuffer {
        private final byte[] data;
        private final int hashCode;
        private String decodedString = null;


        public ByteArrayKey(String string) {
            this(UTF_8.encode(string).array());
            this.decodedString = string;
        }

        public ByteArrayKey(final byte[] data) {
            this.data = data;
            this.hashCode = this.defaultHashCode();
        }

        public byte[] getData() {
            return this.data;
        }

        @Override
        public int offset() {
            return 0;
        }

        public int length() {
            return this.data.length;
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

    public static class ByteArrayKeyMap<E> extends FastHashMap<SpecialByteBuffer, E> {

        public ByteArrayKeyMap(int initialSize) {
            super(initialSize);
        }

        @Override
        protected SpecialByteBuffer[] newKeysArray(int size) {
            return new SpecialByteBuffer[size];
        }

        @Override
        @SuppressWarnings("unchecked")
        protected E[] newValuesArray(int size) {
            return (E[]) new Object[size];
        }
    }

    public static class ByteArrayMap<E> {
        private final int expectedMaxEntriesWithSameLength;
        private ByteArrayKeyMap<E>[] entriesWithSameLength;

        public ByteArrayMap(int expectedMaxByteLength, int expectedEntriesWithSameLength) {
            var positionsSize = Integer.highestOneBit(expectedMaxByteLength << 1);
            if (positionsSize < expectedMaxByteLength << 1) {
                positionsSize <<= 1;
            }
            this.entriesWithSameLength = new ByteArrayKeyMap[positionsSize];
            this.expectedMaxEntriesWithSameLength = expectedEntriesWithSameLength;
        }

        private void grow(final int minimumLength) {
            var newLength = entriesWithSameLength.length << 1;
            while (newLength < minimumLength) {
                newLength = minimumLength << 1;
            }
            final var oldValues = entriesWithSameLength;
            entriesWithSameLength = new ByteArrayKeyMap[newLength];
            System.arraycopy(oldValues, 0, entriesWithSameLength, 0, oldValues.length);
        }

        public void put(SpecialByteBuffer key, E value) {
            final ByteArrayKeyMap<E> map;
            // Ensure the array is large enough
            if (entriesWithSameLength.length < key.length()) {
                grow(key.length());
                map = new ByteArrayKeyMap<>(expectedMaxEntriesWithSameLength);
                entriesWithSameLength[key.length()] = map;
                map.put(key, value);
                return;
            }
            if (entriesWithSameLength[key.length()] == null) {
                map = new ByteArrayKeyMap<>(expectedMaxEntriesWithSameLength);
                entriesWithSameLength[key.length()] = map;
            } else {
                map = entriesWithSameLength[key.length()];
            }
            map.put(key, value);
        }

        public boolean tryPut(SpecialByteBuffer key, E value) {
            final ByteArrayKeyMap<E> map;
            // Ensure the array is large enough
            if (entriesWithSameLength.length < key.length()) {
                grow(key.length());
                map = new ByteArrayKeyMap<>(expectedMaxEntriesWithSameLength);
                entriesWithSameLength[key.length()] = map;
                map.put(key, value);
                return true;
            }
            if (entriesWithSameLength[key.length()] == null) {
                map = new ByteArrayKeyMap<>(expectedMaxEntriesWithSameLength);
                entriesWithSameLength[key.length()] = map;
            } else {
                map = entriesWithSameLength[key.length()];
            }
            return map.tryPut(key, value);
        }

        public E computeIfAbsent(SpecialByteBuffer key, Supplier<E> mappingFunction) {
            final ByteArrayKeyMap<E> map;
            // Ensure the array is large enough
            if (entriesWithSameLength.length < key.length()) {
                grow(key.length());
                map = new ByteArrayKeyMap<>(expectedMaxEntriesWithSameLength);
                entriesWithSameLength[key.length()] = map;
                final var value = mappingFunction.get();
                map.put(key, value);
                return value;
            }
            if (entriesWithSameLength[key.length()] == null) {
                map = new ByteArrayKeyMap<>(expectedMaxEntriesWithSameLength);
                entriesWithSameLength[key.length()] = map;
            } else {
                map = entriesWithSameLength[key.length()];
            }
            return map.computeIfAbsent(key, mappingFunction);
        }

        public E get(SpecialByteBuffer key) {
            if (entriesWithSameLength.length < key.length() || entriesWithSameLength[key.length()] == null) {
                return null;
            }
            return entriesWithSameLength[key.length()].get(key);
        }

        public boolean containsKey(SpecialByteBuffer key) {
            if (entriesWithSameLength.length < key.length() || entriesWithSameLength[key.length()] == null) {
                return false;
            }
            return entriesWithSameLength[key.length()].containsKey(key);
        }

    }

    private static class FixedByteArrayBuffer implements SpecialByteBuffer {
        protected final byte[] data;
        protected int position;

        public FixedByteArrayBuffer(int size) {
            this.data = new byte[size];
            this.position = 0;
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

        public void append(SpecialByteBuffer buffer) {
            if (position + buffer.length() > data.length) {
                throw new IllegalStateException("Buffer overflow when appending: " + buffer.decodeToString());
            }
            System.arraycopy(buffer.getData(), buffer.offset(), data, position, buffer.length());
            position += buffer.length();
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
            return "ByteArrayKey [" + this.decodeToString()  + "]";
        }

        public boolean isFull() {
            return position == data.length;
        }
    }

    private static class ReadonlyByteArrayBuffer implements SpecialByteBuffer {

        private final byte[] data;
        private final int offset;
        private final int length;

        public ReadonlyByteArrayBuffer(byte[] data, int offset, int length) {
            this.data = data;
            this.offset = offset;
            this.length = length;
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
        public String toString() {
            return "ReadonlyByteArrayBuffer [" + this.decodeToString()  + "]";
        }
    }

    private static class QNameFixedByteArrayBuffer extends FixedByteArrayBuffer {
        private int startOfLocalPart = 0; // Index where the local part starts

        public QNameFixedByteArrayBuffer(int bufferSize) {
            super(bufferSize);
        }

        public boolean isRelative() {
            if(position == 0) {
                return false;
            }
            return SHARP == data[0]; // If the first byte is a sharp, it is a relative QName
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
                if (hasPrefix()) {
                    throw new IllegalStateException("Unexpected double colon in QName, already in local part");
                }
                startOfLocalPart = position+1; // Start of local part
            }
            super.append(b);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("QNameFixedByteArrayBuffer [");
            sb.append(this.decodeToString());
            sb.append("]");
            return sb.toString();
        }
    }



    private record AttributeFixedBuffer(QNameFixedByteArrayBuffer name, QNameFixedByteArrayBuffer value) {};
    private static class AttributeCollection {
        private final List<AttributeFixedBuffer> attributeFixedBuffers = new ArrayList<>();
        private int currentAttributeIndex = -1;

        private void newTag() {
            currentAttributeIndex = -1;
        }

        private QNameFixedByteArrayBuffer newAttribute() {
            final AttributeFixedBuffer buffer;
            currentAttributeIndex++;
            if (currentAttributeIndex == attributeFixedBuffers.size()) {
                buffer = new AttributeFixedBuffer(
                        new QNameFixedByteArrayBuffer(ATTRIBUTE_NAME_MAX_LENGTH),
                        new QNameFixedByteArrayBuffer(ATTRIBUTE_VALUE_MAX_LENGTH));
                attributeFixedBuffers.add(buffer);
            } else {
                buffer = attributeFixedBuffers.get(currentAttributeIndex);
                buffer.name.reset();
                buffer.value.reset();
            }
            return buffer.name;
        }

        public FixedByteArrayBuffer currentAttributeValue() {
            return attributeFixedBuffers.get(currentAttributeIndex).value;
        }

        public boolean isEmpty() {
            return currentAttributeIndex < 0;
        }

        public int size() {
            return currentAttributeIndex + 1;
        }

        public AttributeFixedBuffer get(int index) {
            return attributeFixedBuffers.get(currentAttributeIndex);
        }
    }


    private State handleLookingForTagName(InputStream inputStream) throws IOException, ParserException {
        byte b = (byte) inputStream.read();
        if(SLASH == b) {
            // If we encounter a slash right after the left angle bracket, it means we are in a closing tag
            return State.IN_CLOSING_TAG;
        }
        if(isEndOfTagName(b)) {
            // If the first byte is not a valid start of tag name, we throw an exception
            throw new ParserException("Unexpected character at the start of tag name: " + (char) b);
        }
        // If the first char is a '?' or '!', we skip the tag and look for the next one
        // This is typically used for XML declarations or comments
        if(isTagToBeIgnoredDueToFirstChar(b)) {
            // If the first byte is a special character, we skip the tag
            while ((b = (byte) inputStream.read()) != END_OF_STREAM) {
                if (b == RIGHT_ANGLE_BRACKET) {
                    return State.LOOKING_FOR_TAG; // Return to looking for next tag
                }
            }
            throw new ParserException("Unexpected end of stream while skipping tag");
        }
        fixedBufferForTagName.reset();
        fixedBufferForTagName.append(b);
        while ((b = (byte) inputStream.read()) != END_OF_STREAM) {
            if(isEndOfTagName(b)) {
                return switch (b) {
                    case RIGHT_ANGLE_BRACKET -> State.AT_END_OF_OPENING_TAG;
                    case SLASH -> afterSlashExpectClosingTag(inputStream);
                    default -> State.LOOKING_FOR_ATTRIBUTE_NAME;
                };
            }
            fixedBufferForTagName.append(b);
            if(fixedBufferForTagName.isFull()) {
                throw new ParserException("Tag name exceeds maximum length of " + fixedBufferForTagName.length() + " characters");
            }
        }
        throw new ParserException("Unexpected end of stream while looking for tag name");
    }

    private State afterSlashExpectClosingTag(InputStream inputStream) throws IOException, ParserException {
        // If we encounter a slash, it means we are at the end of the opening tag
        // read the next byte to check if it's a right angle bracket
        final byte b = (byte) inputStream.read();
        if (RIGHT_ANGLE_BRACKET != b) {
            if(END_OF_STREAM == b) {
                throw  new ParserException("Unexpected end of stream while looking right angle bracket in self-closing tag");
            }
            throw new ParserException("Unexpected character '" + (char) b + "' after '/' in opening tag");
        }
        return State.AT_END_OF_SELF_CLOSING_TAG; // End of tag
    }

    private State handleLookingForAttributeValue(InputStream inputStream) throws IOException, ParserException {
        final var fixedBufferForAttributeValue = attributeCollection.currentAttributeValue();
        byte b;
        while ((b = (byte) inputStream.read()) != END_OF_STREAM) {
            if (isWhitespace(b)) {
                continue; // Skip whitespace
            }
            if (b == DOUBLE_QUOTE) {
                // Start reading attribute value
                while ((b = (byte) inputStream.read()) != END_OF_STREAM) {
                    if (b == DOUBLE_QUOTE) {
                        // End of attribute value
                        return State.LOOKING_FOR_ATTRIBUTE_NAME; // Return to looking for next attribute name
                    }
                    fixedBufferForAttributeValue.append(b);
                    if (fixedBufferForAttributeValue.isFull()) {
                        throw new ParserException("Attribute value exceeds maximum length of " + fixedBufferForAttributeValue.length() + " characters");
                    }
                }
                throw new ParserException("Unexpected end of stream while looking for end of attribute value");
            } else {
                throw new ParserException("Expected '\"' to start attribute value, but found '" + (char) b + "'");
            }
        }
        throw new ParserException("Unexpected end of stream while looking for attribute value");
    }

    private State handleLookingForAttributeName(InputStream inputStream) throws IOException, ParserException {
        byte b;
        while ((b = (byte) inputStream.read()) != END_OF_STREAM) {
            if (isWhitespace(b)) {
                continue; // Skip whitespace
            }
            switch (b) {
                case SLASH:
                    return afterSlashExpectClosingTag(inputStream);
                case RIGHT_ANGLE_BRACKET:
                    return State.AT_END_OF_OPENING_TAG; // End of tag
                case LEFT_ANGLE_BRACKET, EQUALITY_SIGN:
                    throw new ParserException("Unexpected character '" + (char) b + "' while looking for attribute name");
            }
            // Start reading attribute name
            final var fixedBufferForAttributeName = attributeCollection.newAttribute();
            fixedBufferForAttributeName.append(b);
            while ((b = (byte) inputStream.read()) != END_OF_STREAM) {
                if(isAngleBrackets(b)) {
                    throw new ParserException("Unexpected character in attribute name: " + (char) b);
                }
                if (isWhitespace(b)) {
                    //look for equality sign, while ignoring whitespace
                    while ((b = (byte) inputStream.read()) != END_OF_STREAM) {
                        if (b == EQUALITY_SIGN) {
                            // Found equality sign, we can return to looking for attribute value
                            return State.LOOKING_FOR_ATTRIBUTE_VALUE;
                        } else if (!isWhitespace(b)) {
                            throw new ParserException("Unexpected character '" + (char) b + "' while looking for equality sign after attribute name.");
                        }
                    }
                    throw new ParserException("Unexpected end of stream while looking for equality sign after attribute name");
                }
                if (EQUALITY_SIGN == b) {
                    // Found equality sign, we can return to looking for attribute value
                    return State.LOOKING_FOR_ATTRIBUTE_VALUE;
                }
                fixedBufferForAttributeName.append(b);
                if (fixedBufferForAttributeName.isFull()) {
                    throw new ParserException("Attribute name exceeds maximum length of " + fixedBufferForAttributeName.length() + " characters");
                }
            }
        }
        throw new ParserException("Unexpected end of stream while looking for attribute name");
    }

    private State handleLookingForTag(InputStream inputStream) throws IOException, ParserException {
        byte b;
        while ((b = (byte) inputStream.read()) != END_OF_STREAM) {
            if (LEFT_ANGLE_BRACKET == b) {
                attributeCollection.newTag();
                return State.LOOKING_FOR_TAG_NAME;
            }
        }
        return State.END;
    }
}

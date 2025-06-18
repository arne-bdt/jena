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
import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.GraphMem2Fast;
import org.apache.jena.mem2.GraphMem2Roaring;
import org.apache.jena.mem2.IndexingStrategy;
import org.apache.jena.mem2.collection.FastHashMap;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sys.JenaSystem;
import org.junit.Test;

import javax.xml.XMLConstants;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ParserPoC {

    /* Things to support:
     * <dm:forwardDifferences parseType=\"Statements\"
     * rdf:RDF | rdf:ID | rdf:about | rdf:parseType | rdf:resource | rdf:nodeID | rdf:datatype
     *  rdf:langString
     * | rdf:Description |
     * rdf:parseType="Resource" | rdf:parseType="Statement"
     *
     * Things, that are not supported:
     * <ul>
     *  <li>Namwspace declarations are only supported in the rdf:RDF tag.
     *  <li>rdf:parseType="Collection" is not supported.
     *  <li>Reifying statements using rdf:ID is not supported.
     *  <li>rdf:parseType="Literal" is not supported.
     *  <li>rdf:li is not supported.
     * </ul>
     */

    //private final String file = "C:\\temp\\CGMES_v2.4.15_TestConfigurations_v4.0.3\\MicroGrid\\BaseCase_BC\\CGMES_v2.4.15_MicroGridTestConfiguration_BC_Assembled_CA_v2\\MicroGridTestConfiguration_BC_NL_GL_V2.xml";
    private final String file = "C:\\temp\\v59_3\\AMP_Export_s82_v58_H69.xml";

    private final Charset charset = StandardCharsets.UTF_8;


    /**
     * A simple key class for byte arrays, using the first and last byte for hash code calculation.
     * This is a simplified version for demonstration purposes.
     */
    public static class ByteArrayKey {
        private final byte[] data;
        private final int hashCode;

        public ByteArrayKey(final byte[] data) {
            this.data = data;

            this.hashCode = calculateHashCode();
        }

        public byte[] getData() {
            return this.data;
        }

        /**
         * Returns a hash code based on the first and last byte of the array.
         * This is a simplified version for demonstration purposes.
         */
        private int calculateHashCode() {
            int result = 31 + (int)this.data[0];
            if(this.data.length > 1) {
                result = 31 * result + this.data[this.data.length - 1];
            }
            return result;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;

            if (obj instanceof ByteArrayKey other
                && Arrays.equals(this.data, other.data))
                return true;

            return false;
        }
    }

    public class ByteArrayKeyMap<E> extends FastHashMap<ByteArrayKey, E> {

        public ByteArrayKeyMap(int initialSize) {
            super(initialSize);
        }

        @Override
        protected ByteArrayKey[] newKeysArray(int size) {
            return new ByteArrayKey[size];
        }

        @Override
        @SuppressWarnings("unchecked")
        protected E[] newValuesArray(int size) {
            return (E[]) new Object[size];
        }
    }

    public class ByteArrayMap<E> {
        private static final int DEFAULT_INITIAL_SIZE = 64;
        private ByteArrayKeyMap<E>[] entriesWithSameLength;

        public ByteArrayMap(int expectedMaxByteLength) {
            var positionsSize = Integer.highestOneBit(expectedMaxByteLength << 1);
            if (positionsSize < expectedMaxByteLength << 1) {
                positionsSize <<= 1;
            }
            this.entriesWithSameLength = new ByteArrayKeyMap[positionsSize];
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

        private static byte[] copy(byte[] original, int bytesInKey) {
            final var copy = new byte[bytesInKey];
            System.arraycopy(original, 0, copy, 0, bytesInKey);
            return copy;
        }

        public void putCopy(final byte[] keyToCopy, final int bytesInKey, final E value) {
            putReference(copy(keyToCopy, bytesInKey), value);
        }

        public void putReference(byte[] key, E value) {
            final ByteArrayKeyMap<E> map;
            // Ensure the array is large enough
            if (entriesWithSameLength.length < key.length) {
                grow(key.length);
                map = new ByteArrayKeyMap<>(DEFAULT_INITIAL_SIZE);
                entriesWithSameLength[key.length] = map;
                map.put(new ByteArrayKey(key), value);
                return;
            }
            if (entriesWithSameLength[key.length] == null) {
                map = new ByteArrayKeyMap<>(DEFAULT_INITIAL_SIZE);
                entriesWithSameLength[key.length] = map;
            } else {
                map = entriesWithSameLength[key.length];
            }
            map.put(new ByteArrayKey(key), value);
        }

        public boolean TryPutCopy(byte[] keyToCopy, int bytesInKey, E value) {
            return tryPutReference(copy(keyToCopy, bytesInKey), value);
        }

        public boolean tryPutReference(byte[] key, E value) {
            final ByteArrayKeyMap<E> map;
            // Ensure the array is large enough
            if (entriesWithSameLength.length < key.length) {
                grow(key.length);
                map = new ByteArrayKeyMap<>(DEFAULT_INITIAL_SIZE);
                entriesWithSameLength[key.length] = map;
                map.put(new ByteArrayKey(key), value);
                return true;
            }
            if (entriesWithSameLength[key.length] == null) {
                map = new ByteArrayKeyMap<>(DEFAULT_INITIAL_SIZE);
                entriesWithSameLength[key.length] = map;
            } else {
                map = entriesWithSameLength[key.length];
            }
            return map.tryPut(new ByteArrayKey(key), value);
        }

        public E computeIfAbsentReferential(byte[] key, Supplier<E> mappingFunction) {
            final ByteArrayKeyMap<E> map;
            // Ensure the array is large enough
            if (entriesWithSameLength.length < key.length) {
                grow(key.length);
                map = new ByteArrayKeyMap<>(DEFAULT_INITIAL_SIZE);
                entriesWithSameLength[key.length] = map;
                final var value = mappingFunction.get();
                map.put(new ByteArrayKey(key), value);
                return value;
            }
            if (entriesWithSameLength[key.length] == null) {
                map = new ByteArrayKeyMap<>(DEFAULT_INITIAL_SIZE);
                entriesWithSameLength[key.length] = map;
            } else {
                map = entriesWithSameLength[key.length];
            }
            return map.computeIfAbsent(new ByteArrayKey(key), mappingFunction);
        }

        public E computeIfAbsentCopy(byte[] keyToCopy, int bytesInKey, Supplier<E> mappingFunction) {
            return computeIfAbsentReferential(copy(keyToCopy, bytesInKey), mappingFunction);
        }

        public E get(byte[] key) {
            if (entriesWithSameLength.length < key.length || entriesWithSameLength[key.length] == null) {
                return null;
            }
            return entriesWithSameLength[key.length].get(new ByteArrayKey(key));
        }

        public boolean containsKey(byte[] key) {
            if (entriesWithSameLength.length < key.length || entriesWithSameLength[key.length] == null) {
                return false;
            }
            return entriesWithSameLength[key.length].containsKey(new ByteArrayKey(key));
        }

    }

    public record CIMClass(String name, String namespace, String type) {
        public CIMClass(String name, String namespace) {
            this(name, namespace, "class");
        }
    }

    public class CIMParserPoC {
        private static final int maxBufferSize = 64 * 4096; // 256 KB
        private static final Charset charset = StandardCharsets.UTF_8;
        private static final byte[] LEFT_ANGLE_BRACKET = "<".getBytes(charset);
        private static final byte[] RIGHT_ANGLE_BRACKET = ">".getBytes(charset);
        private static final byte[] XML_DECL_START = "<?xml".getBytes(StandardCharsets.UTF_8);
        private static final byte[] XML_DECL_END = "?>".getBytes(StandardCharsets.UTF_8);
        private static final byte[] RDF_RDF = "<rdf:RDF".getBytes(StandardCharsets.UTF_8);
        private static final byte[] XMLNS = "xmlns".getBytes(StandardCharsets.UTF_8);
        private static final byte[] COMMENT_START = "<!--".getBytes(StandardCharsets.UTF_8);
        private static final byte[] COMMENT_END = "-->".getBytes(StandardCharsets.UTF_8);
        private static final int MARK_SIZE = 16; // Size of the mark buffer for input stream
        private static final int MAX_TAG_LENGTH = 64; // Maximum length of a tag name
        private final ByteArrayMap<String> tagMap = new ByteArrayMap<>(MAX_TAG_LENGTH);

        // Parser state
        private enum State {
            /**
             * Initial state, looking for the XML declaration '&lt;?xml ' plus a single whitespace character.
             * Ignoring whitespace and comments.
             * Next state is {@link #LOOKING_FOR_RDF_RDF}.
             */
            LOOKING_FOR_XML_DECLARATION,

            /**
             * Looking for the end of the XML declaration '?>', ignoring anything else.
             * Next state is {@link #LOOKING_FOR_RDF_RDF}.
             */
            LOOKING_FOR_END_OF_XML_DECLARATION,

            /**
             * Looking for the '&lt;rdf:RDF ' opening tag after the XML declaration, plus a single whitespace character.
             * This lookup ignores anything but the '<rdf:RDF' tag.
             * Next state is {@link #LOOKING_FOR_XMLNS_IN_RDF_RDF}.
             */
            LOOKING_FOR_RDF_RDF,

            /**
             * Looking for the 'xmlns:' attribute in the 'rdf:RDF' tag until the closing '>'.
             * If closing '>' has been found, the next state is {@link #LOOKING_FOR_SUBJECT},
             * if closing '/>' has been found, the next state is {@link #LOOKING_FOR_RDF_RDF}
             * else the next state is {@link #LOOKING_FOR_XMLNS_PREFIX}.
             */
            LOOKING_FOR_XMLNS_IN_RDF_RDF,

            /**
             * Looking for a 'prefix:"' the 'xmlns:' attributes of the 'rdf:RDF' tag.
             * The prefix ends with ':"'.
             * The next state is {@link #LOOKING_FOR_XMLNS_IRI}.
             */
            LOOKING_FOR_XMLNS_PREFIX,
            /**
             * Looking for the namespace IRI in the 'xmlns:prefix="namespace"' attribute of the 'rdf:RDF' tag.
             * Namespace must end with double quotes '"' and it must be an IRI.
             * The next state is {@link #LOOKING_FOR_XMLNS_IN_RDF_RDF}.
             */
            LOOKING_FOR_XMLNS_IRI,
            /**
             * Looking for the next opening tag '<TAG ', which must be a subject opening tag.
             * The subject is identified when the next whitespace is found after the tag name.
             * A subject opening tag either starts with <rdf:Description or '<IRI' or '<cim:Class',
             * where 'IRI' may be any IRI, 'cim' may be any namespace prefix and 'Class' is the class name.
             * The subject needs to contain either 'rdf:about' or rdf:ID' as only attribute.
             * Ten next state is {@link #LOOKING_FOR_RDF_ID_OR_ABOUT}.
             */
            LOOKING_FOR_SUBJECT,
            /**
             * Looking for the 'rdf:ID="' or 'rdf:about="' attribute in the subject opening tag.
             * Finding '&gt;' or '&lt;' is considered an error.
             * The next state is {@link #LOOKING_FOR_RDF_ID_OR_ABOUT_VALUE}.
             */
            LOOKING_FOR_RDF_ID_OR_ABOUT,
            /**
             * Looking for the value of the 'rdf:ID' or 'rdf:about' attribute.
             * The value must end with a double quote '"' and it must be an IRI.
             * If it starts with "_" it is a UUID and the underscore needs to be replaced by "urn:uuid:".
             * The next state is {@link #LOOKING_FOR_END_OF_SUBJECT_OPENING_TAG}.
             */
            LOOKING_FOR_RDF_ID_OR_ABOUT_VALUE,
            /**
             * Looking for the end of the subject opening tag, which is either '>' or '/>'.
             * If it is '>', the next state is {@link #LOOKING_FOR_PROPERTY}.
             * If it is '/>', the next state is {@link #LOOKING_FOR_SUBJECT}.
             */
            LOOKING_FOR_END_OF_SUBJECT_OPENING_TAG,

            /**
             * Looking for a property opening tag '<TAG ' after the subject opening tag.
             * The property is identified when the next whitespace is found after the tag name.
             * A property opening tag starts with '<rdf:' or '<cim:' or '<iri:' where 'iri' may be any IRI.
             * The next state is {@link #LOOKING_FOR_PROPERTY_NAME}.
             */
            LOOKING_FOR_PROPERTY,
            IN_TEXT_CONTENT,
            IN_COMMENT
        }



        public CIMParserPoC() {
            ArrayList al = new ArrayList();
        }

        public void parse(Path filePath) throws Exception {
            try (var channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
                parse(channel);
            }
        }

        public void parse(FileChannel channel) throws Exception {
            final long fileSize = channel.size();
            try(final var is = new BufferedFileChannelInputStream.Builder()
                    .setFileChannel(channel)
                    .setOpenOptions(StandardOpenOption.READ)
                    .setBufferSize((fileSize < maxBufferSize) ? (int) fileSize : maxBufferSize)
                    .get()) {
                parse(is);
            }
        }

        public void parse(InputStream inputStream) throws Exception {
            byte b = skipWhitespace(inputStream);
            b = expectXMLDeclaration(b, inputStream);
            if(-1 < b) {
                b = skipWhitespace(inputStream);
                expectRDF_RDF(b, inputStream);
            }
        }

        private static void expectRDF_RDF(byte b, InputStream inputStream) throws IOException {
            if (matchesByteArray(b, inputStream, RDF_RDF)) {
                // TODO: readNamespaces()
                if (!skipUntil(inputStream, RIGHT_ANGLE_BRACKET)) {
                    throw new IOException("Expected 'xmlns' after 'rdf:RDF'");
                }
            } else {
                throw new IOException("Expected 'rdf:RDF' at the start of the document");
            }
        }

        private static byte expectXMLDeclaration(byte b, InputStream inputStream) throws IOException {
            if (matchesByteArray(b, inputStream, XML_DECL_START)) {
                if (!skipUntil(inputStream, XML_DECL_END)) {
                    throw new IOException("Expected '?>' to close XML declaration");
                }
            } else {
                throw new IOException("Expected '<?xml' for XML declaration");
            }
            return b;
        }

        private static boolean matchesByteArray(byte b, final InputStream inputStream, final byte[] match) throws IOException {
            if(b != match[0]) {
                return false; // Not a match
            }
            for (int i = 1; i < match.length; i++) {
                if ((b = (byte) inputStream.read()) != match[i]) {
                    return false; // Not a match
                }
            }
            return true;
        }

        private static boolean skipUntil(InputStream inputStream, byte[] match) throws IOException {
            byte b;
            int matchIndex = 0;
            while ((b = (byte) inputStream.read()) != -1) {
                if (b == match[matchIndex]) {
                    matchIndex++;
                    if (matchIndex == match.length) {
                        return true; // Match found
                    }
                } else {
                    matchIndex = 0; // Reset match index
                }
            }
            return false; // No match found
        }

        private byte skipWhitespace(InputStream inputStream) throws IOException {
            byte b;
            while ((b = (byte) inputStream.read()) != -1) {
                if (!isWhitespace(b)) {
                    return b;
                }
            }
            return b;
        }



        private static boolean isWhitespace(byte b) {
            return b == ' ' || b == '\t' || b == '\n' || b == '\r';
        }
//
//        public void parse(ByteBuffer buffer) throws IOException {
//            while (buffer.hasRemaining() || fillBufferStrategy.fillBuffer()) {
//                byte b = buffer.get();
//                if (b == OPEN_TAG) {
//                    StringBuilder tagName = new StringBuilder();
//                    while (buffer.hasRemaining() && (b = buffer.get()) != CLOSE_TAG) {
//                        tagName.append((char) b);
//                    }
//                    if (tagName.length() > 0) {
//                        String tag = tagName.toString().trim();
//                        tagMap.computeIfAbsent(tag.getBytes(charset), () -> tag);
//                    }
//                }
//            }
//        }
    }

    public class StreamRDFGraph implements StreamRDF {
        private final Graph graph;

        public StreamRDFGraph(Graph graph) {
            this.graph = graph;
        }

        @Override
        public void start() {

        }

        @Override
        public void triple(Triple triple) {
            this.graph.add(triple);
        }

        @Override
        public void quad(Quad quad) {
            throw new UnsupportedOperationException("Not supported.");
        }

        @Override
        public void base(String base) {
            this.graph.getPrefixMapping().setNsPrefix(XMLConstants.DEFAULT_NS_PREFIX, base);
        }

        @Override
        public void prefix(String prefix, String iri) {
            this.graph.getPrefixMapping().setNsPrefix(prefix, iri);
        }

        @Override
        public void finish() {

        }
    }

    @Test
    public void testTextParser() throws Exception {
        JenaSystem.init();
        final var xmlString = """
                <?xml version="1.0" encoding="utf-8"?>
                <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                            xmlns:dc="http://purl.org/dc/elements/1.1/">
                
                  <rdf:Description rdf:about="http://www.w3.org/TR/rdf-syntax-grammar">
                    <dc:title>RDF 1.1 XML Syntax</dc:title>
                    <dc:title xml:lang="en">RDF 1.1 XML Syntax</dc:title>
                    <dc:title xml:lang="en-US">RDF 1.1 XML Syntax</dc:title>
                  </rdf:Description>
                
                  <rdf:Description rdf:about="http://example.org/buecher/baum" xml:lang="de">
                    <dc:title>Der Baum</dc:title>
                    <dc:description>Das Buch ist außergewöhnlich</dc:description>
                    <dc:title xml:lang="en">The Tree</dc:title>
                  </rdf:Description>
                
                </rdf:RDF>
                """;
        final var is = new ByteArrayInputStream(xmlString.getBytes(StandardCharsets.UTF_8));
        final var graph = new GraphMem2Roaring(IndexingStrategy.LAZY);
        final var expectedGraph = new GraphMem2Roaring(IndexingStrategy.LAZY);
        final var parser = new CIMParser(is, new StreamRDFGraph(graph));

        final var stopWatch = StopWatch.createStarted();
        parser.parse();
        stopWatch.stop();
        // print number of triples parsed and the time taken
        System.out.println("Parsed triples: " + graph.size() + " in " + stopWatch);

        stopWatch.reset();
        stopWatch.start();
        RDFParser.create()
                .source(new ByteArrayInputStream(xmlString.getBytes(StandardCharsets.UTF_8)))
                .lang(org.apache.jena.riot.Lang.RDFXML)
                .parse(new StreamRDFGraph(expectedGraph));
        stopWatch.stop();

        // print number of triples parsed and the time taken
        System.out.println("Parsed expected triples: " + expectedGraph.size() + " in " + stopWatch);

        assertGraphsEqual(expectedGraph, graph);
    }

    @Test
    public void testCimXml() throws Exception {
        JenaSystem.init();
        final var xmlString = """
               <?xml version="1.0" encoding="utf-8"?>
               <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:cim="http://iec.ch/TC57/2013/CIM-schema-cim16#" xmlns:entsoe="http://entsoe.eu/CIM/SchemaExtension/3/1#" xmlns:md="http://iec.ch/TC57/61970-552/ModelDescription/1#">
                 <md:FullModel rdf:about="urn:uuid:4528b685-6ddc-471a-b8a2-8e77e6ec15f8">
                   <md:Model.created>2014-10-24T11:56:40</md:Model.created>
                   <md:Model.scenarioTime>2014-06-01T10:30:00</md:Model.scenarioTime>
                   <md:Model.version>2</md:Model.version>
                   <md:Model.DependentOn rdf:resource="urn:uuid:77b55f87-fc1e-4046-9599-6c6b4f991a86"/>
                   <md:Model.DependentOn rdf:resource="urn:uuid:2399cbd0-9a39-11e0-aa80-0800200c9a66"/>
                   <md:Model.description>CGMES Conformity Assessment: 'MicroGridTestConfiguration....BC (Assembled)Test Configuration. The model is owned by ENTSO-E and is provided by ENTSO-E “as it is”. To the fullest extent permitted by law, ENTSO-E shall not be liable for any damages of any kind arising out of the use of the model (including any of its subsequent modifications). ENTSO-E neither warrants, nor represents that the use of the model will not infringe the rights of third parties. Any use of the model shall  include a reference to ENTSO-E. ENTSO-E web site is the only official source of information related to the model.</md:Model.description>
                   <md:Model.modelingAuthoritySet>http://tennet.nl/CGMES/2.4.15</md:Model.modelingAuthoritySet>
                   <md:Model.profile>http://entsoe.eu/CIM/GeographicalLocation/2/1</md:Model.profile>
                 </md:FullModel>
                 <cim:CoordinateSystem rdf:ID="_50a38719-492c-4622-bba3-e99f0847be1c">
                   <cim:IdentifiedObject.name>WGS84</cim:IdentifiedObject.name>
                   <cim:CoordinateSystem.crsUrn>urn:ogc:def:crs:EPSG::4326</cim:CoordinateSystem.crsUrn>
                 </cim:CoordinateSystem>
                 <cim:Location rdf:ID="_37c3f6d0-1deb-48a8-92dd-18c80617073f">
                   <cim:Location.PowerSystemResources rdf:resource="#_c49942d6-8b01-4b01-b5e8-f1180f84906c"/>
                   <cim:Location.CoordinateSystem rdf:resource="#_50a38719-492c-4622-bba3-e99f0847be1c"/>
                 </cim:Location>
                 <cim:PositionPoint rdf:ID="_ed286b99-f37c-4677-8555-f8489d953cfa">
                   <cim:PositionPoint.sequenceNumber>1</cim:PositionPoint.sequenceNumber>
                   <cim:PositionPoint.Location rdf:resource="#_37c3f6d0-1deb-48a8-92dd-18c80617073f"/>
                   <cim:PositionPoint.xPosition>4.846580</cim:PositionPoint.xPosition>
                   <cim:PositionPoint.yPosition>52.404700</cim:PositionPoint.yPosition>
                 </cim:PositionPoint>
                </rdf:RDF>
                """;
        final var is = new ByteArrayInputStream(xmlString.getBytes(StandardCharsets.UTF_8));
        final var graph = new GraphMem2Roaring(IndexingStrategy.LAZY);
        final var expectedGraph = new GraphMem2Roaring(IndexingStrategy.LAZY);
        final var parser = new CIMParser(is, new StreamRDFGraph(graph));

        final var stopWatch = StopWatch.createStarted();
        parser.setBaseNamespace("urn:uuid");
        parser.doNotHandleCimUuidsWithMissingPrefix();
        parser.treatRdfIdStandardConformant();
        parser.parse();
        stopWatch.stop();
        // print number of triples parsed and the time taken
        System.out.println("Parsed triples: " + graph.size() + " in " + stopWatch);

        stopWatch.reset();
        stopWatch.start();
        RDFParser.create()
                .source(new ByteArrayInputStream(xmlString.getBytes(StandardCharsets.UTF_8)))
                .base("urn:uuid")
                .lang(org.apache.jena.riot.Lang.RDFXML)
                .checking(false)
                .parse(new StreamRDFGraph(expectedGraph));
        stopWatch.stop();

        // print number of triples parsed and the time taken
        System.out.println("Parsed expected triples: " + expectedGraph.size() + " in " + stopWatch);

        assertGraphsEqual(expectedGraph, graph);
    }

    @Test
    public void testRdfId() throws Exception {
        JenaSystem.init();
        final var xmlString = """
               <?xml version="1.0"?>
               <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                           xmlns:ex="http://example.org/stuff/1.0/"
                           xml:base="http://example.org/here/">
    
                 <rdf:Description rdf:ID="snack">
                   <ex:prop rdf:resource="fruit/apple"/>
                 </rdf:Description>
    
               </rdf:RDF>
                """;
        final var is = new ByteArrayInputStream(xmlString.getBytes(StandardCharsets.UTF_8));
        final var graph = new GraphMem2Roaring(IndexingStrategy.LAZY);
        final var expectedGraph = new GraphMem2Roaring(IndexingStrategy.LAZY);
        final var parser = new CIMParser(is, new StreamRDFGraph(graph));

        final var stopWatch = StopWatch.createStarted();
        parser.setBaseNamespace("urn:uuid");
        parser.doNotHandleCimUuidsWithMissingPrefix();
        parser.treatRdfIdStandardConformant();
        parser.parse();
        stopWatch.stop();
        // print number of triples parsed and the time taken
        System.out.println("Parsed triples: " + graph.size() + " in " + stopWatch);

        stopWatch.reset();
        stopWatch.start();
        RDFParser.create()
                .source(new ByteArrayInputStream(xmlString.getBytes(StandardCharsets.UTF_8)))
                .base("urn:uuid")
                .lang(org.apache.jena.riot.Lang.RDFXML)
                .checking(false)
                .parse(new StreamRDFGraph(expectedGraph));
        stopWatch.stop();

        // print number of triples parsed and the time taken
        System.out.println("Parsed expected triples: " + expectedGraph.size() + " in " + stopWatch);

        assertGraphsEqual(expectedGraph, graph);
    }

    @Test
    public void testFileParser() throws Exception {
        JenaSystem.init();
        final var filePath = java.nio.file.Paths.get(file);
        final var graph = new GraphMem2Roaring(IndexingStrategy.LAZY);
        final var expectedGraph = new GraphMem2Roaring(IndexingStrategy.LAZY);
        final var parser = new CIMParser(filePath, new StreamRDFGraph(graph));

        final var stopWatch = StopWatch.createStarted();
        parser.parse();
        stopWatch.stop();
        // print number of triples parsed and the time taken
        System.out.println("Parsed triples: " + graph.size() + " in " + stopWatch);

        stopWatch.reset();
        stopWatch.start();
        RDFParser.create()
                .source(filePath)
                .lang(org.apache.jena.riot.Lang.RDFXML)
                .parse(new StreamRDFGraph(expectedGraph));
        stopWatch.stop();

        // print number of triples parsed and the time taken
        System.out.println("Parsed expected triples: " + expectedGraph.size() + " in " + stopWatch);

        assertGraphsEqual(expectedGraph, graph);
    }

    public void assertGraphsEqual(Graph expected, Graph actual) {
        // check graph sizes
        assertEquals("Graphs are not equal: different sizes.",
                expected.size(), actual.size());
        // check that all triples in expected graph are in actual graph
        expected.find().forEachRemaining(expectedTriple -> {
            if(!actual.contains(expectedTriple)) {
                int i= 0;
            }
            assertTrue("Graphs are not equal: missing triple " + expectedTriple,
                    actual.contains(expectedTriple));
        });

        // check namespace mappings size
        assertEquals("Graphs are not equal: different number of namespaces.",
                expected.getPrefixMapping().numPrefixes(), actual.getPrefixMapping().numPrefixes());

        // check that all namespaces in expected graph are in actual graph
        expected.getPrefixMapping().getNsPrefixMap().forEach((prefix, uri) -> {
            assertTrue("Graphs are not equal: missing namespace " + prefix + " -> " + uri,
                    actual.getPrefixMapping().getNsPrefixMap().containsKey(prefix));
            assertEquals("Graphs are not equal: different URI for namespace " + prefix,
                    uri, actual.getPrefixMapping().getNsPrefixMap().get(prefix));
        });
    }



    @Test
    public void testQuery() throws Exception {
        final var stopWatch = StopWatch.createStarted();
        final var filePath = java.nio.file.Paths.get(file);
        final var subject = NodeFactory.createURI("cim:Location");
        final var predicate = NodeFactory.createURI("cim:Location.name");
        final var object = NodeFactory.createLiteralString("Test Location");
        final var graph = new GraphMem2Fast();
        graph.getPrefixMapping().setNsPrefix("cim", "http://iec.ch/TC57/2013/CIM-schema-cim16#");
        graph.add(subject, predicate, object);

        Query query = QueryFactory.create("""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX cims: <http://iec.ch/TC57/1999/rdf-schema-extensions-19990926#>
        PREFIX cim: <http://iec.ch/TC57/2013/CIM-schema-cim16#>
        SELECT ?s ?p ?o WHERE { ?s ?p ?o }
        """);
        QueryExec.graph(graph)
                .query(query)
                .select()
                .forEachRemaining(vars -> {
                    System.out.println(vars.toString());
                });


        // Create a CIMParserPoC instance
        //CIMXMLParser parser = new CIMXMLParser(filePath, 64 * 4096); // 256 KB
        //CIMXMLParser.ParseResult result = parser.parse(); ByteBuffer.wrap("sadasd".getBytes(charset));

        // Access parsed data
        //Map<String, String> namespaces = result.namespaces;
        //List<CIMXMLParser.Element> elements = result.rootElements;

        // Print summary
        //result.printSummary();
        stopWatch.stop();
        System.out.println(stopWatch);
    }
}

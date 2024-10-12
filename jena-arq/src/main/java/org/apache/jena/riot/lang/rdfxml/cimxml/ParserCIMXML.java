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

package org.apache.jena.riot.lang.rdfxml.cimxml;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.system.ErrorHandler;
import org.apache.jena.riot.system.ParserProfile;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.graph.NodeConst;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.vocabulary.RDF;

import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.util.*;

import static org.apache.jena.riot.SysRIOT.fmtMessage;

/* StAX - stream reader */
class ParserCIMXML {

    private final XMLStreamReader xmlSource;

    // Constants
    private static final String rdfNS = RDF.uri;

    private final ParserProfile parserProfile;
    private final ErrorHandler errorHandler;
    private final StreamRDF destination;

    private final String xmlBase;
    private final String xmlBaseSharp;

    ParserCIMXML(XMLStreamReader reader, String xmlBase, ParserProfile parserProfile, StreamRDF destination, Context context) {
        // Debug
        IndentedWriter out = IndentedWriter.stdout.clone();
        out.setFlushOnNewline(true);
        out.setUnitIndent(4);
        out.setLinePrefix("# ");
        // Debug

        this.xmlSource = reader;
        this.parserProfile = parserProfile;
        this.errorHandler = parserProfile.getErrorHandler();
        this.xmlBase = xmlBase;
        this.xmlBaseSharp = xmlBase + "#";
        this.destination = destination;
    }

    private static final QName rdfRDF = new QName(rdfNS, "RDF");
    private static final QName rdfID = new QName(rdfNS, "ID");
    private static final QName rdfAbout = new QName(rdfNS, "about");

    private static final QName rdfDatatype = new QName(rdfNS, "datatype");
    private static final QName rdfParseType = new QName(rdfNS, "parseType");
    private static final QName rdfResource = new QName(rdfNS, "resource");

    private static final QName xmlQNameLang = new QName(XMLConstants.XML_NS_URI, "lang");


    private record Frame(QName elementName, Node subject) {
    }

    private String addBaseToUriIfMissing(String uri) {
        switch (uri.charAt(0)) {
            case '_':
                return xmlBaseSharp + uri;
            case '#':
                return xmlBase + uri;
            default:
                return uri;
        }
    }

    private Node getOrCreateUriNode(final String uri) {
        return subjects.computeIfAbsent(
                addBaseToUriIfMissing(uri),
                u -> NodeFactory.createURI(u));
    }

    private final Map<String, Node> subjects = new HashMap<String, Node>();


    void parse() {
        final var typesOrPropertiesMap = new HashMap<QName, Node>();
        final var dataTypes = new HashMap<String, RDFDatatype>();
        final var frames = new LinkedList<Frame>();

        destination.start();
        try {
            readPrefixMappingFromRDFElement();

            while (xmlSource.hasNext()) {
                switch (xmlSource.next()) {
                    case XMLStreamReader.START_ELEMENT:
                        if (rdfNS.equals(xmlSource.getNamespaceURI())) {
                            //TODO: read RDF-Elements
                        } else {
                            final var elementName = xmlSource.getName();
                            final var typeOrNode = typesOrPropertiesMap.computeIfAbsent(
                                    xmlSource.getName(),
                                    qName -> NodeFactory.createURI(qName.getNamespaceURI() + qName.getLocalPart()));
                            var tripleCreated = false;
                            String lang = null;
                            RDFDatatype rdfDataType = null;
                            final var attributeCount = xmlSource.getAttributeCount();
                            for (int i = 0; i < attributeCount; i++) {
                                var name = xmlSource.getAttributeName(i);
                                if (rdfAbout.equals(name) || rdfID.equals(name)) {
                                    final var subject = getOrCreateUriNode(xmlSource.getAttributeValue(i));
                                    destination.triple(Triple.create(subject, NodeConst.nodeRDFType, typeOrNode));
                                    frames.add(new Frame(elementName, subject));
                                    tripleCreated = true;

                                } else if (rdfResource.equals(name)) {
                                    final var object = getOrCreateUriNode(xmlSource.getAttributeValue(i));
                                    destination.triple(Triple.create(frames.getLast().subject, typeOrNode, object));
                                    tripleCreated = true;

                                } else if (xmlQNameLang.equals(name)) {
                                    lang = xmlSource.getAttributeValue(i);

                                } else if (rdfDatatype.equals(name)) {
                                    final var dataType = xmlSource.getAttributeValue(i);
                                    rdfDataType = dataTypes.computeIfAbsent(
                                            dataType,
                                            dtype -> NodeFactory.getType(dtype));

                                } else if (rdfParseType.equals(name)) {
                                    final var parseType = xmlSource.getAttributeValue(i);
                                    switch (parseType) {
                                        case "Literal":
                                        case "Statements":
                                            break;
                                        default:
                                            throw new XMLStreamException("Illegal parseType: " + parseType);
                                    }
                                }
                            }
                            if (!tripleCreated) {
                                final var lex = xmlSource.getElementText();
                                if (lex == null) {
                                    throw new XMLStreamException("Illegal empty literal at " + xmlSource.getLocation());
                                }
                                final Node node;
                                if (rdfDataType != null) {
                                    node = this.parserProfile.createTypedLiteral(lex, rdfDataType,
                                            xmlSource.getLocation().getLineNumber(),
                                            xmlSource.getLocation().getColumnNumber());
                                } else if (lang != null) {
                                    node = this.parserProfile.createLangLiteral(lex, lang,
                                            xmlSource.getLocation().getLineNumber(),
                                            xmlSource.getLocation().getColumnNumber());

                                } else {
                                    node = this.parserProfile.createStringLiteral(lex,
                                            xmlSource.getLocation().getLineNumber(),
                                            xmlSource.getLocation().getColumnNumber());
                                }
                                destination.triple(Triple.create(frames.getLast().subject, typeOrNode, node));
                            }
                        }
                        break;
                    case XMLStreamConstants.END_ELEMENT:
                        if (!frames.isEmpty()) {
                            var elementName = xmlSource.getName();
                            var lastFrame = frames.getLast();
                            if (elementName.equals(lastFrame.elementName)) {
                                frames.removeLast();
                            }
                        }
                        break;
                }
            }
        } catch (XMLStreamException e) {
            handleXMLStreamException(e);
        }
        destination.finish();
    }

    private void readPrefixMappingFromRDFElement() throws XMLStreamException {
        boolean didNotFindRDF = true;
        while (xmlSource.hasNext() && didNotFindRDF) {
            if (xmlSource.next() == XMLStreamReader.START_ELEMENT) {
                if (rdfRDF.equals(xmlSource.getName())) {
                    didNotFindRDF = false;
                    final var namespaceCount = xmlSource.getNamespaceCount();
                    for (int i = 0; i < namespaceCount; i++) {
                        destination.prefix(xmlSource.getNamespacePrefix(i), xmlSource.getNamespaceURI(i));
                    }
                }
            }
        }
        if (didNotFindRDF) {
            throw new XMLStreamException("No RDF start element found");
        }
    }

    /**
     * XML parsing error.
     */
    private RiotException handleXMLStreamException(XMLStreamException ex) {
        String msg = xmlStreamExceptionMessage(ex);
        if (ex.getLocation() != null) {
            int line = ex.getLocation().getLineNumber();
            int col = ex.getLocation().getColumnNumber();
            errorHandler.fatal(msg, line, col);
        } else
            errorHandler.fatal(msg, -1, -1);
        // Should not happen.
        return new RiotException(ex.getMessage(), ex);
    }

    /**
     * Get the detail message from an XMLStreamException
     */
    private String xmlStreamExceptionMessage(XMLStreamException ex) {
        String msg = ex.getMessage();
        if (ex.getLocation() != null) {
            // XMLStreamException with a location is two lines, and has the line/col in the first line.
            // Deconstruct the XMLStreamException message to get the detail part.
            String marker = "\nMessage: ";
            int i = msg.indexOf(marker);
            if (i > 0) {
                msg = msg.substring(i + marker.length());
            }
        }
        return msg;
    }
}

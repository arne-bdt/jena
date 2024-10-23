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
import org.codehaus.stax2.XMLStreamReader2;

import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import java.util.*;
import java.util.function.Function;

/* StAX - stream reader */
class ParserCIMXML {

    private final XMLStreamReader2 xmlSource;
    private final ParserProfile parserProfile;
    private final ErrorHandler errorHandler;
    private final StreamRDF destination;

    private final String xmlBase;
    private final String xmlBaseSharp;

    private final Function<String, String> uriHandler;

    ParserCIMXML(XMLStreamReader2 reader, String xmlBase, ParserProfile parserProfile, StreamRDF destination, Context context) {
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
        this.uriHandler = context.isFalseOrUndef(ReaderCIMXML.READ_MRID_AS_UUID)
                ? this::addBaseToUriIfMissing
                : ParserCIMXML::readMRIDAsUUIDs;

    }

    // Constants
    private static final String uuidPrefix = "urn:uuid:";

    private static final String rdfNS = RDF.uri;
    private static final String modelDescriptionNS = "http://iec.ch/TC57/61970-552/ModelDescription/1#";

    private static final QName rdfRDF = new QName(rdfNS, "RDF");
    private static final QName rdfID = new QName(rdfNS, "ID");
    private static final QName rdfDescription = new QName(rdfNS, "Description");
    private static final QName rdfAbout = new QName(rdfNS, "about");
    private static final QName rdfParseType = new QName(rdfNS, "parseType");
    private static final QName rdfResource = new QName(rdfNS, "resource");

    private static final QName xmlQNameLang = new QName(XMLConstants.XML_NS_URI, "lang");

    private static final QName modelHeaderProfile = new QName(modelDescriptionNS, "Model.profile");

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

    private static String readMRIDAsUUIDs(String uri) {
        switch (uri.charAt(0)) {
            case '_':
                return uuidPrefix + getValidUUID(uri.substring(1));
            case '#':
                return uuidPrefix + getValidUUID(uri.substring(2));
            default:
                return uri;
        }
    }

    private static String getValidUUID(final String uuid) {
        switch (uuid.length()) {
            case 36:
                return uuid;
            case 32: {
                return new StringBuilder(36)
                        .append(uuid.substring(0, 7))
                        .append('_')
                        .append(uuid.substring(8, 11))
                        .append('_')
                        .append(uuid.substring(12, 15))
                        .append('_')
                        .append(uuid.substring(16, 19))
                        .append('_')
                        .append(uuid.substring(20, 31))
                        .toString();
                }
            default:
                throw new IllegalArgumentException("Given value for rdf:ID, rdf:about or rdf:resource is not a valid UUID: "
                        + uuid + " (maybe use READ_MRID_AS_UUID == false)");
        }
    }

    private Node getOrCreateUriNode(final String uri) {
        return subjects.computeIfAbsent(
                uriHandler.apply(uri),
                NodeFactory::createURI);
    }

    private final Map<String, Node> subjects = new HashMap<>();


    void parse() {
        final var typesOrPropertiesMap = new HashMap<QName, Node>();
        Node currentSubject = null;

        destination.start();
        try {
            readPrefixMappingFromRDFElement();

            while (xmlSource.hasNext()) {
                if (xmlSource.next() == XMLStreamConstants.START_ELEMENT) {
                    if (rdfNS.equals(xmlSource.getNamespaceURI())) {
                        throw new XMLStreamException("Discovered a tag with the namespace '" + rdfNS + "', which is not supported in CIMXML.",
                                xmlSource.getLocation());
                    } else {
                        final var typeOrNode = typesOrPropertiesMap.computeIfAbsent(
                                xmlSource.getName(),
                                qName -> NodeFactory.createURI(qName.getNamespaceURI() + qName.getLocalPart()));
                        var isLiteral = true;
                        String lang = null;
                        final var attributeCount = xmlSource.getAttributeCount();
                        for (int i = 0; i < attributeCount; i++) {
                            var name = xmlSource.getAttributeName(i);
                            if (rdfAbout.equals(name) || rdfID.equals(name)) {
                                currentSubject = getOrCreateUriNode(xmlSource.getAttributeValue(i));
                                destination.triple(Triple.create(currentSubject, NodeConst.nodeRDFType, typeOrNode));
                                isLiteral = false;

                            } else if (rdfResource.equals(name)) {
                                final var object = getOrCreateUriNode(xmlSource.getAttributeValue(i));
                                destination.triple(Triple.create(currentSubject, typeOrNode, object));
                                isLiteral = false;

                            } else if (rdfParseType.equals(name)) {
                                final var parseType = xmlSource.getAttributeValue(i);
                                if (!parseType.equals("Literal")) {
                                    throw new XMLStreamException("Illegal parseType: " + parseType,
                                            xmlSource.getLocation());
                                }

                            } else if (xmlQNameLang.equals(name)) {
                                lang = xmlSource.getAttributeValue(i);
                            } else {
                                throw new XMLStreamException("Unsupported attribute for CIMXML: " + name,
                                        xmlSource.getLocation());
                            }
                        }
                        if (isLiteral) {
                            final var lex = xmlSource.getElementText();
                            if (lex == null) {
                                throw new XMLStreamException("Empty literal is not allowed.",
                                        xmlSource.getLocation());
                            }
                            final Node node;
                            if (lang != null) {
                                node = this.parserProfile.createLangLiteral(lex, lang,
                                        xmlSource.getLocation().getLineNumber(),
                                        xmlSource.getLocation().getColumnNumber());

                            } else {
                                node = this.parserProfile.createStringLiteral(lex,
                                        xmlSource.getLocation().getLineNumber(),
                                        xmlSource.getLocation().getColumnNumber());
                            }
                            destination.triple(Triple.create(currentSubject, typeOrNode, node));
                        }
                    }
                }
            }
        } catch (XMLStreamException e) {
            handleXMLStreamException(e);
        } catch (Exception e) {
            handleXMLStreamException(new XMLStreamException(e.getMessage(), xmlSource.getLocation()));
        }
        destination.finish();
    }

    private void readPrefixMappingFromRDFElement() throws XMLStreamException {
        boolean didNotFindRDF = true;
        while (xmlSource.hasNext() && didNotFindRDF) {
            if (xmlSource.next() == XMLStreamConstants.START_ELEMENT
                    && rdfRDF.equals(xmlSource.getName())) {
                    didNotFindRDF = false;
                    final var namespaceCount = xmlSource.getNamespaceCount();
                    for (int i = 0; i < namespaceCount; i++) {
                        destination.prefix(xmlSource.getNamespacePrefix(i), xmlSource.getNamespaceURI(i));
                    }
            }
        }
        if (didNotFindRDF) {
            throw new XMLStreamException("No RDF start element found", xmlSource.getLocation());
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

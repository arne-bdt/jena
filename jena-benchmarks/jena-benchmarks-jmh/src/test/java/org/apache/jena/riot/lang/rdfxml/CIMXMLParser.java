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

package org.apache.jena.riot.lang.rdfxml;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.util.JenaXMLInput;
import org.apache.jena.vocabulary.RDF;

import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;

public class CIMXMLParser {

    private static final String rdfNS = RDF.uri;
    private static final String xmlNS = "http://www.w3.org/XML/1998/namespace";

    private static final QName rdfRDF = new QName(rdfNS, "RDF");
    private static final QName rdfDescription = new QName(rdfNS, "Description");
    private static final QName rdfID = new QName(rdfNS, "ID");
    private static final QName rdfNodeID = new QName(rdfNS, "nodeID");
    private static final QName rdfAbout = new QName(rdfNS, "about");
    private static final QName rdfType = new QName(rdfNS, "type");

    private static final QName rdfDatatype = new QName(rdfNS, "datatype");
    private static final QName rdfParseType = new QName(rdfNS, "parseType");
    private static final QName rdfResource = new QName(rdfNS, "resource");


    private static final QName xmlQNameBase = new QName(XMLConstants.XML_NS_URI, "base");
    private static final QName xmlQNameLang = new QName(XMLConstants.XML_NS_URI, "lang");


    private static final Node nodeRDFType = NodeFactory.createURI(rdfNS + "type");

    private record Frame(QName elementName, Node subject) {}

    public static void parse(final InputStream is, final String base, final Graph graph) throws Exception {
        final var baseSharp = base + "#";
        final var typesOrPropertiesMap = new HashMap<QName, Node>();
        final var subjects = new HashMap<String, Node>();
        final var dataTypes = new HashMap<String, RDFDatatype>();
        final var frames = new LinkedList<Frame>();

        var streamReader = JenaXMLInput.newXMLStreamReader(is);
        readPrefixMappingFromRDFElement(streamReader, graph.getPrefixMapping());
        while (streamReader.hasNext()) {
            switch (streamReader.next()) {
                case XMLStreamReader.START_ELEMENT:
                    if(rdfNS.equals(streamReader.getNamespaceURI())) {
                        //TODO: read RDF-Elements
                    } else {
                        final var elementName = streamReader.getName();
                        final var typeOrNode = typesOrPropertiesMap.computeIfAbsent(
                                streamReader.getName(),
                                qName -> NodeFactory.createURI(qName.getNamespaceURI() + qName.getLocalPart()));
                        var tripleCreated = false;
                        String lang = null;
                        RDFDatatype rdfDataType = null;
                        final var attributeCount = streamReader.getAttributeCount();
                        for(int i=0; i<attributeCount; i++) {
                            var name = streamReader.getAttributeName(i);
                            if(rdfAbout.equals(name) || rdfID.equals(name) ) {
                                var subjectURI = streamReader.getAttributeValue(i);
                                if(subjectURI.charAt(0) == '_') {
                                    subjectURI = baseSharp + subjectURI;//.substring(1);
                                }
                                final var subject = subjects.computeIfAbsent(
                                        subjectURI,
                                        uri -> NodeFactory.createURI(uri));
                                graph.add(subject, nodeRDFType, typeOrNode);
                                frames.add(new Frame(elementName, subject));
                                tripleCreated = true;

                            } else if(rdfResource.equals(name)) {
                                var resource = streamReader.getAttributeValue(i);
                                if(resource.charAt(0) == '#' && resource.charAt(1) == '_') {
                                    resource = base + resource;
                                }
                                final var object = subjects.computeIfAbsent(
                                        resource,
                                        uri -> NodeFactory.createURI(uri));
                                graph.add(frames.getLast().subject, typeOrNode, object);
                                tripleCreated = true;

                            } else if(xmlQNameLang.equals(name)) {
                                lang = streamReader.getAttributeValue(i);

                            } else if(rdfDatatype.equals(name)) {
                                final var dataType = streamReader.getAttributeValue(i);
                                rdfDataType = dataTypes.computeIfAbsent(
                                        dataType,
                                        dtype -> NodeFactory.getType(dtype));

                            } else if (rdfParseType.equals(name)) {
                                final var parseType = streamReader.getAttributeValue(i);
                                switch (parseType) {
                                    case "Literal":
                                    case "Statement":
                                        break;
                                    default:
                                        throw new XMLStreamException("Illegal parseType: " + parseType);
                                }
                            }
                        }
                        if(!tripleCreated) {
                            final var lex = streamReader.getElementText();
                            if(lex == null) {
                                throw new XMLStreamException("Illegal empty literal at " + streamReader.getLocation());
                            }
                            final Node node;
                            if(rdfDataType != null) {
                                node = NodeFactory.createLiteral(lex, lang, rdfDataType);
                            } else if (lang != null) {
                                node = NodeFactory.createLiteralLang(lex, lang);

                            } else {
                                node = NodeFactory.createLiteralString(lex);
                            }
                            graph.add(frames.getLast().subject, typeOrNode, node);
                        }
                    }
                    break;
                case XMLStreamConstants.END_ELEMENT:
                    if(!frames.isEmpty()) {
                        var elementName = streamReader.getName();
                        var lastFrame = frames.getLast();
                        if (elementName.equals(lastFrame.elementName)) {
                            frames.removeLast();
                        }
                    }
                    break;
            }
        }
    }

    private static void readPrefixMappingFromRDFElement(final XMLStreamReader streamReader, final PrefixMapping prefixMapping) throws XMLStreamException {
        boolean didNotFindRDF = true;
        while (streamReader.hasNext() && didNotFindRDF) {
            if(streamReader.next() == XMLStreamReader.START_ELEMENT) {
                if(rdfRDF.equals(streamReader.getName())) {
                    didNotFindRDF = false;
                    final var namespaceCount = streamReader.getNamespaceCount();
                    for(int i=0; i<namespaceCount; i++) {
                        prefixMapping.setNsPrefix(streamReader.getNamespacePrefix(i), streamReader.getNamespaceURI(i));
                    }
                }
            }
        }
        if(didNotFindRDF) {
            throw new XMLStreamException("No RDF start element found");
        }
    }

}

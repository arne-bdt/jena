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

package org.apache.jena.riot.lang.cimxml;

import org.apache.jena.atlas.web.ContentType;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.ReaderRIOT;
import org.apache.jena.riot.ReaderRIOTFactory;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.lang.rdfxml.SysRRX;
import org.apache.jena.riot.lang.rdfxml.rrx_stax_sr.ReaderRDFXML_StAX_SR;
import org.apache.jena.riot.system.*;
import org.apache.jena.sparql.util.Context;
import org.codehaus.stax2.XMLInputFactory2;
import org.codehaus.stax2.XMLStreamReader2;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.io.Reader;

/**
 * CIM/XML parser.
 * <p>
 * This implementation uses StAX via {@link XMLStreamReader}.
 *
 * @see <a href="https://webstore.iec.ch/en/publication/25939">https://webstore.iec.ch/en/publication/25939</a>
 */
public class ReaderCIMXML_StAX_SR
{
    public static XMLInputFactory2 createXMLInputFactory() {
        var factory = SysRRX.initAndConfigure(new com.fasterxml.aalto.stax.InputFactoryImpl());
        factory.configureForSpeed();
        return factory;
    }

    private static final XMLInputFactory2 xmlInputFactory = createXMLInputFactory();

    public static boolean TRACE = false;

    public final ErrorHandler errorHandler;

    public ReaderCIMXML_StAX_SR() {
        this(ErrorHandlerFactory.errorHandlerStd);
    }

    public ReaderCIMXML_StAX_SR(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    public void read(InputStream input, StreamCIMXML output) {
        try {
            var xmlStreamReader = (XMLStreamReader2) xmlInputFactory.createXMLStreamReader(input);
            parse(xmlStreamReader, output);
        } catch (XMLStreamException ex) {
            throw new RiotException("Failed to create the XMLEventReader", ex);
        }
    }

    public void read(Reader reader, StreamCIMXML output) {
        try {
            var xmlStreamReader = (XMLStreamReader2) xmlInputFactory.createXMLStreamReader(reader);
            parse(xmlStreamReader, output);
        } catch (XMLStreamException ex) {
            throw new RiotException("Failed to create the XMLEventReader", ex);
        }
    }

     private void parse(XMLStreamReader2 xmlStreamReader, StreamCIMXML destination) {
        var parser = new ParserCIMXML_StAX_SR(xmlStreamReader, destination, errorHandler);
        destination.start();
        try {
            parser.parse();
        } catch (RiotException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RiotException(ex);
        }
        finally {
            destination.finish();
        }
    }
}

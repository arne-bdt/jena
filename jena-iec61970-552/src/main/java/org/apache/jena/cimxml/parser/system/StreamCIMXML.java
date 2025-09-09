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

package org.apache.jena.cimxml.parser.system;

import org.apache.jena.cimxml.CimVersion;
import org.apache.jena.cimxml.CimXmlDocumentContext;
import org.apache.jena.cimxml.graph.CimModelHeader;
import org.apache.jena.cimxml.sparql.core.CimDatasetGraph;
import org.apache.jena.riot.system.StreamRDF;

/**
 * An extension of {@link StreamRDF} to provide access to the underlying {@link CimDatasetGraph}
 * and to store additional information about the CIMXML file being processed.
 */
public interface StreamCIMXML extends StreamRDF {

    /**
     *  Gets the underlying {@link CimDatasetGraph} that is being populated
     */
    CimDatasetGraph getCIMDatasetGraph();

    /**
     *  Gets the model header information as found in the CIMXML file
     */
    CimModelHeader getModelHeader();

    /**
     * Sets the preprocessing instruction version of IEC 61970-552
     * @param versionOfCIMXML the version string as found in the CIMXML file
     */
    void setVersionOfIEC61970_552(String versionOfCIMXML);

    /**
     * Gets the preprocessing instruction version of IEC 61970-552
     * @return the version string as found in the CIMXML file
     */
    String getVersionOfIEC61970_552();

    /**
     * Gets the CIMXMLVersion enum value for the given version string
     * @return the CIMXMLVersion enum value
     */
    CimVersion getVersionOfCIMXML();

    /**
     * Sets the CIMXMLVersion enum value for the given version string
     * @param versionOfCIMXML the CIMXMLVersion enum value
     */
    void setVersionOfCIMXML(CimVersion versionOfCIMXML);

    /**
     * Gets the current document context
     */
    CimXmlDocumentContext getCurrentContext();

    /**
     * Sets the current document context
     * @param context the current document context
     */
    void setCurrentContext(CimXmlDocumentContext context);
}

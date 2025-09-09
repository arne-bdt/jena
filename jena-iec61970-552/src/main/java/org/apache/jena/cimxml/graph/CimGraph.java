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

package org.apache.jena.cimxml.graph;

import org.apache.jena.cimxml.CimVersion;
import org.apache.jena.graph.Graph;

import java.util.Objects;

/**
 * A specialization of {@link Graph} that provides methods to determine the CIM version
 * of the graph based on its namespace prefixes.
 */
public interface CimGraph extends Graph {

    /**
     * Determines the CIM version of this graph based on its namespace prefixes.
     * If the graph does not use a CIM namespace, {@link CimVersion#NO_CIM} is returned.
     * @return The CIM version of the graph, or {@link CimVersion#NO_CIM} if no CIM namespace is used.
     */
    default CimVersion getCIMVersion() {
        return getCIMXMLVersion(this);
    }

    /**
     * Determines the CIM version of the given graph based on its namespace prefixes.
     * If the graph does not use a CIM namespace, {@link CimVersion#NO_CIM} is returned.
     * @param graph The graph to determine the CIM version for. Must not be null.
     * @return The CIM version of the graph, or {@link CimVersion#NO_CIM} if no CIM namespace is used.
     * @throws NullPointerException if the graph is null.
     */
    static CimVersion getCIMXMLVersion(Graph graph) {
        Objects.requireNonNull(graph, "graph");
        var cimURI = graph.getPrefixMapping().getNsPrefixURI("cim");
        if (cimURI == null)
            return CimVersion.NO_CIM;

        return CimVersion.fromCimNamespace(graph.getPrefixMapping().getNsPrefixURI("cim"));
    }
}

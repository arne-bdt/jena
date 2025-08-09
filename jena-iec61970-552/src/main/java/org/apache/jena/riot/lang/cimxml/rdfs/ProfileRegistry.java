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

package org.apache.jena.riot.lang.cimxml.rdfs;

import org.apache.jena.graph.Graph;

import java.util.Collection;

public interface ProfileRegistry {
    /**
     * Registers a profile graph in the registry.
     * This method is used to register a graph that contains RDFS CIM profiles.
     * The graph should contain the necessary RDFS CIM profile definitions
     * that can be used to validate or interpret CIMXML data.
     * <p>     *
     * @param graph
     */
    void registerProfile(Graph graph);



    Collection<Graph> getHeaderProfiles();
}

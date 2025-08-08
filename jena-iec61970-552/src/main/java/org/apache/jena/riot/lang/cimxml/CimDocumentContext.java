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

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

public enum CimDocumentContext {
    metadata,
    forwardDifferences,
    reverseDifferences,
    preconditions,
    body;

    public static Node getGraphName(CimDocumentContext context) {
        return switch (context) {
            case metadata -> GRAPH_METADATA;
            case forwardDifferences -> GRAPH_FORWARD_DIFFERENCES;
            case reverseDifferences -> GRAPH_REVERSE_DIFFERENCES;
            case preconditions -> GRAPH_PRECONDITIONS;
            case body -> null;
        };
    }

    public final static Node GRAPH_FORWARD_DIFFERENCES = NodeFactory.createURI("dm:forwardDifferences");
    public final static Node GRAPH_REVERSE_DIFFERENCES = NodeFactory.createURI("dm:reverseDifferences");
    public final static Node GRAPH_PRECONDITIONS = NodeFactory.createURI("dm:preconditions");
    public final static Node GRAPH_METADATA = NodeFactory.createURI("md:Model");
}

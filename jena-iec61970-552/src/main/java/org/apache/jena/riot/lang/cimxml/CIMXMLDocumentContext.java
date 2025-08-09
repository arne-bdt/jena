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
import org.apache.jena.riot.lang.cimxml.graph.ModelHeader;

public enum CIMXMLDocumentContext {
    fullModel,
    body,
    differenceModel,
    forwardDifferences,
    reverseDifferences,
    preconditions;


    public static Node getGraphName(CIMXMLDocumentContext context) {
        return switch (context) {
            case fullModel -> ModelHeader.TYPE_FULL_MODEL;
            case body -> null;
            case differenceModel -> ModelHeader.TYPE_DIFFERENCE_MODEL;
            case forwardDifferences -> GRAPH_FORWARD_DIFFERENCES;
            case reverseDifferences -> GRAPH_REVERSE_DIFFERENCES;
            case preconditions -> GRAPH_PRECONDITIONS;
        };
    }

    public final static String TAG_NAME_FORWARD_DIFFERENCES = "forwardDifferences";
    public final static String TAG_NAME_REVERSE_DIFFERENCES = "reverseDifferences";
    public final static String TAG_NAME_PRECONDITIONS = "preconditions";

    public final static Node GRAPH_FORWARD_DIFFERENCES = NodeFactory.createURI(ModelHeader.NS_DM + TAG_NAME_FORWARD_DIFFERENCES);
    public final static Node GRAPH_REVERSE_DIFFERENCES = NodeFactory.createURI(ModelHeader.NS_DM + TAG_NAME_REVERSE_DIFFERENCES);
    public final static Node GRAPH_PRECONDITIONS = NodeFactory.createURI(ModelHeader.NS_DM + TAG_NAME_PRECONDITIONS);
}

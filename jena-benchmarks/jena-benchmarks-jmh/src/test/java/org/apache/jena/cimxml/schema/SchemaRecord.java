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

package org.apache.jena.cimxml.schema;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;

/**
 * A record containing a schema URI, the graph of the schema, and a base URI for parsing.
 * @param schemaUri the URI of the schema, which is used to identify the schema
 * @param schemaGraph the graph of the schema
 * @param baseUriForParsing the base URI for parsing instances of graphs using this schema
 */
public record SchemaRecord(Node schemaUri, Graph schemaGraph, String baseUriForParsing) {
}

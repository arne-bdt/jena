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

/**
 * The base URI for RDF/XML instance graphs.
 * Unfortunately most RDF/XML files in the CGMES context do not contain a base URI.
 * This is a problem since a missing base URI is replaced by the file or uri the graph is read from. (Jena default behavior)
 * So we can not join graphs from different sources since the base URI is different.
 * This is not what we want. We want to have a default base URI for all graphs.
 */
public final class BaseURI {

    public static final String DEFAULT_BASE_URI = "xx:";

    private BaseURI() {
    }
}

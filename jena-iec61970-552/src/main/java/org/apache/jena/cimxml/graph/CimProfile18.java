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

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.graph.GraphWrapper;
import org.apache.jena.vocabulary.RDF;

import java.util.Set;
import java.util.stream.Collectors;

public class CimProfile18 extends CimProfile17 implements CimProfile {

    final static String DOCUMENT_HEADER_VERSION_IRI_START = "https://ap-voc.cim4.eu/DocumentHeader";

    public CimProfile18(Graph graph, boolean isHeaderProfile) {
        super(graph, isHeaderProfile);
    }

    public static boolean hasVersionIRIAndKeyword(Graph graph) {
        return graph.find(Node.ANY, PREDICATE_DCAT_KEYWORD, Node.ANY).hasNext()
                && graph.find(Node.ANY, PREDICATE_OWL_VERSION_IRI, Node.ANY).hasNext();
    }

    public static boolean isHeaderProfile(Graph graph) {
        if(!hasOntology(graph))
            return false;
        var ontology = getOntology(graph);

        // look for https://ap.cim4.eu/DocumentHeader# without # in version IRIs
        return graph.stream(ontology, PREDICATE_OWL_VERSION_IRI, Node.ANY)
                .anyMatch(t
                        -> t.getObject().isURI()
                        && t.getObject().getURI().startsWith(DOCUMENT_HEADER_VERSION_IRI_START));
    }
}

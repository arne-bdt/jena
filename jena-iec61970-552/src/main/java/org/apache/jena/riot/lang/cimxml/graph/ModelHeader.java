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

package org.apache.jena.riot.lang.cimxml.graph;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.lang.cimxml.CIMHeaderVocabulary;
import org.apache.jena.sparql.graph.GraphWrapper;
import org.apache.jena.vocabulary.RDF;

import java.util.stream.Stream;


public interface ModelHeader extends Graph {


    default boolean isFullModel() {
        return find(Node.ANY, RDF.type.asNode(), CIMHeaderVocabulary.TYPE_FULL_MODEL).hasNext();
    }

    default boolean isDifferenceModel() {
        return find(Node.ANY, RDF.type.asNode(), CIMHeaderVocabulary.TYPE_DIFFERENCE_MODEL).hasNext();
    }

    default Node getModel() {
        var iter = find(Node.ANY, RDF.type.asNode(), CIMHeaderVocabulary.TYPE_FULL_MODEL);
        if(iter.hasNext()) {
            return iter.next().getSubject();
        }
        iter = find(Node.ANY, RDF.type.asNode(), CIMHeaderVocabulary.TYPE_DIFFERENCE_MODEL);
        if(iter.hasNext()) {
            return iter.next().getSubject();
        }
        throw new IllegalStateException("Found neither FullModel nor DifferenceModel in the header graph.");
    }

    default Stream<Node> getProfiles() {
        return stream(getModel(), CIMHeaderVocabulary.PREDICATE_PROFILE, Node.ANY)
                .map(Triple::getObject);
    }

    default Stream<Node> getSupersedes() {
        return stream(getModel(), CIMHeaderVocabulary.PREDICATE_SUPERSEDES, Node.ANY)
                .map(Triple::getObject);
    }

    default Stream<Node> getDependentOn() {
        return stream(getModel(), CIMHeaderVocabulary.PREDICATE_DEPENDENT_ON, Node.ANY)
                .map(Triple::getObject);
    }

    static ModelHeader wrap(Graph graph) {
        if (graph == null) {
            return null;
        }
        if (graph instanceof ModelHeader modelHeader) {
            return modelHeader;
        }
        return new ModelHeaderGraphWrapper(graph);
    }

    class ModelHeaderGraphWrapper extends GraphWrapper implements ModelHeader {
        public ModelHeaderGraphWrapper(Graph graph) {
            super(graph);
        }
    }
}

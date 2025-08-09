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
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.graph.GraphWrapper;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.vocabulary.RDF;

public interface ModelHeader extends Graph {
    String NS_MD = "http://iec.ch/TC57/61970-552/ModelDescription/1#";
    String NS_DM = "http://iec.ch/TC57/61970-552/DifferenceModel/1#";
    String CLASSNAME_FULL_MODEL = "FullModel";
    String CLASSNAME_DIFFERENCE_MODEL = "DifferenceModel";

    Node PREDICATE_PROFILE = NodeFactory.createURI(NS_MD + "Model.profile");
    Node PREDICATE_SUPERSEDES = NodeFactory.createURI(NS_MD + "Model.Supersedes");
    Node TYPE_FULL_MODEL = NodeFactory.createURI(NS_MD + CLASSNAME_FULL_MODEL);
    Node TYPE_DIFFERENCE_MODEL = NodeFactory.createURI(NS_DM + CLASSNAME_DIFFERENCE_MODEL);

    default boolean isFullModel() {
        return find(Node.ANY, RDF.type.asNode(), TYPE_FULL_MODEL).hasNext();
    }

    default boolean isDifferenceModel() {
        return find(Node.ANY, RDF.type.asNode(), TYPE_DIFFERENCE_MODEL).hasNext();
    }

    default Node getModel() {
        var iter = find(Node.ANY, RDF.type.asNode(), TYPE_FULL_MODEL);
        if(iter.hasNext()) {
            return iter.next().getSubject();
        }
        iter = find(Node.ANY, RDF.type.asNode(), TYPE_DIFFERENCE_MODEL);
        if(iter.hasNext()) {
            return iter.next().getSubject();
        }
        throw new IllegalStateException("Found neither FullModel nor DifferenceModel in the header graph.");
    }
    default ExtendedIterator<Node> getProfiles(Node model) {
        return find(model, PREDICATE_PROFILE, Node.ANY)
                .mapWith(Triple::getObject);
    }
    default ExtendedIterator<Node> Supersedes(Node model) {
        return find(model, PREDICATE_SUPERSEDES, Node.ANY)
                .mapWith(Triple::getObject);
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

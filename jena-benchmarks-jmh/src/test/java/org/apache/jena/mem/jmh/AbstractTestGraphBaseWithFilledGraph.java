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

package org.apache.jena.mem.jmh;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;

public abstract class AbstractTestGraphBaseWithFilledGraph extends AbstractJmhTestGraphBase {
    /**
     * The graph under test. This is initialized in {@link #fillTriplesList()}.
     */
    protected Graph sut;

    @Setup(Level.Trial)
    public void fillTriplesList() throws Exception {
        super.fillTriplesList();
        this.sut = createGraph();
        // Add the same triples to the graph under test as new instances so that they are not reference equals.
        // This is important because the graph under test must not use reference equality as shortcut during the
        // benchmark.
        this.triples.forEach(t -> sut.add(Triple.create(cloneNode(t.getSubject()), cloneNode(t.getPredicate()), cloneNode(t.getObject()))));
    }

    public static Node cloneNode(Node node) {
        if(node.isLiteral()) {
            return NodeFactory.createLiteralByValue(node.getLiteralValue(), node.getLiteralLanguage(), node.getLiteralDatatype());
        }
        if(node.isURI()) {
            return NodeFactory.createURI(node.getURI());
        }
        if(node.isBlank()) {
            return NodeFactory.createBlankNode(node.getBlankNodeLabel());
        }
        throw new IllegalArgumentException("Only literals, URIs and blank nodes are supported");
    }
}
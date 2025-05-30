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

package org.apache.jena.shared;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.GraphMemFactory;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.WrappedGraph ;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.util.iterator.ExtendedIterator;

/**
 * Wraps a graph and randomizes the order of find results.
 */
public class RandomOrderGraph extends WrappedGraph {

	public static Graph createDefaultGraph() {
		return new RandomOrderGraph(GraphMemFactory.createDefaultGraph());
	}

	public static Model createDefaultModel() {
		return ModelFactory.createModelForGraph(createDefaultGraph());
	}

    final private int bufsz;
	/**
	 * @param base
	 */
	public RandomOrderGraph(int bufsz, Graph base) {
		super(base);
		this.bufsz = bufsz;
	}

	/**
	 * @param base
	 */
	public RandomOrderGraph(Graph base) {
		this(10,base);
	}

    @Override
    public ExtendedIterator<Triple> find(Triple triple) {
        return new RandomOrderIterator<>(bufsz, super.find(triple));
    }

    @Override
    public ExtendedIterator<Triple> find(Node s, Node p, Node o) {
        return new RandomOrderIterator<>(bufsz, super.find(s, p, o));
    }
}

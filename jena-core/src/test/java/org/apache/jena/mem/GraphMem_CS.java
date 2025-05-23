/*
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package org.apache.jena.mem;

import static org.apache.jena.testing_framework.GraphHelper.graphAdd;
import static org.apache.jena.testing_framework.GraphHelper.graphWith;
import static org.apache.jena.testing_framework.GraphHelper.memGraph;
import static org.apache.jena.testing_framework.GraphHelper.triple;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;

import org.junit.runner.RunWith;

import org.apache.jena.graph.*;
import org.apache.jena.shared.JenaException;
import org.apache.jena.testing_framework.AbstractGraphProducer;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.xenei.junit.contract.Contract.Inject;
import org.xenei.junit.contract.ContractImpl;
import org.xenei.junit.contract.ContractSuite;
import org.xenei.junit.contract.ContractTest;
import org.xenei.junit.contract.IProducer;

@RunWith(ContractSuite.class)
@ContractImpl(GraphMem.class)
@SuppressWarnings("deprecation")
public class GraphMem_CS {

	protected IProducer<Graph> graphProducer = new AbstractGraphProducer<Graph>() {

		@Override
		protected Graph createNewGraph() {
			return GraphMemFactory.createDefaultGraph();
		}

		@Override
		public Graph[] getDependsOn(Graph g) {
			return null;
		}

		@Override
		public Graph[] getNotDependsOn(Graph g) {
			return new Graph[] { memGraph() };
		}

	};

	@Inject
	public IProducer<Graph> getGraphProducer() {
		return graphProducer;
	}

	@ContractTest
	public void testContainsConcreteDoesntUseFind() {
		Graph g = new GraphMemWithoutFind();
		graphAdd(g, "x P y; a Q b");
		assertTrue(g.contains(triple("x P y")));
		assertTrue(g.contains(triple("a Q b")));
		assertFalse(g.contains(triple("a P y")));
		assertFalse(g.contains(triple("y R b")));
	}

	protected final class GraphMemWithoutFind extends GraphMem {
		@Override
		public ExtendedIterator<Triple> graphBaseFind(Triple t) {
			throw new JenaException("find is Not Allowed");
		}
	}

	@ContractTest
	public void testUnnecessaryMatches() {
		Node special = new Node_URI("eg:foo") {
			@Override
			public boolean matches(Node s) {
				fail("Matched called superfluously.");
				return true;
			}
		};
		Graph g = graphWith(graphProducer.newInstance(), "x p y");
		g.add(Triple.create(special, special, special));
		exhaust(g.find(special, Node.ANY, Node.ANY));
		exhaust(g.find(Node.ANY, special, Node.ANY));
		exhaust(g.find(Node.ANY, Node.ANY, special));

	}

	protected void exhaust(Iterator<?> it) {
		while (it.hasNext())
			it.next();
	}

}

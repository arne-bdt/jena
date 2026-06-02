/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *   SPDX-License-Identifier: Apache-2.0
 */
package org.apache.jena.mem.graph.helper;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.*;
import org.apache.jena.memvalue.GraphMemValue;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.GraphView;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.sparql.core.mem.DatasetGraphInMemoryMvccTxn;
import org.apache.jena.sparql.core.mem.GraphMemIndexedSetCowTxn;
import org.apache.jena.sparql.core.mem.GraphMemIndexedSetMvccTxn;
import org.apache.jena.sparql.core.mem.GraphMemIndexedSetTxn;
import org.apache.jena.system.Txn;

public class GraphTripleNodeHelperCurrent implements GraphTripleNodeHelper<Graph, Triple, Node> {

    @SuppressWarnings("deprecation")
    @Override
    public Graph createGraph(Context.GraphClass graphClass) {
        return switch (graphClass) {
            case GraphMemValue -> new GraphMemValue();
            case GraphMemFast -> new GraphMemFast();
            case GraphMemLegacy -> new GraphMemLegacy();
            case GraphMemIndexedSetEager -> new GraphMemIndexedSet(IndexingStrategy.EAGER);
            case GraphMemIndexedSetLazy -> new GraphMemIndexedSet(IndexingStrategy.LAZY);
            case GraphMemIndexedSetLazyParallel -> new GraphMemIndexedSet(IndexingStrategy.LAZY_PARALLEL);
            case GraphMemIndexedSetMinimal -> new GraphMemIndexedSet(IndexingStrategy.MINIMAL);
            case GraphMemIndexedSetManual -> new GraphMemIndexedSet(IndexingStrategy.MANUAL);
            case GraphMemRoaringEager -> new GraphMemRoaring(IndexingStrategy.EAGER);
            case GraphMemRoaringLazy -> new GraphMemRoaring(IndexingStrategy.LAZY);
            case GraphMemRoaringLazyParallel -> new GraphMemRoaring(IndexingStrategy.LAZY_PARALLEL);
            case GraphMemRoaringMinimal -> new GraphMemRoaring(IndexingStrategy.MINIMAL);
            case GraphMemRoaringManual -> new GraphMemRoaring(IndexingStrategy.MANUAL);
            case GraphMemIndexedSetTxnEager -> new GraphMemIndexedSetTxn(IndexingStrategy.EAGER);
            case GraphMemIndexedSetCowTxnEager -> new GraphMemIndexedSetCowTxn(IndexingStrategy.EAGER);
            case GraphMemIndexedSetMvccTxnEager -> new GraphMemIndexedSetMvccTxn(IndexingStrategy.EAGER);
            // The MVCC dataset is benchmarked through its default graph; the
            // GraphView keeps a reference to the (transactional) dataset, which
            // executeWrite recovers via GraphView.getDataset().
            case DatasetGraphInMemoryMvccTxnEager ->
                    new DatasetGraphInMemoryMvccTxn(IndexingStrategy.EAGER).getDefaultGraph();
        };
    }

    /**
     * {@inheritDoc}
     * <p>
     * The three standalone transactional graphs implement {@link Transactional}
     * directly. The MVCC dataset is exposed as the default {@link GraphView},
     * whose controlling {@link Transactional} is the dataset reachable through
     * {@link GraphView#getDataset()}. In both cases the bulk mutation is run in
     * a single write transaction; non-transactional graphs run it directly.
     */
    @Override
    public void executeWrite(Graph graph, Runnable action) {
        final Transactional txn = asTransactional(graph);
        if (txn != null) {
            Txn.executeWrite(txn, action);
        } else {
            action.run();
        }
    }

    /** @return the {@link Transactional} controlling the graph, or {@code null} if it is not transactional. */
    private static Transactional asTransactional(Graph graph) {
        if (graph instanceof Transactional t) {
            return t;
        }
        if (graph instanceof GraphView gv && gv.getDataset() instanceof Transactional t) {
            return t;
        }
        return null;
    }

    @Override
    public List<Triple> readTriples(String graphUri) {
        var list = new ArrayList<Triple>();
        @SuppressWarnings("deprecation")
        var g1 = new GraphMemValue() {
            @Override
            public void add(Triple t) {
                list.add(t);
            }
        };
        RDFDataMgr.read(g1, graphUri);
        return list;
    }

    @Override
    public List<Triple> cloneTriples(List<Triple> triples) {
        var list = new ArrayList<Triple>(triples.size());
        triples.forEach(triple -> list.add(cloneTriple(triple)));
        return list;
    }

    @Override
    public Triple cloneTriple(Triple triple) {
        return Triple.create(cloneNode(triple.getSubject()), cloneNode(triple.getPredicate()), cloneNode(triple.getObject()));
    }


    @Override
    public Node cloneNode(Node node) {
        if (node.isLiteral()) {
            return NodeFactory.createLiteral(node.getLiteralLexicalForm(), node.getLiteralLanguage(), node.getLiteralDatatype());
        }
        if (node.isURI()) {
            return NodeFactory.createURI(node.getURI());
        }
        if (node.isBlank()) {
            return NodeFactory.createBlankNode(node.getBlankNodeLabel());
        }
        throw new IllegalArgumentException("Only literals, URIs and blank nodes are supported");
    }
}

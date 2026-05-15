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

package org.apache.jena.sparql.core.mem;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.Test;

/**
 * Regression: if any per-graph {@code begin()} throws partway through the
 * eager pinned-topology walk in
 * {@code DatasetGraphInMemoryCowTxn.begin(READ_*)}, the per-graph
 * transactions that were already opened must be {@code end()}-ed before
 * the exception propagates — otherwise the caller's thread is left with
 * ghost per-graph state (active {@code ThreadLocal} slots) on the
 * graphs we managed to begin().
 * <p>
 * The test reflectively swaps one named graph in the topology with a
 * subclass that throws on {@code begin()}, then triggers
 * {@code dsg.begin(READ_PROMOTE)}. The default graph is always begun
 * first (before any named graph), so after the failed dataset
 * {@code begin} the default graph must not be in a transaction.
 * Pre-fix code leaves the default graph's per-thread slot active.
 */
public class TestDatasetGraphInMemoryCowTxnPartialBeginUnwind {

    private static Node uri(String x) { return NodeFactory.createURI("http://ex/" + x); }

    @Test
    public void thrownPerGraphBeginUnwindsAlreadyBegun() throws Exception {
        DatasetGraphInMemoryCowTxn dsg = new DatasetGraphInMemoryCowTxn();
        dsg.executeWrite(() -> {
            dsg.add(Quad.create(uri("g1"), uri("s"), uri("p"), uri("o")));
            dsg.add(Quad.create(uri("g2"), uri("s"), uri("p"), uri("o")));
        });

        GraphMemIndexedSetCowTxn poisoned = new ThrowOnBeginGraph();

        // Build a new topology that replaces g2 with the poisoned graph
        // and reflectively write it into the dataset's volatile slot.
        replaceNamedGraph(dsg, uri("g2"), poisoned);

        // begin(READ_PROMOTE) walks the topology in order: defaultGraph
        // first, then each named graph. When it reaches the poisoned g2
        // it throws — the catch path must have ended the defaultGraph
        // (and any named graph begun before g2) before re-throwing.
        assertThrows(RuntimeException.class, () -> dsg.begin(TxnType.READ_PROMOTE));

        // The default graph is always begun first; if the unwind worked,
        // it must no longer be in a transaction on this thread.
        Field defaultGraphField = DatasetGraphInMemoryCowTxn.class.getDeclaredField("defaultGraph");
        defaultGraphField.setAccessible(true);
        GraphMemIndexedSetCowTxn defGraph = (GraphMemIndexedSetCowTxn) defaultGraphField.get(dsg);
        assertFalse(defGraph.isInTransaction(),
                "defaultGraph must be end()-ed when the per-graph begin loop throws");

        // The dataset itself must also be in a clean state.
        assertFalse(dsg.isInTransaction());
    }

    /**
     * Replace the entry for {@code name} in the dataset's
     * {@code namedTopology} with {@code replacement}, preserving any
     * other entries.
     */
    private static void replaceNamedGraph(DatasetGraphInMemoryCowTxn dsg,
                                          Node name,
                                          GraphMemIndexedSetCowTxn replacement)
            throws Exception {
        Field topologyField = DatasetGraphInMemoryCowTxn.class.getDeclaredField("namedTopology");
        topologyField.setAccessible(true);
        Object oldTopology = topologyField.get(dsg);
        Method graphsAccessor = oldTopology.getClass().getDeclaredMethod("graphs");
        graphsAccessor.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Node, GraphMemIndexedSetCowTxn> oldMap =
                (Map<Node, GraphMemIndexedSetCowTxn>) graphsAccessor.invoke(oldTopology);
        Map<Node, GraphMemIndexedSetCowTxn> newMap = new LinkedHashMap<>(oldMap);
        newMap.put(name, replacement);

        Class<?> topologyClass = oldTopology.getClass();
        Constructor<?> ctor = topologyClass.getDeclaredConstructor(Map.class);
        ctor.setAccessible(true);
        Object newTopology = ctor.newInstance(Map.copyOf(newMap));
        topologyField.set(dsg, newTopology);
    }

    /**
     * A {@link GraphMemIndexedSetCowTxn} that throws on every
     * {@code begin}. Used purely as a fault-injection point: when the
     * dataset reaches us in the eager-per-graph begin loop, this throw
     * forces the unwind path.
     */
    private static final class ThrowOnBeginGraph extends GraphMemIndexedSetCowTxn {
        @Override
        public void begin(TxnType type) {
            throw new RuntimeException("injected begin() failure");
        }
    }
}

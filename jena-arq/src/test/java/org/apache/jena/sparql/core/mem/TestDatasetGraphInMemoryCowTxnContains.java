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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@code contains(g,s,p,o)} and {@code isEmpty()} overrides
 * in {@link DatasetGraphInMemoryCowTxn}.
 * <p>
 * The defaults from {@code DatasetGraphBase} build an iterator just to
 * ask {@code hasNext()}; the overrides short-circuit on per-graph
 * {@code CowStore.contains} / {@code isEmpty}. These tests pin the
 * functional contract: same answers as the iterator path, correct
 * routing across the four graph-specifier shapes (default, union,
 * wildcard, specific named), and snapshot stability when a writer
 * commits concurrently with a reader.
 */
public class TestDatasetGraphInMemoryCowTxnContains {

    private static Node uri(String s) { return NodeFactory.createURI("http://ex/" + s); }
    private static Quad nq(String g, String s, String p, String o) {
        return Quad.create(uri(g), uri(s), uri(p), uri(o));
    }
    private static Quad dq(String s, String p, String o) {
        return Quad.create(Quad.defaultGraphIRI, uri(s), uri(p), uri(o));
    }

    private static DatasetGraph populated() {
        DatasetGraph ds = new DatasetGraphInMemoryCowTxn();
        ds.begin(TxnType.WRITE);
        try {
            ds.add(dq("ds", "p", "do"));
            ds.add(nq("g1", "s1", "p1", "o1"));
            ds.add(nq("g2", "s2", "p2", "o2"));
            ds.commit();
        } finally {
            ds.end();
        }
        return ds;
    }

    // ----- contains: specific named graph -----------------------------------

    @Test public void contains_namedGraph_present() {
        DatasetGraph ds = populated();
        ds.begin(TxnType.READ);
        try {
            assertTrue(ds.contains(uri("g1"), uri("s1"), uri("p1"), uri("o1")));
        } finally { ds.end(); }
    }

    @Test public void contains_namedGraph_absent() {
        DatasetGraph ds = populated();
        ds.begin(TxnType.READ);
        try {
            assertFalse(ds.contains(uri("g1"), uri("nope"), uri("p"), uri("o")));
        } finally { ds.end(); }
    }

    @Test public void contains_namedGraph_doesNotExist() {
        DatasetGraph ds = populated();
        ds.begin(TxnType.READ);
        try {
            assertFalse(ds.contains(uri("missing"), uri("s"), uri("p"), uri("o")));
        } finally { ds.end(); }
    }

    @Test public void contains_namedGraph_wildcard() {
        DatasetGraph ds = populated();
        ds.begin(TxnType.READ);
        try {
            assertTrue(ds.contains(uri("g1"), Node.ANY, Node.ANY, Node.ANY));
        } finally { ds.end(); }
    }

    // ----- contains: default graph ------------------------------------------

    @Test public void contains_defaultGraph_present() {
        DatasetGraph ds = populated();
        ds.begin(TxnType.READ);
        try {
            assertTrue(ds.contains(Quad.defaultGraphIRI, uri("ds"), uri("p"), uri("do")));
            // defaultGraphNodeGenerated should also route to the default graph.
            assertTrue(ds.contains(Quad.defaultGraphNodeGenerated, uri("ds"), uri("p"), uri("do")));
        } finally { ds.end(); }
    }

    @Test public void contains_defaultGraph_absentSubjectInNamed() {
        DatasetGraph ds = populated();
        ds.begin(TxnType.READ);
        try {
            // s1 lives in g1, not the default.
            assertFalse(ds.contains(Quad.defaultGraphIRI, uri("s1"), uri("p1"), uri("o1")));
        } finally { ds.end(); }
    }

    // ----- contains: wildcard graph -----------------------------------------

    @Test public void contains_wildcard_findsInDefault() {
        DatasetGraph ds = populated();
        ds.begin(TxnType.READ);
        try {
            assertTrue(ds.contains(Node.ANY, uri("ds"), uri("p"), uri("do")));
            assertTrue(ds.contains(null,    uri("ds"), uri("p"), uri("do")));
        } finally { ds.end(); }
    }

    @Test public void contains_wildcard_findsInNamed() {
        DatasetGraph ds = populated();
        ds.begin(TxnType.READ);
        try {
            assertTrue(ds.contains(Node.ANY, uri("s2"), uri("p2"), uri("o2")));
            assertTrue(ds.contains(null,    uri("s2"), uri("p2"), uri("o2")));
        } finally { ds.end(); }
    }

    @Test public void contains_wildcard_absent() {
        DatasetGraph ds = populated();
        ds.begin(TxnType.READ);
        try {
            assertFalse(ds.contains(Node.ANY, uri("nope"), Node.ANY, Node.ANY));
            assertFalse(ds.contains(null,    uri("nope"), Node.ANY, Node.ANY));
        } finally { ds.end(); }
    }

    // ----- contains: union graph --------------------------------------------

    @Test public void contains_unionGraph_findsInNamedNotDefault() {
        DatasetGraph ds = populated();
        ds.begin(TxnType.READ);
        try {
            // Quads in named graphs are visible through the union.
            assertTrue(ds.contains(Quad.unionGraph, uri("s1"), uri("p1"), uri("o1")));
            // Triples in the default graph are NOT.
            assertFalse(ds.contains(Quad.unionGraph, uri("ds"), uri("p"), uri("do")));
        } finally { ds.end(); }
    }

    // ----- contains: snapshot isolation -------------------------------------

    @Test public void contains_isStableAcrossConcurrentCommit() throws Exception {
        DatasetGraph ds = new DatasetGraphInMemoryCowTxn();
        ds.begin(TxnType.WRITE);
        ds.add(nq("g1", "s", "p", "o"));
        ds.commit();
        ds.end();

        CountDownLatch readerBegun = new CountDownLatch(1);
        CountDownLatch writerDone  = new CountDownLatch(1);
        AtomicReference<Boolean> sawNew = new AtomicReference<>();
        AtomicReference<Throwable> err  = new AtomicReference<>();

        Thread reader = new Thread(() -> {
            try {
                ds.begin(TxnType.READ);
                readerBegun.countDown();
                writerDone.await(5, TimeUnit.SECONDS);
                // The writer added q2; the captured-view contains() must NOT see it.
                sawNew.set(ds.contains(uri("g1"), uri("after"), uri("p"), uri("o")));
                ds.end();
            } catch (Throwable t) { err.set(t); }
        });
        reader.start();
        assertTrue(readerBegun.await(5, TimeUnit.SECONDS));

        ds.begin(TxnType.WRITE);
        ds.add(nq("g1", "after", "p", "o"));
        ds.commit();
        ds.end();
        writerDone.countDown();

        reader.join(5_000);
        if (err.get() != null) throw new AssertionError(err.get());
        assertEquals(Boolean.FALSE, sawNew.get(),
                "contains() inside a READ transaction must observe the captured snapshot, not a later commit");

        // A fresh reader after the writer must see the new quad.
        ds.begin(TxnType.READ);
        try {
            assertTrue(ds.contains(uri("g1"), uri("after"), uri("p"), uri("o")));
        } finally { ds.end(); }
    }

    @Test public void contains_findsOwnWritesInsideWriteTxn() {
        DatasetGraph ds = populated();
        ds.begin(TxnType.WRITE);
        try {
            ds.add(nq("g1", "self", "p", "o"));
            assertTrue(ds.contains(uri("g1"),  uri("self"), uri("p"), uri("o")));
            assertTrue(ds.contains(Node.ANY,   uri("self"), uri("p"), uri("o")));
            assertTrue(ds.contains(Quad.unionGraph, uri("self"), uri("p"), uri("o")));
            ds.commit();
        } finally { ds.end(); }
    }

    @Test public void contains_outsideTxnAutoWraps() {
        DatasetGraph ds = populated();
        // No surrounding transaction; the override's access() helper auto-wraps.
        assertTrue(ds.contains(uri("g1"), uri("s1"), uri("p1"), uri("o1")));
        assertFalse(ds.contains(uri("g1"), uri("nope"), Node.ANY, Node.ANY));
    }

    // ----- isEmpty ----------------------------------------------------------

    @Test public void isEmpty_brandNewDataset() {
        DatasetGraph ds = new DatasetGraphInMemoryCowTxn();
        ds.begin(TxnType.READ);
        try {
            assertTrue(ds.isEmpty());
        } finally { ds.end(); }
    }

    @Test public void isEmpty_afterDefaultGraphWrite() {
        DatasetGraph ds = new DatasetGraphInMemoryCowTxn();
        ds.begin(TxnType.WRITE);
        ds.add(dq("s", "p", "o"));
        ds.commit();
        ds.end();
        ds.begin(TxnType.READ);
        try {
            assertFalse(ds.isEmpty());
        } finally { ds.end(); }
    }

    @Test public void isEmpty_afterNamedGraphWrite() {
        DatasetGraph ds = new DatasetGraphInMemoryCowTxn();
        ds.begin(TxnType.WRITE);
        ds.add(nq("g1", "s", "p", "o"));
        ds.commit();
        ds.end();
        ds.begin(TxnType.READ);
        try {
            assertFalse(ds.isEmpty());
        } finally { ds.end(); }
    }

    @Test public void isEmpty_afterDeleteOfAllContent() {
        DatasetGraph ds = new DatasetGraphInMemoryCowTxn();
        ds.begin(TxnType.WRITE);
        ds.add(dq("s", "p", "o"));
        ds.add(nq("g1", "s", "p", "o"));
        ds.commit();
        ds.end();

        ds.begin(TxnType.WRITE);
        ds.delete(dq("s", "p", "o"));
        ds.delete(nq("g1", "s", "p", "o"));
        ds.commit();
        ds.end();

        ds.begin(TxnType.READ);
        try {
            assertTrue(ds.isEmpty(),
                    "after deleting every quad the dataset must report empty");
        } finally { ds.end(); }
    }

    @Test public void isEmpty_outsideTxnAutoWraps() {
        DatasetGraph ds = populated();
        assertFalse(ds.isEmpty());
        DatasetGraph empty = new DatasetGraphInMemoryCowTxn();
        assertTrue(empty.isEmpty());
    }
}

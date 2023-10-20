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

package org.apache.jena.sparql.core.mem2;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.shared.Lock;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.util.Context;

import java.util.Iterator;
import java.util.stream.Stream;

public class DatasetGraphMem2Fast implements DatasetGraph {
    @Override
    public Graph getDefaultGraph() {
        return null;
    }

    @Override
    public Graph getGraph(Node graphNode) {
        return null;
    }

    @Override
    public Graph getUnionGraph() {
        return null;
    }

    @Override
    public boolean containsGraph(Node graphNode) {
        return false;
    }

    @Override
    public void addGraph(Node graphName, Graph graph) {

    }

    @Override
    public void removeGraph(Node graphName) {

    }

    @Override
    public Iterator<Node> listGraphNodes() {
        return null;
    }

    @Override
    public void add(Quad quad) {

    }

    @Override
    public void delete(Quad quad) {

    }

    @Override
    public void add(Node g, Node s, Node p, Node o) {

    }

    @Override
    public void delete(Node g, Node s, Node p, Node o) {

    }

    @Override
    public void deleteAny(Node g, Node s, Node p, Node o) {

    }

    @Override
    public Iterator<Quad> find(Quad quad) {
        return null;
    }

    @Override
    public Iterator<Quad> find(Node g, Node s, Node p, Node o) {
        return null;
    }

    @Override
    public Iterator<Quad> findNG(Node g, Node s, Node p, Node o) {
        return null;
    }

    @Override
    public Stream<Quad> stream(Node g, Node s, Node p, Node o) {
        return DatasetGraph.super.stream(g, s, p, o);
    }

    @Override
    public boolean contains(Node g, Node s, Node p, Node o) {
        return false;
    }

    @Override
    public boolean contains(Quad quad) {
        return false;
    }

    @Override
    public void clear() {

    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public Lock getLock() {
        return null;
    }

    @Override
    public Context getContext() {
        return null;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public PrefixMap prefixes() {
        return null;
    }

    @Override
    public boolean supportsTransactions() {
        return false;
    }

    @Override
    public boolean supportsTransactionAbort() {
        return true;
    }

    @Override
    public void begin(TxnType type) {

    }

    @Override
    public boolean promote(Promote mode) {
        return false;
    }

    @Override
    public void commit() {

    }

    @Override
    public void abort() {

    }

    @Override
    public void end() {

    }

    @Override
    public ReadWrite transactionMode() {
        return null;
    }

    @Override
    public TxnType transactionType() {
        return null;
    }

    @Override
    public boolean isInTransaction() {
        return false;
    }
}

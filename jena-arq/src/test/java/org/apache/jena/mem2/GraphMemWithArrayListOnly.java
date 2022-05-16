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

package org.apache.jena.mem2;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphWithPerform;
import org.apache.jena.mem.GraphMemBase;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;

import java.util.ArrayList;
import java.util.List;

public class GraphMemWithArrayListOnly extends GraphMemBase implements GraphWithPerform {

    public List<Triple> triples = new ArrayList<>();

    public GraphMemWithArrayListOnly() {
        super();
    }

    /**
     * Sub-classes over-ride this method to release any resources they no
     * longer need once fully closed.
     */
    @Override
    protected void destroy() {
        this.triples.clear();
        this.triples = null;
    }

    /**
     * Add a triple to the triple store. The default implementation throws an
     * AddDeniedException; subclasses must override if they want to be able to
     * add triples.
     *
     * @param t triple to add
     */
    @Override
    public void performAdd(Triple t) {
        this.triples.add(t);
    }

    /**
     * Returns an iterator over all Triples in the graph.
     * Equivalent to {@code find(Node.ANY, Node.ANY, Node.ANY)}
     *
     * @return an iterator of all triples in this graph
     */
    @Override
    public ExtendedIterator<Triple> find() {
        return WrappedIterator.create(this.triples.iterator());
    }

    /**
     * Remove a triple from the triple store. The default implementation throws
     * a DeleteDeniedException; subclasses must override if they want to be able
     * to remove triples.
     *
     * @param t triple to delete
     */
    @Override
    public void performDelete(Triple t) {
       throw new NotImplementedException();
    }

    /**
     * Remove all the statements from this graph.
     */
    @Override
    public void clear() {
        super.clear(); /* deletes all triples --> could be done better but later*/
        this.triples.clear();
    }

    /**
     * Answer true if the graph contains any triple matching <code>t</code>.
     * The default implementation uses <code>find</code> and checks to see
     * if the iterator is non-empty.
     *
     * @param triple triple which may be contained
     */
    @Override
    protected boolean graphBaseContains(Triple triple) {
        throw new NotImplementedException();
    }

    /**
     * Answer the number of triples in this graph. Default implementation counts its
     * way through the results of a findAll. Subclasses must override if they want
     * size() to be efficient.
     */
    @Override
    protected int graphBaseSize() {
        return this.triples.size();
        //return super.graphBaseSize(); /*reads all triples and counts them --> possibly override*/
    }

    @Override
    public ExtendedIterator<Triple> graphBaseFind(Triple triplePattern) {
        throw new NotImplementedException();
    }
}
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

import org.apache.jena.graph.Capabilities;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.AllCapabilities;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.mem2.GraphMem2Fast;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

public class FastDeltaGraph extends GraphBase {

    private final Graph base;
    private final Graph additions;
    private final Set<Triple> deletions;

    public FastDeltaGraph(Graph base) {
        super();
        if (base == null)
            throw new IllegalArgumentException("base graph must not be null");
        if (base.getCapabilities().handlesLiteralTyping())
            throw new IllegalArgumentException("base graph must not handle literal typing");
        if (!base.getCapabilities().sizeAccurate())
            throw new IllegalArgumentException("base graph must be size accurate");
        this.base = base;
        this.additions = new GraphMem2Fast();
        this.deletions = new HashSet<>();
    }

    public FastDeltaGraph(Graph newBase, FastDeltaGraph deltaGraphToRebase) {
        super();
        if (newBase == null)
            throw new IllegalArgumentException("base graph must not be null");
        if (newBase.getCapabilities().handlesLiteralTyping())
            throw new IllegalArgumentException("base graph must not handle literal typing");
        if (!newBase.getCapabilities().sizeAccurate())
            throw new IllegalArgumentException("base graph must be size accurate");
        this.base = newBase;
        this.additions = deltaGraphToRebase.additions;
        this.deletions = deltaGraphToRebase.deletions;
    }

    public Iterator<Triple> getAdditions() {
        return additions.find();
    }

    public Iterator<Triple> getDeletions() {
        return deletions.iterator();
    }

    public boolean hasChanges() {
        return !additions.isEmpty() || !deletions.isEmpty();
    }

    public Graph getBase() {
        return base;
    }

    @Override
    public void performAdd(Triple t) {
        if (!base.contains(t))
            additions.add(t);
        deletions.remove(t);
    }

    @Override
    public void performDelete(Triple t) {
        additions.delete(t);
        if (base.contains(t))
            deletions.add(t);
    }

    @Override
    public boolean dependsOn(Graph other) {
        return base.equals(other) || base.dependsOn(other);
    }

    @Override
    protected boolean graphBaseContains(Triple t) {
        if (t.isConcrete()) {
            if(base.contains(t)) {
                return !deletions.contains(t);
            }
            return additions.contains(t);
        } else {
            return graphBaseFind(t).hasNext();
        }
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple triplePattern) {
        return base.find(triplePattern)
                .filterDrop(deletions::contains)
                .andThen(additions.find(triplePattern));
    }

    @Override
    public ExtendedIterator<Triple> find() {
        return base.find()
                .filterDrop(deletions::contains)
                .andThen(additions.find());
    }

    @Override
    public Stream<Triple> stream() {
        return Stream.concat(
                base.stream().filter(t -> !deletions.contains(t)),
                additions.stream());
    }

    @Override
    public Stream<Triple> stream(Node s, Node p, Node o) {
        return Stream.concat(
                base.stream(s, p, o).filter(t -> !deletions.contains(t)),
                additions.stream(s, p, o));
    }

    @Override
    public void close() {
        super.close();
        base.close();
        additions.close();
        deletions.clear();
    }

    @Override
    public int graphBaseSize() {
        return base.size() + additions.size() - deletions.size();
    }

    @Override
    public Capabilities getCapabilities() {
        return AllCapabilities.updateAllowed;
    }
}

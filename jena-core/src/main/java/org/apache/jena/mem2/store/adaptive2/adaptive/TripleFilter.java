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
package org.apache.jena.mem2.store.adaptive2.adaptive;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

import java.util.function.Predicate;

/**
 * A class that encapsulates a filter on fields on a triple.
 * <p>
 * The filter is a predicate that takes a triple and returns true if it passes
 * the filter and false otherwise.
 * </p>
 */
public class TripleFilter {

    public static final TripleFilter EMPTY_FILTER = new TripleFilter(null);

    private Predicate<Triple> filter = null;

    private final Triple tripleMatch;

    public TripleFilter(Triple tripleMatch) {
        this.tripleMatch = tripleMatch;
    }

    public boolean hasFilter() {
        return null != filter;
    }

    public Predicate<Triple> getFilter()
    {
        return filter;
    }

    public TripleFilter filterOnSubject() {
        final Node subj = tripleMatch.getSubject();
        if(subj.isConcrete()) {
            if(filter == null) {
                filter = t -> subj.equals(t.getSubject());
            } else {
                filter = filter.and(t -> subj.equals(t.getSubject()));
            }
        }
        return this;
    }

    public TripleFilter filterOnPredicate() {
        final Node pred = tripleMatch.getPredicate();
        if(pred.isConcrete()) {
            if(filter == null) {
                filter = t -> pred.equals(t.getPredicate());
            } else {
                filter = filter.and(t -> pred.equals(t.getPredicate()));
            }
        }
        return this;
    }

    public TripleFilter filterOnObject() {
        final Node obj = tripleMatch.getObject();
        if(tripleMatch.getObject().isConcrete()) {
            if(filter == null) {
                filter = t -> obj.equals(t.getObject());
            } else {
                filter = filter.and(t -> obj.equals(t.getObject()));
            }
        }
        return this;
    }
}

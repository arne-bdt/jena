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

package org.apache.jena.mem2.store.adaptive2.adaptive.set;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.store.adaptive2.adaptive.QueryableTripleSet;
import org.apache.jena.mem2.store.adaptive2.adaptive.TripleFilter;
import org.apache.jena.mem2.store.adaptive2.adaptive.base.AdaptiveTripleListSetBase;

public class TripleListSet__S extends AdaptiveTripleListSetBase {

    @Override
    protected TripleFilter getMatchFilter(Triple tripleMatch) {
        return new TripleFilter(tripleMatch).filterOnSubject();
    }

    @Override
    public Node getIndexingNode() {
        return this.get(0).getObject();
    }

    @Override
    protected QueryableTripleSet createSetForTransition() {
        return new IndexedSet__S(this.getIndexingNode(), this.size());
    }
}

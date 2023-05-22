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

package org.apache.jena.mem2.specialized;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

public interface TripleSetWithIndexingNodeSG {

    boolean isEmpty();

    ExtendedIterator<Triple> iterator();

    Stream<Triple> stream();

    boolean contains(Triple tripeMatch);

    Node getIndexingNode();

    void addUnchecked(Triple t);

    boolean areOperationsWithHashCodesSupported();

    boolean add(Triple t);

    boolean add(Triple t, int hashCode);

    void addUnchecked(Triple t, int hashCode);

    boolean remove(Triple t);

    boolean remove(Triple t, int hashCode);

    void removeUnchecked(Triple t);

    void removeUnchecked(Triple t, int hashCode);
    int size();
}

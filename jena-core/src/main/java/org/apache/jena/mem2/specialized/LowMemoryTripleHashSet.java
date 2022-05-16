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

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.generic.LowMemoryHashSet;
import org.apache.jena.mem2.helper.TripleEqualsOrMatches;

import java.util.function.Predicate;

public class LowMemoryTripleHashSet extends LowMemoryHashSet<Triple> {

    public LowMemoryTripleHashSet() {
        super();
    }

    public LowMemoryTripleHashSet(int initialCapacity) {
        super(initialCapacity);
    }

    @Override
    protected int getHashCode(Triple value) {
        return (value.getSubject().getIndexingValue().hashCode() >> 1)
                ^ value.getPredicate().getIndexingValue().hashCode()
                ^ (value.getObject().getIndexingValue().hashCode() << 1);
    }

    @Override
    protected Predicate<Object> getContainsPredicate(Triple value) {
        if(TripleEqualsOrMatches.isEqualsForObjectOk(value.getObject())) {
            return t -> value.equals(t);
        }
        return t -> value.matches((Triple) t);
    }
}

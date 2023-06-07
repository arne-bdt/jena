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
package org.apache.jena.mem2.store.legacy;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem2.store.legacy.collection.HashCommonSet;
import java.util.function.Predicate;

public class HashedTripleBunch extends HashCommonSet<Triple> implements TripleBunch {
    protected HashedTripleBunch(final TripleBunch b) {
        super( nextSize( (int) (b.size() / loadFactor) ) );
        b.keySpliterator().forEachRemaining(t -> this.put(t));
    }

    @Override
    protected Triple[] newKeyArray(int size) {
        return new Triple[size];
    }

    @Override
    public boolean containsBySameValueAs(Triple t, Predicate<Triple> predicate) {
        final int hash = t.hashCode();
        int index = initialIndexFor( hash );
        while (true)
        {
            final Triple current = keys[index];
            if (current == null) return false;
            if (predicate.test( current )) return true;
            if (--index < 0) index += keys.length;
        }
    }

    @Override
    public boolean isHashed() {
        return true;
    }
}

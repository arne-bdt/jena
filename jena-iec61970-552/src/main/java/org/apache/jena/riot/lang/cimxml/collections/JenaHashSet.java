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

package org.apache.jena.riot.lang.cimxml.collections;

import org.apache.jena.mem2.collection.FastHashSet;

public class JenaHashSet<E> extends FastHashSet<E> {

    public JenaHashSet(int initialSize) {
        super(initialSize);
    }

    public JenaHashSet() {
        super();
    }

    public E getMatchingKey(E key) {
        final var pIndex = findPosition(key, key.hashCode());
        if (pIndex < 0) {
            return null;
        } else {
            return keys[positions[pIndex]];
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected E[] newKeysArray(int size) {
        return (E[]) new Object[size];
    }

}
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
package org.apache.jena.mem.txn.store;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem.spliterator.ArraySpliterator;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * An ArrayBunch implements TripleBunch with a linear search of a short-ish
 * array of Triples. The array grows by factor 2.
 */
public abstract class FastArrayBunchPersistableImmutable extends FastArrayBunchPersistable {

    private final FastArrayBunchPersistable mutableParent;

    /**
     * Copy constructor.
     * The new bunch will contain all the same triples of the bunch to copy.
     * But it will reserve only the space needed to contain them. Growing is still possible.
     *
     * @param bunchToCopy
     */
    protected FastArrayBunchPersistableImmutable(final FastArrayBunchPersistable bunchToCopy) {
        super(bunchToCopy);
        this.mutableParent = bunchToCopy;
    }

    @Override
    public boolean isImmutable() {
        return true;
    }

    @Override
    public FastArrayBunchPersistable getMutableParentBunch() {
        return mutableParent;
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("This bunch is read-only");
    }

    @Override
    public boolean tryAdd(final Triple t) {
        throw new UnsupportedOperationException("This bunch is read-only");

    }

    @Override
    public void addUnchecked(final Triple t) {
        throw new UnsupportedOperationException("This bunch is read-only");

    }

    @Override
    public boolean tryRemove(final Triple t) {
        throw new UnsupportedOperationException("This bunch is read-only");

    }

    @Override
    public void removeUnchecked(final Triple t) {
        throw new UnsupportedOperationException("This bunch is read-only");

    }
}

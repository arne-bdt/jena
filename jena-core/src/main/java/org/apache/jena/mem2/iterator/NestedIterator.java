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

package org.apache.jena.mem2.iterator;

import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Function;

public class NestedIterator<E, I> extends NiceIterator<E> {

    final Iterator<I> parentIterator;
    final Function<I, ExtendedIterator<E>> mapper;
    ExtendedIterator<E> currentIterator;

    private boolean hasNext = false;

    public NestedIterator(Iterator<I> parentIterator, Function<I, ExtendedIterator<E>> mapper) {
        this.parentIterator = parentIterator;
        this.mapper = mapper;
        this.currentIterator = parentIterator.hasNext()
                ? mapper.apply(parentIterator.next())
                : NiceIterator.emptyIterator();
    }

    @Override
    public boolean hasNext() {
        if (this.currentIterator.hasNext()) {
            return hasNext = true;
        }
        while (this.parentIterator.hasNext()) {
            this.currentIterator = this.mapper.apply(this.parentIterator.next());
            if (this.currentIterator.hasNext()) {
                return hasNext = true;
            }
        }
        return hasNext = false;
    }

    @Override
    public E next() {
        if (hasNext || this.currentIterator.hasNext()) {
            hasNext = false;
            return this.currentIterator.next();
        }
        while (this.parentIterator.hasNext()) {
            this.currentIterator = this.mapper.apply(this.parentIterator.next());
            if (this.currentIterator.hasNext()) {
                return this.currentIterator.next();
            }
        }
        throw new NoSuchElementException();
    }

    @Override
    public void forEachRemaining(Consumer<? super E> action) {
        if (this.currentIterator != null && this.currentIterator.hasNext()) {
            this.currentIterator.forEachRemaining(action);
        }
        this.parentIterator.forEachRemaining(i -> this.mapper.apply(i).forEachRemaining(action));
    }
}

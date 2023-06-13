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

package org.apache.jena.mem2.collection;

import org.apache.jena.mem2.iterator.SparseArrayIterator;
import org.apache.jena.mem2.spliterator.SparseArraySpliterator;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.ConcurrentModificationException;
import java.util.Spliterator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Shared stuff for our hashing implementations: does the base work for
 * hashing and growth sizes.
 */
public abstract class HashCommonMap<Key, Value> extends HashCommonBase<Key> implements JenaMap<Key, Value> {

    protected Value[] values;

    /**
     * Initialise this hashed thingy to have <code>initialCapacity</code> as its
     * capacity and the corresponding threshold. All the key elements start out
     * null.
     */
    protected HashCommonMap(int initialCapacity) {
        super(initialCapacity);
        this.values = newValuesArray(keys.length);
    }

    public void clear(int initialCapacity) {
        super.clear(initialCapacity);
        this.values = newValuesArray(keys.length);
    }

    @Override
    public abstract void clear();

    protected abstract Value[] newValuesArray(int size);

    /**
     * Search for the slot in which <code>key</code> is found. If it is absent,
     * return the index of the free slot in which it could be placed. If it is present,
     * return the bitwise complement of the index of the slot it appears in. Hence
     * negative values imply present, positive absent, and there's no confusion
     * around 0.
     */
    protected int findSlot(Key key) {
        int index = initialIndexFor(key.hashCode());
        while (true) {
            Key current = keys[index];
            if (current == null) return index;
            if (key.equals(current)) return ~index;
            if (--index < 0) index += keys.length;
        }
    }

    @Override
    public boolean containsKey(Key key) {
        return findSlot(key) < 0;
    }

    @Override
    public boolean anyMatch(Predicate<Key> predicate) {
        for (int i = 0; i < keys.length; i++) {
            if (keys[i] != null && predicate.test(keys[i]))
                return true;
        }
        return false;
    }

    @Override
    public boolean tryPut(Key key, Value value) {
        final var slot = findSlot(key);
        if (slot < 0) {
            values[~slot] = value;
            return false;
        }
        keys[slot] = key;
        values[slot] = value;
        if (++size > threshold) grow();
        return true;
    }

    @Override
    public void put(Key key, Value value) {
        final var slot = findSlot(key);
        if (slot < 0) {
            values[~slot] = value;
            return;
        }
        keys[slot] = key;
        values[slot] = value;
        if (++size > threshold) grow();
    }

    @Override
    public Value get(Key key) {
        final var slot = findSlot(key);
        if (slot < 0) return values[~slot];
        return null;
    }

    @Override
    public Value getOrDefault(Key key, Value defaultValue) {
        final var slot = findSlot(key);
        if (slot < 0) return values[~slot];
        return defaultValue;
    }

    @Override
    public Value computeIfAbsent(Key key, Supplier<Value> absentValueSupplier) {
        final var slot = findSlot(key);
        if (slot < 0) return values[~slot];
        final var value = absentValueSupplier.get();
        keys[slot] = key;
        values[slot] = value;
        if (++size > threshold) grow();
        return value;
    }

    @Override
    public void compute(Key key, Function<Value, Value> valueProcessor) {
        final var slot = findSlot(key);
        if (slot < 0) {
            final var value = valueProcessor.apply(values[~slot]);
            if (value == null) {
                removeFrom(~slot);
            } else {
                values[~slot] = value;
            }
        } else {
            final var value = valueProcessor.apply(null);
            if (value == null)
                return;
            keys[slot] = key;
            values[slot] = value;
            if (++size > threshold) grow();
        }
    }

    /**
     * Remove the object <code>key</code> from this hash's keys if it
     * is present (if it's absent, do nothing).
     */
    @Override
    public boolean tryRemove(Key key) {
        int slot = findSlot(key);
        if (slot < 0) {
            removeFrom(~slot);
            return true;
        }
        return false;
    }

    /**
     * Remove the object <code>key</code> from this hash's keys if it
     * is present (if it's absent, do nothing).
     */
    @Override
    public void removeUnchecked(Key key) {
        int slot = findSlot(key);
        if (slot < 0) {
            removeFrom(~slot);
        }
    }

    @Override
    protected void grow() {
        final Key[] oldContents = keys;
        final Value[] oldValues = values;
        keys = newKeysArray(calcGrownCapacityAndSetThreshold());
        values = newValuesArray(keys.length);
        for (int i = 0; i < oldContents.length; i += 1) {
            final Key key = oldContents[i];
            if (key != null) {
                final int slot = findSlot(key);
                keys[slot] = key;
                values[slot] = oldValues[i];
            }
        }
    }

    /**
     * Remove the triple at element <code>i</code> of <code>contents</code>.
     * This is an implementation of Knuth's Algorithm R from tAoCP vol3, p 527,
     * with exchanging of the roles of i and j so that they can be usefully renamed
     * to <i>here</i> and <i>scan</i>.
     * <p>
     * It relies on linear probing but doesn't require a distinguished REMOVED
     * value. Since we resize the table when it gets fullish, we don't worry [much]
     * about the overhead of the linear probing.
     * <p>
     * Iterators running over the keys may miss elements that are moved from the
     * bottom of the table to the top because of Iterator::remove. removeFrom
     * returns such a moved key as its result, and null otherwise.
     */
    @Override
    protected void removeFrom(int here) {
        size -= 1;
        while (true) {
            keys[here] = null;
            values[here] = null;
            int scan = here;
            while (true) {
                if (--scan < 0) scan += keys.length;
                if (keys[scan] == null) return;
                final int r = initialIndexFor(keys[scan].hashCode());
                if (scan <= r && r < here || r < here && here < scan || here < scan && scan <= r) {
                    /* Nothing. We'd have preferred an `unless` statement. */
                } else {
                    keys[here] = keys[scan];
                    values[here] = values[scan];
                    here = scan;
                    break;
                }
            }
        }
    }

    public ExtendedIterator<Key> keyIterator() {
        final var initialSize = size;
        final Runnable checkForConcurrentModification = () -> {
            if (size != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArrayIterator<>(keys, checkForConcurrentModification);
    }

    public Spliterator<Key> keySpliterator() {
        final var initialSize = size;
        final Runnable checkForConcurrentModification = () -> {
            if (size != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArraySpliterator<>(keys, checkForConcurrentModification);
    }

    @Override
    public ExtendedIterator<Value> valueIterator() {
        final var initialSize = size;
        final Runnable checkForConcurrentModification = () -> {
            if (size != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArrayIterator<>(values, checkForConcurrentModification);
    }

    @Override
    public Spliterator<Value> valueSpliterator() {
        final var initialSize = size;
        final Runnable checkForConcurrentModification = () -> {
            if (size != initialSize) throw new ConcurrentModificationException();
        };
        return new SparseArraySpliterator<>(values, checkForConcurrentModification);
    }
}

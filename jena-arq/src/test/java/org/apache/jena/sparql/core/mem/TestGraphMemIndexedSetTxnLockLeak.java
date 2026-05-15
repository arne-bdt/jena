/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *   SPDX-License-Identifier: Apache-2.0
 */

package org.apache.jena.sparql.core.mem;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.jena.graph.Triple;
import org.apache.jena.mem.store.TripleStore;
import org.apache.jena.query.TxnType;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.junit.jupiter.api.Test;

/**
 * Regression: if {@code published.copy()} throws inside
 * {@code GraphMemIndexedSetTxn.begin(WRITE)}, the writer slot must be
 * released — otherwise the {@code writeLock} is held forever (no
 * caller-visible transaction state was recorded that could end() or
 * abort() to release it), and every subsequent writer deadlocks.
 * <p>
 * We inject a {@link TripleStore} that throws on the first {@code copy()}
 * call and forwards to a real store thereafter, reflectively swap it into
 * the graph's {@code published} slot, and assert that a second
 * {@code begin(WRITE)} on a different thread does <i>not</i> deadlock.
 * Pre-fix code blocks the second thread forever (lock leaked); the test
 * fails the JUnit assertion via a join timeout.
 */
public class TestGraphMemIndexedSetTxnLockLeak {

    @Test
    public void copyThrowDoesNotLeakWriteLock() throws Exception {
        GraphMemIndexedSetTxn g = new GraphMemIndexedSetTxn();
        Field publishedField = GraphMemIndexedSetTxn.class.getDeclaredField("published");
        publishedField.setAccessible(true);
        TripleStore real = (TripleStore) publishedField.get(g);
        ThrowOnceTripleStore poisoned = new ThrowOnceTripleStore(real);
        publishedField.set(g, poisoned);

        // First begin(WRITE) must propagate the injected throw.
        assertThrows(RuntimeException.class, () -> g.begin(TxnType.WRITE));

        // Restore a non-throwing store so the second begin(WRITE) doesn't
        // hit the poison again — we are testing lock-release, not the
        // poisoning itself.
        publishedField.set(g, real);

        // A second begin(WRITE) on another thread must be able to acquire
        // the writer slot. If the slot was leaked, the join below times
        // out and the test fails.
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                g.begin(TxnType.WRITE);
                g.commit();
            } catch (Throwable th) {
                failure.set(th);
            } finally {
                done.countDown();
            }
        }, "second-writer");
        t.start();
        boolean finished = done.await(2, TimeUnit.SECONDS);
        if (!finished) {
            t.interrupt();
            fail("second begin(WRITE) deadlocked — writeLock was leaked by the throwing first begin(WRITE)");
        }
        if (failure.get() != null) throw new AssertionError(failure.get());
        assertTrue(finished);
    }

    /** Delegate that throws on the first {@code copy()} only. */
    private static final class ThrowOnceTripleStore implements TripleStore {
        private final TripleStore delegate;
        private boolean thrown = false;
        ThrowOnceTripleStore(TripleStore delegate) { this.delegate = delegate; }

        @Override public TripleStore copy() {
            if (!thrown) { thrown = true; throw new RuntimeException("injected copy() failure"); }
            return delegate.copy();
        }
        @Override public void add(Triple triple) { delegate.add(triple); }
        @Override public void remove(Triple triple) { delegate.remove(triple); }
        @Override public void clear() { delegate.clear(); }
        @Override public int countTriples() { return delegate.countTriples(); }
        @Override public boolean isEmpty() { return delegate.isEmpty(); }
        @Override public boolean contains(Triple t) { return delegate.contains(t); }
        @Override public Stream<Triple> stream() { return delegate.stream(); }
        @Override public Stream<Triple> stream(Triple t) { return delegate.stream(t); }
        @Override public ExtendedIterator<Triple> find(Triple t) { return delegate.find(t); }
    }
}

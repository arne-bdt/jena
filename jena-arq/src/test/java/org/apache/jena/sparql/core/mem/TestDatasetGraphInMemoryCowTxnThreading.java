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

import static org.apache.jena.graph.NodeFactory.createBlankNode;
import static org.apache.jena.query.ReadWrite.READ;
import static org.apache.jena.query.ReadWrite.WRITE;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import org.apache.jena.sparql.core.Quad;
import org.slf4j.Logger;

/**
 * Threading tests mirroring {@link TestDatasetGraphInMemoryThreading}, exercising
 * the same writer-reader interleavings against {@link DatasetGraphInMemoryCowTxn}.
 */
public class TestDatasetGraphInMemoryCowTxnThreading {

    Logger log = getLogger(TestDatasetGraphInMemoryCowTxnThreading.class);

    Quad q = Quad.create(createBlankNode(), createBlankNode(), createBlankNode(), createBlankNode());

    @Test
    public void abortedChangesNeverBecomeVisible() {
        final DatasetGraphInMemoryCowTxn dsg = new DatasetGraphInMemoryCowTxn();
        final AtomicBoolean addedButNotAborted = new AtomicBoolean(false);
        final AtomicBoolean addedCheckedButNotAborted = new AtomicBoolean(false);
        final AtomicBoolean aborted = new AtomicBoolean(false);

        dsg.begin(READ);
        assertTrue(dsg.isEmpty());
        dsg.end();

        new Thread() {
            @Override
            public void run() {
                dsg.begin(WRITE);
                log.debug("Writer: Added test quad.");
                dsg.add(q);
                assertFalse(dsg.isEmpty());
                addedButNotAborted.set(true);
                log.debug("Writer: Waiting to abort addition of test quad.");
                await().untilTrue(addedCheckedButNotAborted);
                assertFalse(dsg.isEmpty());
                log.debug("Writer: Aborting test quad.");
                dsg.abort();
                log.debug("Writer: Aborted test quad.");
                aborted.set(true);
            }
        }.start();
        log.debug("Reader: Waiting for test quad to be added in Writer thread.");
        await().untilTrue(addedButNotAborted);
        dsg.begin(READ);
        assertTrue(dsg.isEmpty());
        dsg.end();
        log.debug("Reader: Checked to see test quad is not visible.");
        addedCheckedButNotAborted.set(true);
        log.debug("Reader: Waiting to see Writer transaction aborted.");
        await().untilTrue(aborted);
        dsg.begin(READ);
        assertTrue(dsg.isEmpty());
        dsg.end();
    }

    @Test
    public void snapshotsShouldBeIsolated() {
        final DatasetGraphInMemoryCowTxn dsg = new DatasetGraphInMemoryCowTxn();
        final AtomicBoolean addedButNotCommitted = new AtomicBoolean(false);
        final AtomicBoolean addedCheckedButNotCommitted = new AtomicBoolean(false);
        final AtomicBoolean committed = new AtomicBoolean(false);

        dsg.begin(READ);
        assertTrue(dsg.isEmpty());
        dsg.end();

        new Thread() {
            @Override
            public void run() {
                dsg.begin(WRITE);
                log.debug("Writer: Added test quad.");
                dsg.add(q);
                assertFalse(dsg.isEmpty());
                addedButNotCommitted.set(true);
                log.debug("Writer: Waiting to commit test quad.");
                await().untilTrue(addedCheckedButNotCommitted);
                log.debug("Writer: Committing test quad.");
                dsg.commit();
                log.debug("Writer: Committed test quad.");
                committed.set(true);
            }
        }.start();
        log.debug("Reader: Waiting for test quad to be added in Writer thread.");
        await().untilTrue(addedButNotCommitted);

        dsg.begin(READ);
        assertTrue(dsg.isEmpty());
        log.debug("Reader: Checked to see test quad is not yet visible.");
        addedCheckedButNotCommitted.set(true);
        log.debug("Reader: Waiting to see test quad committed.");
        await().untilTrue(committed);
        assertTrue(dsg.isEmpty());
        dsg.end();
        dsg.begin(READ);
        assertFalse(dsg.isEmpty());
        dsg.end();
    }

    @Test
    public void locksAreCorrectlyDistributed() {
        final DatasetGraphInMemoryCowTxn dsg = new DatasetGraphInMemoryCowTxn();
        final AtomicBoolean readLockCaptured = new AtomicBoolean(false);
        final AtomicBoolean writeLockCaptured = new AtomicBoolean(false);

        dsg.begin(WRITE);

        new Thread() {
            @Override
            public void run() {
                dsg.begin(READ);
                readLockCaptured.set(true);
                dsg.end();

                dsg.begin(WRITE);
                writeLockCaptured.set(true);
            }
        }.start();
        await().untilTrue(readLockCaptured);
        if (writeLockCaptured.get()) fail("Write lock captured by two threads at once!");

        dsg.abort();
        dsg.end();
        await().untilTrue(writeLockCaptured);
        assertTrue(writeLockCaptured.get(), "Lock was not handed over to waiting thread!");
    }
}

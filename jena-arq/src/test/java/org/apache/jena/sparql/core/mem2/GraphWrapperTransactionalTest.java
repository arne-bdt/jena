/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.sparql.core.mem2;

import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.JenaTransactionException;
import org.awaitility.Awaitility;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.Semaphore;

import static org.apache.jena.testing_framework.GraphHelper.triple;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class GraphWrapperTransactionalTest {

    @Test
    public void testAddWithoutTransaction() {
        var sut = new GraphWrapperTransactional();
        var t = triple("s p o");
        assertThrows(JenaTransactionException.class, () -> sut.add(t));
        sut.close();
    }

    @Test
    public void testAddAndCommit() {
        var sut = new GraphWrapperTransactional();
        sut.begin(ReadWrite.WRITE);
        assertEquals(0, sut.size());
        sut.add(triple("s p o"));
        assertEquals(1, sut.size());
        sut.commit();
        sut.begin(ReadWrite.READ);
        assertEquals(1, sut.size());
        sut.end();
    }

    @Test
    public void testAddAndAbort() {
        var sut = new GraphWrapperTransactional();
        sut.begin(ReadWrite.WRITE);
        assertEquals(0, sut.size());
        sut.add(triple("s p o"));
        assertEquals(1, sut.size());
        sut.abort();
        sut.begin(ReadWrite.READ);
        assertEquals(0, sut.size());
        sut.end();
    }

    @Test
    public void testAddAndEnd() {
        var sut = new GraphWrapperTransactional();
        sut.begin(ReadWrite.WRITE);
        assertEquals(0, sut.size());
        sut.add(triple("s p o"));
        assertEquals(1, sut.size());
        assertThrows(JenaTransactionException.class, () -> sut.end());
        sut.begin(ReadWrite.READ);
        assertEquals(0, sut.size());
        sut.end();
    }

    @Test
    public void testReaderThatStartedTransactionBeforeWriteDoesNotSeeWrittenData() throws InterruptedException {
        var sut = new GraphWrapperTransactional();
        var threadHasStarted = new Semaphore(1);
        var newDataWritten = new Semaphore(1);
        threadHasStarted.acquire();
        newDataWritten.acquire();

        var readerThread = new Thread(() -> {
            sut.begin(ReadWrite.READ);
            assertEquals(0, sut.size());
            threadHasStarted.release();
            try {
                newDataWritten.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            assertEquals(0, sut.size());
            sut.end();
        });
        readerThread.start();
        threadHasStarted.acquire();
        threadHasStarted.release();

        sut.begin(ReadWrite.WRITE);
        sut.add(triple("s p o"));
        sut.commit();
        newDataWritten.release();
        sut.begin(ReadWrite.READ);
        assertEquals(1, sut.size());
        sut.end();

        readerThread.join();
    }

    @Test
    public void testReaderThatStartedAfterWriteSeesWrittenData() throws InterruptedException {
        var sut = new GraphWrapperTransactional();
        var threadHasStarted = new Semaphore(1);
        var newDataWritten = new Semaphore(1);
        threadHasStarted.acquire();
        newDataWritten.acquire();

        var readerThread = new Thread(() -> {
            sut.begin(ReadWrite.READ);
            assertEquals(0, sut.size());
            sut.end();
            threadHasStarted.release();
            try {
                newDataWritten.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            sut.begin(ReadWrite.READ);
            assertEquals(1, sut.size());
            sut.end();
        });

        readerThread.start();
        threadHasStarted.acquire();
        threadHasStarted.release();

        sut.begin(ReadWrite.WRITE);
        sut.add(triple("s p o"));
        sut.commit();
        newDataWritten.release();

        readerThread.join();
    }

    @Test
    public void testThatMultipleReaderSeeDifferentThingsWhenStartedAfterDifferentCommits() throws InterruptedException {
        var sut = new GraphWrapperTransactional();
        var threadHasStarted = new Semaphore(1);
        var oneTripleWritten = new Semaphore(1);
        var twoTriplesWritten = new Semaphore(1);
        oneTripleWritten.acquire();
        twoTriplesWritten.acquire();

        var readerThread1 = new Thread(() -> {
            threadHasStarted.release();
            try {
                oneTripleWritten.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            sut.begin(ReadWrite.READ);
            assertEquals(1, sut.size());
            sut.end();
        });
        threadHasStarted.acquire();
        readerThread1.start();
        threadHasStarted.release();

        var readerThread2 = new Thread(() -> {
            threadHasStarted.release();
            try {
                twoTriplesWritten.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            sut.begin(ReadWrite.READ);
            assertEquals(2, sut.size());
            sut.end();
        });
        threadHasStarted.acquire();
        readerThread2.start();
        threadHasStarted.release();

        sut.begin(ReadWrite.WRITE);
        sut.add(triple("s1 p1 o1"));
        sut.commit();
        oneTripleWritten.release();

        sut.begin(ReadWrite.WRITE);
        sut.add(triple("s2 p2 o2"));
        sut.commit();
        twoTriplesWritten.release();

        readerThread1.join();
        readerThread2.join();
    }

    @Test
    public void testDeltasAreProcessed() {
        var sut = new GraphWrapperTransactional();
        sut.begin(ReadWrite.WRITE);
        sut.add(triple("s p o"));
        sut.commit();

        assertEquals(1, sut.getActiveGraphLengthOfDeltaChain());
        assertEquals(0, sut.getStaleGraphLengthOfDeltaChain());

        // a separate thread should apply the deltas to the stale graph
        Awaitility
                .waitAtMost(Duration.ofMillis(200))
                .until(() -> sut.getNumberOfDeltasToApplyToStaleGraph() == 0);

        assertEquals(0, sut.getNumberOfDeltasToApplyToStaleGraph());
        assertEquals(1, sut.getActiveGraphLengthOfDeltaChain());
        assertEquals(0, sut.getStaleGraphLengthOfDeltaChain());

        // next read should switch the graphs, as there are no deltas on the stale graph
        sut.begin(ReadWrite.READ);
        sut.end();

        //expect hat active and stale have been swapped
        assertEquals(0, sut.getActiveGraphLengthOfDeltaChain());

        // a separate thread should apply the deltas to the stale graph
        Awaitility
                .waitAtMost(Duration.ofMillis(200))
                .until(() -> sut.getStaleGraphLengthOfDeltaChain() == 0);

        // all cleaned up
        assertEquals(0, sut.getNumberOfDeltasToApplyToStaleGraph());
        assertEquals(0, sut.getActiveGraphLengthOfDeltaChain());
        assertEquals(0, sut.getStaleGraphLengthOfDeltaChain());
    }
}
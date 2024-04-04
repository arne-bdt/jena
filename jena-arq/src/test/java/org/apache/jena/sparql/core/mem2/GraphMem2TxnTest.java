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

import org.apache.jena.mem2.GraphMem2Fast;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.JenaTransactionException;
import org.awaitility.Awaitility;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.jena.testing_framework.GraphHelper.triple;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.*;

public class GraphMem2TxnTest {

    @Test
    public void testAddWithoutTransaction() {
        var sut = new GraphMem2Txn();
        var t = triple("s p o");
        assertThrows(JenaTransactionException.class, () -> sut.add(t));
        sut.close();
    }

    @Test
    public void testAddAndCommit() {
        var sut = new GraphMem2Txn();
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
    public void testDeleteAndCommitDoesNotAffectWrappedGraph() {
        var graphToWrap = new GraphMem2Fast();
        graphToWrap.add(triple("s p o"));
        assertEquals(1, graphToWrap.size());

        var sut = new GraphMem2Txn(graphToWrap, GraphMem2Fast::new);
        sut.begin(ReadWrite.WRITE);
        assertEquals(1, sut.size());
        sut.delete(triple("s p o"));
        assertEquals(0, sut.size());
        sut.commit();
        sut.begin(ReadWrite.READ);
        assertEquals(0, sut.size());
        sut.end();

        assertEquals(1, graphToWrap.size());
    }

    @Test
    public void testAddAndAbort() {
        var sut = new GraphMem2Txn();
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
        var sut = new GraphMem2Txn();
        sut.begin(ReadWrite.WRITE);
        assertEquals(0, sut.size());
        sut.add(triple("s p o"));
        assertEquals(1, sut.size());
        assertThrows(JenaTransactionException.class, sut::end);
        sut.begin(ReadWrite.READ);
        assertEquals(0, sut.size());
        sut.end();
    }

    @Test
    public void testReaderThatStartedTransactionBeforeWriteDoesNotSeeWrittenData() throws InterruptedException {
        var sut = new GraphMem2Txn();
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
        var sut = new GraphMem2Txn();
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
        var sut = new GraphMem2Txn();
        var threadHasStarted = new Semaphore(1);
        var oneTripleWritten = new Semaphore(1);
        var twoTriplesWritten = new Semaphore(1);
        var oneTripleRead = new Semaphore(1);
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
            oneTripleRead.release();
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
        assertEquals(0, sut.size());
        sut.add(triple("s1 p1 o1"));
        assertEquals(1, sut.size());
        sut.commit();
        oneTripleRead.acquire();
        oneTripleWritten.release();
        oneTripleRead.acquire();
        oneTripleRead.release();

        sut.begin(ReadWrite.WRITE);
        assertEquals(1, sut.size());
        sut.add(triple("s2 p2 o2"));
        assertEquals(2, sut.size());
        sut.commit();
        twoTriplesWritten.release();

        readerThread1.join();
        readerThread2.join();
    }

    @Test
    public void testDeltasAreProcessed()  {
        var sut = new GraphMem2Txn();
        sut.begin(ReadWrite.WRITE);
        sut.add(triple("s p o"));
        sut.commit();

        // a separate thread should apply the deltas to the stale graph
        Awaitility
                .waitAtMost(Duration.ofMillis(200))
                .until(() -> sut.getActiveGraphLengthOfDeltaQueue() == 0
                        && sut.getStaleGraphLengthOfDeltaQueue() == 0
                        && sut.getActiveGraphLengthOfDeltaChain() == 0
                        && sut.getStaleGraphLengthOfDeltaChain() == 0);
    }

    @Test
    public void testWriteTransactionTimeout() throws InterruptedException {
        var semaphore = new Semaphore(1);
        var scheduler = new TransactionCoordinatorSchedulerImpl(50);
        var coordinator = new TransactionCoordinatorImpl(400, 10, scheduler) {
            @Override
            public void registerCurrentThread(Runnable timedOutRunnable) {
                super.registerCurrentThread(() -> {
                    timedOutRunnable.run();
                    semaphore.release();
                });
            }
        };

        var sut = new GraphMem2Txn(GraphMem2Fast::new, 3, coordinator);

        var t = new Thread(() -> {
            sut.begin(ReadWrite.WRITE);
            sut.add(triple("s p o"));
            sut.commit();
        });
        t.start();
        t.join();


        sut.begin(ReadWrite.READ);
        assertEquals(1, sut.size());
        sut.end();

        semaphore.acquire();

        AtomicBoolean exceptionThrown = new AtomicBoolean(false);

        t = new Thread(() -> {
            sut.begin(ReadWrite.WRITE);
            sut.add(triple("s1 p1 o1"));
            assertEquals(2, sut.size());
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            semaphore.release();
            try
            {
                sut.commit();
            }
            catch (JenaTransactionException e)
            {
                exceptionThrown.set(true);
            }
        });
        t.start();

        await().atMost(1000, TimeUnit.MILLISECONDS).until(() -> semaphore.availablePermits() == 1);

        t.join();

        assertTrue(exceptionThrown.get());

        sut.begin(ReadWrite.READ);
        assertEquals(1, sut.size());
        sut.end();
    }

    @Test
    public void testReadTransactionTimeout() throws InterruptedException {
        var semaphore = new Semaphore(1);
        var scheduler = new TransactionCoordinatorSchedulerImpl(50);
        var coordinator = new TransactionCoordinatorImpl(400, 10, scheduler) {
            @Override
            public void registerCurrentThread(Runnable timedOutRunnable) {
                super.registerCurrentThread(() -> {
                    timedOutRunnable.run();
                    semaphore.release();
                });
            }
        };
        var sut = new GraphMem2Txn(GraphMem2Fast::new, 3, coordinator);

        sut.begin(ReadWrite.WRITE);
        sut.add(triple("s p o"));
        sut.commit();

        semaphore.acquire();

        AtomicBoolean exceptionThrown = new AtomicBoolean(false);

        var t = new Thread(() -> {
            sut.begin(ReadWrite.READ);
            assertEquals(1, sut.size());
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            semaphore.release();
            try
            {
                sut.size();
            }
            catch (JenaTransactionException e)
            {
                exceptionThrown.set(true);
            }

        });
        t.start();

        await().atMost(1000, TimeUnit.MILLISECONDS).until(() -> semaphore.availablePermits() == 1);

        t.join();

        assertTrue(exceptionThrown.get());
    }
}
package org.apache.jena.sparql.core.mem2;

import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.JenaTransactionException;
import org.junit.Test;

import java.util.concurrent.Semaphore;

import static org.apache.jena.testing_framework.GraphHelper.triple;
import static org.junit.Assert.*;

public class GraphWrapperTransactionalTest {

    @Test
    public void testAddWithoutTransaction() {
        try (final var transactionCoordinator = new TransactionCoordinatorMRPlusSW()) {
            var sut = new GraphWrapperTransactional(transactionCoordinator);
            assertThrows(JenaTransactionException.class, () -> sut.add(triple("s p o")));
            sut.close();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testAddAndCommit() {
        try (final var transactionCoordinator = new TransactionCoordinatorMRPlusSW()) {
            var sut = new GraphWrapperTransactional(transactionCoordinator);
            sut.begin(ReadWrite.WRITE);
            assertEquals(0, sut.size());
            sut.add(triple("s p o"));
            assertEquals(1, sut.size());
            sut.commit();
            sut.begin(ReadWrite.READ);
            assertEquals(1, sut.size());
            sut.end();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testAddAndAbort() {
        try (final var transactionCoordinator = new TransactionCoordinatorMRPlusSW()) {
            var sut = new GraphWrapperTransactional(transactionCoordinator);
            sut.begin(ReadWrite.WRITE);
            assertEquals(0, sut.size());
            sut.add(triple("s p o"));
            assertEquals(1, sut.size());
            sut.abort();
            sut.begin(ReadWrite.READ);
            assertEquals(0, sut.size());
            sut.end();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testAddAndEnd() {
        try (final var transactionCoordinator = new TransactionCoordinatorMRPlusSW()) {
            var sut = new GraphWrapperTransactional(transactionCoordinator);
            sut.begin(ReadWrite.WRITE);
            assertEquals(0, sut.size());
            sut.add(triple("s p o"));
            assertEquals(1, sut.size());
            sut.end();
            sut.begin(ReadWrite.READ);
            assertEquals(0, sut.size());
            sut.end();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testReaderThatStartedTransactionBeforeWriteDoesNotSeeWrittenData() {
        try (final var transactionCoordinator = new TransactionCoordinatorMRPlusSW()) {
            var sut = new GraphWrapperTransactional(transactionCoordinator);
            var newDataWritten = new Semaphore(1);
            newDataWritten.acquire();

            var readerThread = new Thread(() -> {
                sut.begin(ReadWrite.READ);
                assertEquals(0, sut.size());
                try {
                    newDataWritten.acquire();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                assertEquals(0, sut.size());
                sut.end();
            });
            readerThread.start();

            sut.begin(ReadWrite.WRITE);
            sut.add(triple("s p o"));
            sut.commit();
            newDataWritten.release();
            sut.begin(ReadWrite.READ);
            assertEquals(1, sut.size());
            sut.end();

            readerThread.join();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testReaderThatStartedAfterWriteSeesWrittenData() {
        try (final var transactionCoordinator = new TransactionCoordinatorMRPlusSW()) {
            var sut = new GraphWrapperTransactional(transactionCoordinator);
            var newDataWritten = new Semaphore(1);
            newDataWritten.acquire();

            var readerThread = new Thread(() -> {
                sut.begin(ReadWrite.READ);
                assertEquals(0, sut.size());
                sut.end();
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

            sut.begin(ReadWrite.WRITE);
            sut.add(triple("s p o"));
            sut.commit();
            newDataWritten.release();

            readerThread.join();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testDeltasAreProcessed() {
        try (final var transactionCoordinator = new TransactionCoordinatorMRPlusSW()) {
            var sut = new GraphWrapperTransactional(transactionCoordinator);
            sut.begin(ReadWrite.WRITE);
            sut.add(triple("s p o"));
            sut.commit();

            assertEquals(1, sut.getNumberOfDeltasToApplyToStaleGraph());
            assertEquals(1, sut.getActiveGraphLengthOfDeltaChain());
            assertEquals(0, sut.getStaleGraphLengthOfDeltaChain());

            var timout = System.currentTimeMillis() + 200;
            while (sut.getNumberOfDeltasToApplyToStaleGraph() > 0 && System.currentTimeMillis() < timout) {
                Thread.sleep(10);
            }
            assertEquals(0, sut.getNumberOfDeltasToApplyToStaleGraph());
            assertEquals(1, sut.getActiveGraphLengthOfDeltaChain());
            assertEquals(0, sut.getStaleGraphLengthOfDeltaChain());

            sut.begin(ReadWrite.READ);
            sut.end();

            //expect hat active and stale have been swapped
            assertEquals(0, sut.getActiveGraphLengthOfDeltaChain());
            assertEquals(1, sut.getStaleGraphLengthOfDeltaChain());

            timout = System.currentTimeMillis() + 200;
            while (sut.getStaleGraphLengthOfDeltaChain() > 0 && System.currentTimeMillis() < timout) {
                Thread.sleep(10);
            }

            assertEquals(0, sut.getNumberOfDeltasToApplyToStaleGraph());
            assertEquals(0, sut.getActiveGraphLengthOfDeltaChain());
            assertEquals(0, sut.getStaleGraphLengthOfDeltaChain());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
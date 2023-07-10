package org.apache.jena.sparql.core.mem2;

import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.JenaTransactionException;
import org.junit.Test;

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
}
package org.apache.jena.sparql.core.mem2;

public interface TransactionCoordinatorScheduler extends AutoCloseable {

    static TransactionCoordinatorScheduler getInstance() {
        return TransactionCoordinatorSchedulerImpl.getInstance();
    }

    void register(TransactionCoordinator coordinator);

    void unregister(TransactionCoordinator coordinator);

    int getStaleTransactionRemovalTimerIntervalMs();
}

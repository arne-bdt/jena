package org.apache.jena.sparql.core.mem2;


import org.apache.jena.sparql.JenaTransactionException;

public interface TransactionCoordinator {

    int DEFAULT_TRANSACTION_TIMEOUT_MS = 30000;

    int KEEP_INFO_ABOUT_TRANSACTION_TIMEOUT_FOR_X_TIMES_THE_TIMEOUT = 10;

    int DEFAULT_STAlE_TRANSACTION_REMOVAL_TIMER_INTERVAL_MS = 5000;

    /**
     * Looks up if a transaction for the given threadID recently timed out.
     * Timeouts are only remembered for KEEP_INFO_ABOUT_TRANSACTION_TIMEOUT_FOR_X_TIMES_THE_TIMEOUT
     * times the timeout interval.
     *
     * @param threadID
     * @return
     */
    boolean hasTimedOut(long threadID);

    void registerCurrentThread(Runnable timedOutRunnable);

    /**
     * Refreshes the timeout for the current thread.
     * Throws an exception if the current thread is not registered.
     * Throws an exception if the current thread has timed out.
     *
     * @throws JenaTransactionException
     */
    void refreshTimeoutForCurrentThread();

    void unregisterCurrentThread();


    // Diagnostics
    int getStaleTransactionRemovalTimerIntervalMs();

    int getTransactionTimeoutMs();

    // Diagnostics
    int getNumberOfActiveReadTransactions();

    boolean hasActiveWriteTransaction();

    /**
     * Counts the number of READ transactions that have recently been aborted due to a timeout.
     * Timeouts are only remembered for KEEP_INFO_ABOUT_TRANSACTION_TIMEOUT_FOR_X_TIMES_THE_TIMEOUT
     * times the timeout interval.
     *
     * @return the number of transactions that have been aborted due to a timeout
     */
    int getReadTimeoutsCount();

    /**
     * Counts the number of WRITE transactions that have recently been aborted due to a timeout.
     * Timeouts are only remembered for KEEP_INFO_ABOUT_TRANSACTION_TIMEOUT_FOR_X_TIMES_THE_TIMEOUT
     * times the timeout interval.
     *
     * @return the number of transactions that have been aborted due to a timeout
     */
    int getWriteTimeoutsCount();


}

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

package org.apache.jena.sparql.core.mem2;


import org.apache.jena.sparql.JenaTransactionException;

/**
 * This class is responsible for keeping track of the threads that are currently running transactions.
 * It is also responsible for checking if a thread has timed out and calling the runnable that was passed to the
 * {@link #registerCurrentThread(Runnable)} method.
 */
public interface TransactionCoordinator extends AutoCloseable {

    int DEFAULT_TRANSACTION_TIMEOUT_MS = 30000;

    int DEFAULT_KEEP_INFO_ABOUT_TRANSACTION_TIMEOUT_FOR_X_TIMES_THE_TIMEOUT = 10;

    void registerCurrentThread(Runnable timedOutRunnable);

    /**
     * Refreshes the timeout for the current thread.
     * Throws an exception if the current thread is not registered.
     * Throws an exception if the current thread has timed out.
     *
     * @throws JenaTransactionException if the current thread is not registered or has timed out.
     */
    void refreshTimeoutForCurrentThread();

    void unregisterCurrentThread();


    // Diagnostics
    int getStaleTransactionRemovalTimerIntervalMs();

    int getTransactionTimeoutMs();

    void checkForTimeouts();

    void removeLongTimedOutTransactions();
}

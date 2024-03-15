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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A singleton scheduler for {@link TransactionCoordinator}s.
 * <p>
 * This scheduler is responsible for periodically checking for stale transactions and for
 * periodically checking for transactions that have timed out.
 * </p>
 */
public class TransactionCoordinatorSchedulerImpl implements TransactionCoordinatorScheduler {

    private static final Object DUMMY = new Object();
    private static final int DEFAULT_STALE_TRANSACTION_REMOVAL_TIMER_INTERVAL_MS = 5000;
    private static final TransactionCoordinatorSchedulerImpl instance = new TransactionCoordinatorSchedulerImpl();
    private final ConcurrentHashMap<TransactionCoordinator, Object> transactionCoordinators = new ConcurrentHashMap<>();

    final int staleTransactionRemovalTimerIntervalMs;

    private ScheduledExecutorService scheduledExecutorService;
    private boolean running = false;

    /*package*/ TransactionCoordinatorSchedulerImpl() {
        this(DEFAULT_STALE_TRANSACTION_REMOVAL_TIMER_INTERVAL_MS);
    }

    /*package*/ TransactionCoordinatorSchedulerImpl(final int staleTransactionRemovalTimerIntervalMs) {
        this.staleTransactionRemovalTimerIntervalMs = staleTransactionRemovalTimerIntervalMs;
    }

    public static TransactionCoordinatorScheduler getInstance() {
        return instance;
    }

    @Override
    public synchronized void register(TransactionCoordinator coordinator) {
        transactionCoordinators.put(coordinator, DUMMY);
        if (!running) {
            this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory());
            this.scheduledExecutorService
                    .scheduleWithFixedDelay(this::staleTransactionCleanup,
                            staleTransactionRemovalTimerIntervalMs,
                            staleTransactionRemovalTimerIntervalMs, TimeUnit.MILLISECONDS);
            running = true;
        }
    }

    @Override
    public synchronized void unregister(TransactionCoordinator coordinator) {
        transactionCoordinators.remove(coordinator);
        if (transactionCoordinators.isEmpty()) {
            if(this.scheduledExecutorService != null) {
                this.scheduledExecutorService.shutdown();
                this.scheduledExecutorService = null;
            }
            running = false;
        }
    }

    @Override
    public int getStaleTransactionRemovalTimerIntervalMs() {
        return staleTransactionRemovalTimerIntervalMs;
    }

    private void staleTransactionCleanup() {
        transactionCoordinators.keySet().forEach(tc -> {
            tc.removeLongTimedOutTransactions(); // do this fist, because the next method may add new transactions
            tc.checkForTimeouts();
        });
    }

    @Override
    public synchronized void close() throws Exception {
        if (this.scheduledExecutorService != null) {
            this.scheduledExecutorService.shutdown();
        }
        this.transactionCoordinators.clear();
        running = false;
    }
}

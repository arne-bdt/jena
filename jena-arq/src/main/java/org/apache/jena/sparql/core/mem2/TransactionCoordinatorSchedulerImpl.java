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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionCoordinatorSchedulerImpl implements TransactionCoordinatorScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionCoordinatorSchedulerImpl.class);
    private static final Object DUMMY = new Object();
    private static final int STALE_TRANSACTION_REMOVAL_TIMER_INTERVAL_MS = 5000;
    private static TransactionCoordinatorSchedulerImpl instance = null;
    private final ConcurrentHashMap<TransactionCoordinator, Object> transactionCoordinators = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduledExecutorService;
    private final ReentrantLock lock = new ReentrantLock();
    private boolean running = false;

    private TransactionCoordinatorSchedulerImpl() {
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    }

    public static TransactionCoordinatorScheduler getInstance() {
        if (instance == null) {
            instance = new TransactionCoordinatorSchedulerImpl();
        }
        return instance;
    }

    @Override
    public void register(TransactionCoordinator coordinator) {
        try {
            lock.lock();
            transactionCoordinators.put(coordinator, DUMMY);
            if (!running) {
                this.scheduledExecutorService
                        .scheduleWithFixedDelay(this::staleTransactionCleanup,
                                STALE_TRANSACTION_REMOVAL_TIMER_INTERVAL_MS,
                                STALE_TRANSACTION_REMOVAL_TIMER_INTERVAL_MS, TimeUnit.MILLISECONDS);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void unregister(TransactionCoordinator coordinator) {
        try {
            lock.lock();
            transactionCoordinators.remove(coordinator);
            if (transactionCoordinators.isEmpty()) {
                this.scheduledExecutorService.shutdown();
                running = false;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int getStaleTransactionRemovalTimerIntervalMs() {
        return STALE_TRANSACTION_REMOVAL_TIMER_INTERVAL_MS;
    }

    private void staleTransactionCleanup() {
        transactionCoordinators.keySet().forEach(tc -> {
            try {
                tc.removeLongTimedOutTransactions(); // do this fist, because the next method may add new transactions
                tc.checkForTimeouts();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }

    @Override
    public void close() throws Exception {
        try {
            lock.lock();
            this.scheduledExecutorService.shutdown();
            this.transactionCoordinators.clear();
            running = false;
        } finally {
            lock.unlock();
        }
        instance = null;
    }
}

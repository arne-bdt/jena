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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TransactionCoordinatorMRPlusSW implements TransactionCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionCoordinatorMRPlusSW.class);
    private final ConcurrentHashMap<Long, TheadTransactionInfo> activeThreadsByThreadId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, TheadTransactionInfo> timedOutThreadsByThreadId = new ConcurrentHashMap<>();
    private final int staleTransactionRemovalTimerIntervalMs;
    private final int transactionTimeoutMs;
    private final int timeToKeepTransactionsAfterTimeoutMs;
    private final ScheduledExecutorService scheduledExecutorService;

    public TransactionCoordinatorMRPlusSW(int transactionTimeoutMs, int staleTransactionRemovalTimerIntervalMs,
                                          int keepInfoAboutTransactionTimeoutForXTimesTheTimeout) {
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.staleTransactionRemovalTimerIntervalMs = staleTransactionRemovalTimerIntervalMs;
        this.timeToKeepTransactionsAfterTimeoutMs
                = transactionTimeoutMs * keepInfoAboutTransactionTimeoutForXTimesTheTimeout;
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        this.scheduledExecutorService
                .scheduleWithFixedDelay(this::staleTransactionCleanup, staleTransactionRemovalTimerIntervalMs,
                        staleTransactionRemovalTimerIntervalMs, TimeUnit.MILLISECONDS);

    }

    public TransactionCoordinatorMRPlusSW() {
        this(TransactionCoordinator.DEFAULT_TRANSACTION_TIMEOUT_MS,
                TransactionCoordinator.DEFAULT_STAlE_TRANSACTION_REMOVAL_TIMER_INTERVAL_MS,
                TransactionCoordinator.DEFAULT_KEEP_INFO_ABOUT_TRANSACTION_TIMEOUT_FOR_X_TIMES_THE_TIMEOUT);

    }

    private void staleTransactionCleanup() {
        removeOldTransactions(); // do this fist, because the next method may add new transactions
        checkTransactionsForTimeouts();
    }

    private void checkTransactionsForTimeouts() {
        activeThreadsByThreadId.values().stream()
                .filter(tInfo -> tInfo.isTimedOut() || !tInfo.getThread().isAlive())
                .forEach(tInfo -> {
                    if (!tInfo.getThread().isAlive()) {
                        LOGGER.error("Thread '{}' [{}] has timed out. Calling runnable for timed out thread.",
                                tInfo.getThread().getName(), tInfo.getThread().getId());
                    } else {
                        LOGGER.error("Thread '{}' [{}] is not alive. Calling runnable timed out thread.",
                                tInfo.getThread().getName(), tInfo.getThread().getId());
                    }
                    tInfo.callTimedOutRunnableAndCatchAll();
                    activeThreadsByThreadId.remove(tInfo.getThread().getId());
                    timedOutThreadsByThreadId.put(tInfo.getThread().getId(), tInfo);
                });
    }

    private void removeOldTransactions() {
        timedOutThreadsByThreadId.entrySet().removeIf(entry -> entry.getValue().isOldEnoughToBeRemoved());
    }

    @Override
    public void registerCurrentThread(Runnable timedOutRunnable) {
        final var thread = Thread.currentThread();
        activeThreadsByThreadId.put(thread.getId(), new TheadTransactionInfo(thread, timedOutRunnable));
    }

    @Override
    public void refreshTimeoutForCurrentThread() {
        final var thread = Thread.currentThread();
        var threadTransactionInfo = activeThreadsByThreadId.get(thread.getId());
        if (threadTransactionInfo == null) {
            threadTransactionInfo = timedOutThreadsByThreadId.get(thread.getId());
            if (threadTransactionInfo == null) {
                LOGGER.error("Thread '{}' [{}] is not registered", thread.getName(), thread.getId());
                throw new JenaTransactionException("Thread is not registered");
            }
            throw new JenaTransactionException("Thread has timed out");
        }
        if (threadTransactionInfo.isTimedOut()) {
            activeThreadsByThreadId.remove(thread.getId());
            timedOutThreadsByThreadId.put(thread.getId(), threadTransactionInfo);
            LOGGER.error("Thread '{}' [{}] has timed out", thread.getName(), thread.getId());
            throw new JenaTransactionException("Thread has timed out");
        }
        threadTransactionInfo.refresh();
    }

    @Override
    public void unregisterCurrentThread() {
        final var thread = Thread.currentThread();
        var removedThreadInfo = activeThreadsByThreadId.remove(thread.getId());
        if (removedThreadInfo == null) {
            removedThreadInfo = timedOutThreadsByThreadId.remove(thread.getId());
            if (removedThreadInfo == null) {
                LOGGER.error("Thread '{}' [{}] is not registered", thread.getName(), thread.getId());
                throw new JenaTransactionException("Thread is not registered");
            } else {
                LOGGER.error("Thread '{}' [{}] has timed out", thread.getName(), thread.getId());
                throw new JenaTransactionException("Thread has timed out before it was unregistered.");
            }
        }
    }

    @Override
    public int getStaleTransactionRemovalTimerIntervalMs() {
        return this.staleTransactionRemovalTimerIntervalMs;
    }

    @Override
    public int getTransactionTimeoutMs() {
        return this.transactionTimeoutMs;
    }

    @Override
    public void close() throws Exception {
        this.scheduledExecutorService.shutdownNow();
        this.activeThreadsByThreadId.values().stream()
                .forEach(tInfo -> {
                    LOGGER.error("Thread '{}' [{}] time out runnable is called due to closing of transaction coordinator",
                            tInfo.getThread().getName(), tInfo.getThread().getId());
                    tInfo.callTimedOutRunnableAndCatchAll();
                });
        this.activeThreadsByThreadId.clear();
        this.timedOutThreadsByThreadId.clear();
    }

    private class TheadTransactionInfo {
        private final Thread thread;
        private final Runnable timedOutRunnable;
        private long lastRefreshedTime;

        public TheadTransactionInfo(Thread thread, Runnable timedOutRunnable) {
            this.thread = thread;
            this.timedOutRunnable = timedOutRunnable;
            this.lastRefreshedTime = System.currentTimeMillis();
        }

        public Thread getThread() {
            return thread;
        }

        public void callTimedOutRunnableAndCatchAll() {
            try {
                timedOutRunnable.run();
            } catch (Throwable t) {
                LOGGER.error(String.format("Error while calling runnable for timed out thread '%s' [%s]",
                                thread.getName(),
                                thread.getId()),
                        t);
            }
        }

        public void refresh() {
            lastRefreshedTime = System.currentTimeMillis();
        }

        public boolean isTimedOut() {
            return System.currentTimeMillis() - lastRefreshedTime > timeToKeepTransactionsAfterTimeoutMs;
        }

        public boolean isOldEnoughToBeRemoved() {
            return System.currentTimeMillis() - lastRefreshedTime > timeToKeepTransactionsAfterTimeoutMs;
        }
    }
}

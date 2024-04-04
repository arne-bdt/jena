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
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is responsible for keeping track of the threads that are currently running transactions.
 * It is also responsible for checking if a thread has timed out and calling the runnable that was passed to the
 * {@link #registerCurrentThread(Runnable)} method.
 */
public class TransactionCoordinatorImpl implements TransactionCoordinator {

    private static final AtomicLong instanceCounter = new AtomicLong(0);
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionCoordinatorImpl.class);
    private final ConcurrentHashMap<Long, TheadTransactionInfo> activeThreadsByThreadId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, TheadTransactionInfo> timedOutThreadsByThreadId = new ConcurrentHashMap<>();
    private final int transactionTimeoutMs;
    private final int timeToKeepTransactionsAfterTimeoutMs;
    private final TransactionCoordinatorScheduler transactionCoordinatorScheduler;

    private final long instanceId = instanceCounter.incrementAndGet();

    public TransactionCoordinatorImpl(final int transactionTimeoutMs,
                                      final int keepInfoAboutTransactionTimeoutForXTimesTheTimeout,
                                      final TransactionCoordinatorScheduler transactionCoordinatorScheduler) {
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.timeToKeepTransactionsAfterTimeoutMs
                = transactionTimeoutMs * keepInfoAboutTransactionTimeoutForXTimesTheTimeout;
        this.transactionCoordinatorScheduler = transactionCoordinatorScheduler;
    }

    public TransactionCoordinatorImpl() {
        this(TransactionCoordinator.DEFAULT_TRANSACTION_TIMEOUT_MS,
                TransactionCoordinator.DEFAULT_KEEP_INFO_ABOUT_TRANSACTION_TIMEOUT_FOR_X_TIMES_THE_TIMEOUT,
                TransactionCoordinatorScheduler.getInstance());
    }

    public void checkForTimeouts() {
        activeThreadsByThreadId.values().stream()
                .filter(tInfo -> tInfo.isTimedOut() || !tInfo.getThread().isAlive())
                .forEach(tInfo -> {
                    if (tInfo.getThread().isAlive()) {
                        LOGGER.error("Thread '{}' [{}] has timed out. Calling runnable for timed out thread.",
                                tInfo.getThread().getName(), tInfo.getThread().threadId());
                    } else {
                        LOGGER.error("Thread '{}' [{}] is not alive. Calling runnable timed out thread.",
                                tInfo.getThread().getName(), tInfo.getThread().threadId());
                    }
                    tInfo.callTimedOutRunnableAndCatchAll();
                    activeThreadsByThreadId.remove(tInfo.getThread().threadId());
                    timedOutThreadsByThreadId.put(tInfo.getThread().threadId(), tInfo);
                });
    }

    public void removeLongTimedOutTransactions() {
        timedOutThreadsByThreadId.entrySet().removeIf(entry -> entry.getValue().isOldEnoughToBeRemoved());
    }

    @Override
    public synchronized void registerCurrentThread(Runnable timedOutRunnable) {
        if (activeThreadsByThreadId.isEmpty()) {
            transactionCoordinatorScheduler.register(this);
        }
        final var thread = Thread.currentThread();
        activeThreadsByThreadId.put(thread.threadId(), new TheadTransactionInfo(thread, timedOutRunnable));
    }

    @Override
    public void refreshTimeoutForCurrentThread() {
        final var thread = Thread.currentThread();
        var threadTransactionInfo = activeThreadsByThreadId.get(thread.threadId());
        if (threadTransactionInfo == null) {
            threadTransactionInfo = timedOutThreadsByThreadId.get(thread.threadId());
            if (threadTransactionInfo == null) {
                LOGGER.error("Thread '{}' [{}] is not registered", thread.getName(), thread.threadId());
                throw new JenaTransactionException("Thread is not registered");
            }
            throw new JenaTransactionException("Thread has timed out");
        }
        if (threadTransactionInfo.isTimedOut()) {
            activeThreadsByThreadId.remove(thread.threadId());
            timedOutThreadsByThreadId.put(thread.threadId(), threadTransactionInfo);
            LOGGER.error("Thread '{}' [{}] has timed out", thread.getName(), thread.threadId());
            throw new JenaTransactionException("Thread has timed out");
        }
        threadTransactionInfo.refresh();
    }

    @Override
    public synchronized void unregisterCurrentThread() {
        final var thread = Thread.currentThread();
        var removedThreadInfo = activeThreadsByThreadId.remove(thread.threadId());
        if (removedThreadInfo == null) {
            removedThreadInfo = timedOutThreadsByThreadId.remove(thread.threadId());
            if (removedThreadInfo == null) {
                LOGGER.error("Thread '{}' [{}] is not registered", thread.getName(), thread.threadId());
                throw new JenaTransactionException("Thread is not registered");
            } else {
                LOGGER.error("Thread '{}' [{}] has timed out", thread.getName(), thread.threadId());
                throw new JenaTransactionException("Thread has timed out before it was unregistered.");
            }
        } else {
            timedOutThreadsByThreadId.remove(thread.threadId());
        }
        if (activeThreadsByThreadId.isEmpty() && timedOutThreadsByThreadId.isEmpty()) {
            transactionCoordinatorScheduler.unregister(this);
        }
    }

    @Override
    public int getStaleTransactionRemovalTimerIntervalMs() {
        return this.transactionCoordinatorScheduler.getStaleTransactionRemovalTimerIntervalMs();
    }

    @Override
    public int getTransactionTimeoutMs() {
        return this.transactionTimeoutMs;
    }

    @Override
    public void close() throws Exception {
        this.transactionCoordinatorScheduler.unregister(this);
        this.activeThreadsByThreadId.values()
                .forEach(tInfo -> {
                    LOGGER.error("Thread '{}' [{}] time out runnable is called due to closing of transaction coordinator",
                            tInfo.getThread().getName(), tInfo.getThread().threadId());
                    tInfo.callTimedOutRunnableAndCatchAll();
                });
        this.activeThreadsByThreadId.clear();
        this.timedOutThreadsByThreadId.clear();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TransactionCoordinatorImpl that = (TransactionCoordinatorImpl) o;

        return instanceId == that.instanceId;
    }

    @Override
    public int hashCode() {
        return (int) (instanceId ^ (instanceId >>> 32));
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
            } catch (Exception exception) {
                LOGGER.error(String.format("Error while calling runnable for timed out thread '%s' [%s]",
                                thread.getName(),
                                thread.threadId()),
                        exception);
            }
        }

        public void refresh() {
            lastRefreshedTime = System.currentTimeMillis();
        }

        public boolean isTimedOut() {
            return (System.currentTimeMillis() - lastRefreshedTime) > transactionTimeoutMs;
        }

        public boolean isOldEnoughToBeRemoved() {
            return (System.currentTimeMillis() - lastRefreshedTime) > timeToKeepTransactionsAfterTimeoutMs;
        }
    }
}

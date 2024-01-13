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

/**
 * A singleton scheduler for {@link TransactionCoordinator}s.
 * <p>
 * This scheduler is responsible for periodically checking for stale transactions and for
 * periodically checking for transactions that have timed out.
 * </p>
 */
public interface TransactionCoordinatorScheduler extends AutoCloseable {

    static TransactionCoordinatorScheduler getInstance() {
        return TransactionCoordinatorSchedulerImpl.getInstance();
    }

    void register(TransactionCoordinator coordinator);

    void unregister(TransactionCoordinator coordinator);

    int getStaleTransactionRemovalTimerIntervalMs();
}

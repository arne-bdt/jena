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
package org.apache.jena.mem2.collection;

import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.Spliterator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface JenaMap<K, V> extends JenaMapSetCommon<K> {

    boolean tryPut(K key, V value);

    void put(K key, V value);

    V get(K key);

    V getOrDefault(K key, V defaultValue);

    V computeIfAbsent(K key, Supplier<V> absentValueSupplier);

    void compute(K key, UnaryOperator<V> valueProcessor);

    ExtendedIterator<V> valueIterator();

    Spliterator<V> valueSpliterator();

    default Stream<V> valueStream() {
        return StreamSupport.stream(valueSpliterator(), false);
    }

    default Stream<V> valueStreamParallel() {
        return StreamSupport.stream(valueSpliterator(), false);
    }
}

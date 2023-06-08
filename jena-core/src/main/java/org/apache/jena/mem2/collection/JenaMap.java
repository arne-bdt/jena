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
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface JenaMap<Key, Value> extends JenaMapSetCommon<Key> {

    boolean tryPut(Key key, Value value);

    void put(Key key, Value value);

    Value get(Key key);

    Value getOrDefault(Key key, Value defaultValue);

    Value computeIfAbsent(Key key, Supplier<Value> absentValueSupplier);

    void compute(Key key, Function<Value, Value> valueProcessor);

    ExtendedIterator<Value> valueIterator();

    Spliterator<Value> valueSpliterator();

    default Stream<Value> valueStream() {
        return StreamSupport.stream(valueSpliterator(), false);
    }

    default Stream<Value> valueStreamParallel() {
        return StreamSupport.stream(valueSpliterator(), false);
    }
}

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
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface JenaMapSetCommon<Key> {

    void clear();

    int size();

    boolean isEmpty();

    boolean containsKey(Key key);

    boolean anyMatch(Predicate<Key> predicate);

    boolean tryRemove(Key key);

    void removeUnchecked(Key key);

    ExtendedIterator<Key> keyIterator();

    Spliterator<Key> keySpliterator();

    default Stream<Key> keyStream() {
        return StreamSupport.stream(keySpliterator(), false);
    }

}

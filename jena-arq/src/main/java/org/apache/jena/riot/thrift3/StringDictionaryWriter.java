/**
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

package org.apache.jena.riot.thrift3;

import java.util.List;

public class StringDictionaryWriter {
    private final StringSet stringSet = new StringSet();

    private int flushStartIndex = 0;

    public int getIndex(String string) {
        final var index = stringSet.addAndGetIndex(string);
        if(index < 0) {
            return ~index;
        }
        return index;
    }

    public boolean hasStringsToFlush() {
        return flushStartIndex < stringSet.size();
    }

    public List<String> flush() {
        if(hasStringsToFlush()) {
            final var result = stringSet.subList(flushStartIndex, stringSet.size());
            flushStartIndex = stringSet.size();
            return result;
        }
        throw  new IllegalStateException("No strings to flush");
    }

    public void clear() {
        stringSet.clear();
        flushStartIndex = 0;
    }
}

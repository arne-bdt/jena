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

package org.apache.jena.cimxml.utils;

import java.util.ArrayList;
import java.util.List;

public class AttributeCollection {
    private final StreamBufferRoot streamingBufferRoot;
    private final List<AttributeFixedBuffer> attributeFixedBuffers = new ArrayList<>();
    private int currentAttributeIndex = -1;

    public AttributeCollection(StreamBufferRoot streamingBufferRoot) {
        this.streamingBufferRoot = streamingBufferRoot;
    }

    public void reset() {
        currentAttributeIndex = -1;
    }

    public QNameByteBuffer newAttribute() {
        final AttributeFixedBuffer buffer;
        currentAttributeIndex++;
        if (currentAttributeIndex == attributeFixedBuffers.size()) {
            buffer = new AttributeFixedBuffer(
                    new QNameByteBuffer(streamingBufferRoot),
                    new DecodingTextByteBuffer(streamingBufferRoot));
            attributeFixedBuffers.add(buffer);
        } else {
            buffer = attributeFixedBuffers.get(currentAttributeIndex);
            buffer.resetToUnconsumed();
        }
        return buffer.name();
    }

    public void discardCurrentAttribute() {
        currentAttributeIndex--;
    }

    public DecodingTextByteBuffer currentAttributeValue() {
        return attributeFixedBuffers.get(currentAttributeIndex).value();
    }

    public boolean isEmpty() {
        return currentAttributeIndex < 0;
    }

    public int size() {
        return currentAttributeIndex + 1;
    }

    public AttributeFixedBuffer get(int index) {
        return attributeFixedBuffers.get(index);
    }
}

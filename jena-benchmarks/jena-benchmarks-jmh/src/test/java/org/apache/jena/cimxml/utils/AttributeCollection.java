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

public class AttributeCollection {
    private final StreamBufferRoot streamingBufferRoot;
    private AttributeFixedBuffer[] attributeFixedBuffers = new AttributeFixedBuffer[8];
    private int currentAttributeIndex = -1;
    private int numerOfFilledAttributes = 0;

    public AttributeCollection(StreamBufferRoot streamingBufferRoot) {
        this.streamingBufferRoot = streamingBufferRoot;
    }

    public void reset() {
        currentAttributeIndex = -1;
    }

    private void growAttributeFixedBuffersIfNeeded() {
        if(currentAttributeIndex >= attributeFixedBuffers.length) {
            final var newLength = attributeFixedBuffers.length + 8;
            final var col = attributeFixedBuffers;
            attributeFixedBuffers = new AttributeFixedBuffer[newLength];
            System.arraycopy(col, 0, attributeFixedBuffers, 0, col.length);
        }
    }

    public QNameByteBuffer newAttribute() {
        final AttributeFixedBuffer buffer;
        currentAttributeIndex++;
        if (currentAttributeIndex == numerOfFilledAttributes) {
            growAttributeFixedBuffersIfNeeded();
            buffer = new AttributeFixedBuffer(
                    new QNameByteBuffer(streamingBufferRoot),
                    new DecodingTextByteBuffer(streamingBufferRoot));
            attributeFixedBuffers[currentAttributeIndex] = buffer;
            numerOfFilledAttributes++;
        } else {
            buffer = attributeFixedBuffers[currentAttributeIndex];
            buffer.resetToUnconsumed();
        }
        return buffer.name();
    }

    public void discardCurrentAttribute() {
        currentAttributeIndex--;
    }

    public DecodingTextByteBuffer currentAttributeValue() {
        return attributeFixedBuffers[currentAttributeIndex].value();
    }

    public boolean isEmpty() {
        return currentAttributeIndex < 0;
    }

    public int size() {
        return currentAttributeIndex + 1;
    }

    public AttributeFixedBuffer get(int index) {
        return attributeFixedBuffers[index];
    }
}

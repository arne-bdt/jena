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

import static java.nio.charset.StandardCharsets.UTF_8;

public class WrappedByteArray implements SpecialByteBuffer {

    private final byte[] data;
    private final int offset;
    private final int length;
    private final int hashCode;

    public WrappedByteArray(byte[] data, int offset, int length) {
        this.data = data;
        this.offset = offset;
        this.length = length;
        this.hashCode = this.defaultHashCode();
    }

    @Override
    public int offset() {
        return offset;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public String decodeToString() {
        return new String(data, offset, length, UTF_8);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SpecialByteBuffer otherBuffer) {
            return this.equals(otherBuffer);
        }
        return false;
    }

    @Override
    public String toString() {
        return "ReadonlyByteArrayBuffer [" + this.decodeToString() + "]";
    }
}

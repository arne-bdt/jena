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

import java.io.IOException;
import java.io.InputStream;

public class StreamBufferRoot {

    /**
     * Input stream from which the data is read.
     * This stream is expected to be used by the child buffers to read data.
     */
    InputStream inputStream;

    /**
     * The byte array buffer that holds the data read from the input stream.
     */
    final byte[] buffer;

    /**
     * The position in the buffer where the next byte will be read.
     */
    int position = 0;

    /**
     * This marks the position to which the buffer is filled.
     */
    protected int filledToExclusive = 0;

    public StreamBufferRoot(final int maxBufferSize) {
        if (maxBufferSize <= 0) {
            throw new IllegalArgumentException("Buffer size must be greater than zero");
        }
        this.buffer = new byte[maxBufferSize];
        this.position = 0;
        this.filledToExclusive = 0;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public void setInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    public boolean hasRemainingCapacity() {
        return filledToExclusive < buffer.length;
    }

    public void copyRemainingBytesToStart() {
        System.arraycopy(buffer, position, buffer, 0, filledToExclusive - position);
        this.filledToExclusive -= position;
        this.position = 0;
    }

    protected boolean tryFillFromInputStream() throws IOException {
        if (hasRemainingCapacity()) {
            var bytesRead = inputStream.read(this.buffer, filledToExclusive,
                    buffer.length - filledToExclusive);
            if (bytesRead == -1) {
                return false;
            }
            filledToExclusive += bytesRead;
            return true;
        }
        return false;
    }
}

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

import static org.apache.jena.cimxml.utils.ParserConstants.*;

public class QNameByteBuffer extends StreamBufferChild {
    private int startOfLocalPart = 0; // Index where the local part starts

    public QNameByteBuffer(StreamBufferRoot parent) {
        super(parent);
    }

    @Override
    public void reset() {
        super.reset();
        this.startOfLocalPart = 0;
    }

    public boolean hasPrefix() {
        return startOfLocalPart != 0; // If local part starts after the first byte, it has a prefix
    }

    public WrappedByteArray getPrefix() {
        return new WrappedByteArray(root.buffer, start, startOfLocalPart - start - 1); // Exclude the colon
    }

    public WrappedByteArray getLocalPart() {
        return new WrappedByteArray(root.buffer, startOfLocalPart, endExclusive - startOfLocalPart);
    }

    @Override
    protected void afterConsumeCurrent() {
        if (root.buffer[root.position] == DOUBLE_COLON) {
            startOfLocalPart = root.position + 1; // Set the start of local part after the colon
        }
    }

    public boolean tryConsumeToEndOfTagName() throws IOException {
        if (root.position >= root.filledToExclusive) {
            if (!root.tryFillFromInputStream()) {
                return false; // No more data to read
            }
        }
        while (root.position < root.filledToExclusive) {
            if (isEndOfTagName(root.buffer[root.position])) {
                return true;
            }
            afterConsumeCurrent();
            if (++root.position == root.filledToExclusive) {
                if (!root.tryFillFromInputStream()) {
                    return false; // No more data to read
                }
            }
        }
        return false; // Byte not found
    }

    public boolean tryConsumeUntilNonWhitespace() throws IOException {
        if (root.position >= root.filledToExclusive) {
            if (!root.tryFillFromInputStream()) {
                return false; // No more data to read
            }
        }
        while (root.position < root.filledToExclusive) {
            if (!isWhitespace(root.buffer[root.position])) {
                // no need to call afterConsumeCurrent here, as we are skipping whitespace
                return true;
            }
            afterConsumeCurrent(); // Consume the current byte
            if (++root.position == root.filledToExclusive) {
                if (!root.tryFillFromInputStream()) {
                    return false; // No more data to read
                }
            }
        }
        return false; // Byte not found
    }

    /**
     * Tries to consume until the end of the QName, which is defined as
     * the first whitespace or the equality sign ('=').
     * @return
     * @throws IOException
     */
    public boolean tryConsumeUntilEndOfQName() throws IOException {
        if (root.position >= root.filledToExclusive) {
            if (!root.tryFillFromInputStream()) {
                return false; // No more data to read
            }
        }
        while (root.position < root.filledToExclusive) {
            if (isWhitespace(root.buffer[root.position]) || root.buffer[root.position] == EQUALITY_SIGN) {
                // no need to call afterConsumeCurrent here, as we are skipping whitespace and equality sign
                return true;
            }
            afterConsumeCurrent();
            if (++root.position == root.filledToExclusive) {
                if (!root.tryFillFromInputStream()) {
                    return false; // No more data to read
                }
            }
        }
        return false; // Byte not found
    }
}

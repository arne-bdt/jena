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
import static org.apache.jena.cimxml.utils.ParserConstants.isEndOfTagName;
import static org.apache.jena.cimxml.utils.ParserConstants.isWhitespace;

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
    protected void afterConsumeCurrent(byte currentByte) {
        if (currentByte == DOUBLE_COLON) {
            startOfLocalPart = root.position + 1; // Set the start of local part after the colon
        }
    }

    public boolean tryConsumeToEndOfTagName() throws IOException {
        while (true) {
            if (root.position >= root.filledToExclusive) {
                if (!root.tryFillFromInputStream()) {
                    return false; // No more data to read
                }
            }
            while (root.position < root.filledToExclusive) {
                final byte currentByte = root.buffer[root.position];
                if (isEndOfTagName(currentByte)) {
                    return true;
                }
                afterConsumeCurrent(currentByte);
                root.position++;
            }
        }
    }

    public boolean tryConsumeUntilNonWhitespace() throws IOException {
        while (true) {
            if (root.position >= root.filledToExclusive) {
                if (!root.tryFillFromInputStream()) {
                    return false; // No more data to read
                }
            }
            while (root.position < root.filledToExclusive) {
                final byte currentByte = root.buffer[root.position];
                if (!isWhitespace(currentByte)) {
                    return true;
                }
                afterConsumeCurrent(currentByte);
                root.position++;
            }
        }
    }

    /**
     * Tries to consume until the end of the QName, which is defined as
     * the first whitespace or the equality sign ('=').
     * @return
     * @throws IOException
     */
    public boolean tryConsumeUntilEndOfQName() throws IOException {
        while (true) {
            if (root.position >= root.filledToExclusive) {
                if (!root.tryFillFromInputStream()) {
                    return false; // No more data to read
                }
            }
            while (root.position < root.filledToExclusive) {
                final byte currentByte = root.buffer[root.position];
                if (currentByte == EQUALITY_SIGN || isWhitespace(currentByte) ) {
                    return true;
                }
                afterConsumeCurrent(currentByte);
                root.position++;
            }
        }
    }
}

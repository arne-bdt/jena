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

import static java.nio.charset.StandardCharsets.UTF_8;
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

   public boolean tryConsumeToEndOfTagName() throws IOException {
        while (true) {
            if (root.position >= root.filledToExclusive
                    && !root.tryFillFromInputStream()) {
                return false; // No more data to read
            }
            final int endPos = root.filledToExclusive;
            final var buffer = root.buffer;
            var pos = root.position;
            while (pos < endPos) {
                final byte currentByte = buffer[pos];
                if (isEndOfTagName(currentByte)) {
                    root.position = pos;
                    return true;
                }
                pos++;
                if (currentByte == DOUBLE_COLON) {
                    startOfLocalPart = pos; // Set the start of local part after the colon
                }
            }
            root.position = pos;
        }
    }

    public boolean tryForwardToRightAngleBracket() throws IOException {
        while (true) {
            if (root.position >= root.filledToExclusive
                    && !root.tryFillFromInputStream()) {
                return false; // No more data to read
            }
            final int endPos = root.filledToExclusive;
            while (root.position < endPos) {
                if (root.buffer[root.position] == RIGHT_ANGLE_BRACKET) {
                    return true;
                }
                root.position++;
            }
        }
    }

    public boolean tryConsumeUntilNonWhitespace() throws IOException {
        while (true) {
            if (root.position >= root.filledToExclusive
                    && !root.tryFillFromInputStream()) {
                return false; // No more data to read
            }
            final int endPos = root.filledToExclusive;
            while (root.position < endPos) {
                if (!isWhitespace(root.buffer[root.position])) {
                    return true;
                }
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
            if (root.position >= root.filledToExclusive
                    && !root.tryFillFromInputStream()) {
                return false; // No more data to read
            }
            final int endPos = root.filledToExclusive;
            while (root.position < endPos) {
                final byte currentByte = root.buffer[root.position];
                if (currentByte == EQUALITY_SIGN || isWhitespace(currentByte) ) {
                    return true;
                }
                root.position++;
                if (currentByte == DOUBLE_COLON) {
                    startOfLocalPart = root.position; // Set the start of local part after the colon
                }
            }
        }
    }

    @Override
    public String decodeToString() {
        return new String(this.getData(), this.offset(), this.length(), UTF_8);
    }
}

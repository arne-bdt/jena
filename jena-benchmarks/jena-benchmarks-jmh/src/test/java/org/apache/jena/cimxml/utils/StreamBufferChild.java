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
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.jena.cimxml.utils.ParserConstants.END_OF_STREAM;

public abstract class StreamBufferChild implements SpecialByteBuffer {
    /**
     * The root buffer that this child belongs to.
     * This is used to access the input stream for reading data.
     * It also holds the last used buffer, which is used to handover remaining bytes.
     */
    protected final StreamBufferRoot root;
    /**
     * The offset in the buffer where the data starts.
     */
    protected int start = 0;
    /**
     * Marks the end of relevant data in the buffer.
     */
    protected int endExclusive = 0;

    public StreamBufferChild(StreamBufferRoot parent) {
        if (parent == null) {
            throw new IllegalArgumentException("Parent buffer cannot be null");
        }
        this.root = parent;
    }

    public void reset() {
        this.start = 0;
    }

    public void setCurrentByteAsStartPositon() {
        this.start = this.root.position;
    }

    public void setNextByteAsStartPositon() {
        this.start = this.root.position + 1;
    }

    public void setEndPositionExclusive() {
        this.endExclusive = this.root.position;
    }

    public boolean tryForwardAndSetStartPositionAfter(byte byteToSeek) throws IOException {
        if(tryForwardToByte(byteToSeek)) {
            setNextByteAsStartPositon();
            return  true;
        }
        return false;
    }

    public boolean tryForwardAndSetEndPositionExclusive(byte byteToSeek) throws IOException {
        if(tryForwardToByte(byteToSeek)) {
            setEndPositionExclusive();
            return  true;
        }
        return false;
    }

    protected abstract void afterConsumeCurrent(byte currentByte);

    public boolean tryForwardToByte(byte byteToSeek) throws IOException {
        while (true) {
            if (root.position >= root.filledToExclusive) {
                if (!root.tryFillFromInputStream()) {
                    return false; // No more data to read
                }
            }
            while (root.position < root.filledToExclusive) {
                final byte currentByte = root.buffer[root.position];
                if (currentByte == byteToSeek) {
                    return true;
                }
                afterConsumeCurrent(currentByte);
                root.position++;
            }
        }
    }

    public boolean tryForwardToByteAfter(byte byteToSeek) throws IOException {
        boolean found = tryForwardToByte(byteToSeek);
        root.position++;
        return found;
    }



    public byte peek() throws IOException {
        if (root.position >= root.filledToExclusive) {
            if (!root.tryFillFromInputStream()) {
                return END_OF_STREAM;
            }
        }
        return root.buffer[root.position];
    }

    /**
     * Reads the next byte from the buffer and advances the position.
     *
     * @return the next byte in the buffer
     * @throws IOException if an I/O error occurs while reading from the input stream
     */
    public byte next() throws IOException {
        root.position++;
        return peek();
    }

    /**
     * Skips the current byte and moves to the next one.
     * This does not change the start or end positions.
     *
     * @throws IOException if an I/O error occurs while reading from the input stream
     */
    public void skip() throws IOException {
        root.position++;
        peek();
    }

    /**
     * Skips the current byte and moves to the next one.
     * This does not change the start or end positions.
     *
     * @throws IOException if an I/O error occurs while reading from the input stream
     */
    public void skip(int bytesToSkip) throws IOException {
        root.position += bytesToSkip;
        peek();
    }

    @Override
    public int offset() {
        return start;
    }

    @Override
    public int length() {
        return endExclusive - start;
    }

    @Override
    public byte[] getData() {
        return this.root.buffer;
    }

    @Override
    public String toString() {
        String text;
        if (start == 0 && endExclusive == 0) {
            text = "Start at 0:[" +
                    new String(root.buffer, start, root.position - start + 1, UTF_8)
                    + "]--> end not defined yet";
        } else if (start > endExclusive) {
            if (start < root.position) {
                text = new String(root.buffer, start, root.position - start + 1, UTF_8)
                        + "][--> end not defined yet";
            } else {
                text = new String(root.buffer, start, 1, UTF_8)
                        + "][--> end not defined yet";
            }
        } else {
            text = this.decodeToString();
        }
        return "StreamBufferChild [" + text + "]";
    }

    public String wholeBufferToString() {
        return new String(this.root.buffer, 0, this.root.filledToExclusive, UTF_8);
    }

    public String remainingBufferToString() {
        return new String(this.root.buffer, root.position, root.filledToExclusive - root.position, UTF_8);
    }
}

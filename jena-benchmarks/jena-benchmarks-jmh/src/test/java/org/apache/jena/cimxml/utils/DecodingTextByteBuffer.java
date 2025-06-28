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

import static org.apache.jena.cimxml.utils.ParserConstants.*;

public class DecodingTextByteBuffer extends StreamBufferChild {
    protected int lastAmpersandPosition = -1; // Position of the last '&' character, used for decoding

    public DecodingTextByteBuffer(StreamBufferRoot parent) {
        super(parent);
    }

    @Override
    public void reset() {
        super.reset();
        this.lastAmpersandPosition = -1;
    }

    @Override
    protected void afterConsumeCurrent(byte currentByte) {
        switch (currentByte) {
            case AMPERSAND -> lastAmpersandPosition = root.position; // Store the position of the last '&'
            case SEMICOLON -> {
                var charsBetweenAmpersandAndSemicolon = root.position - lastAmpersandPosition - 1;
                switch (charsBetweenAmpersandAndSemicolon) {
                    case 2: {
                        if (root.buffer[lastAmpersandPosition + 2] == 't') {
                            if (root.buffer[lastAmpersandPosition + 1] == 'l') {
                                root.buffer[lastAmpersandPosition] = LEFT_ANGLE_BRACKET; // &lt;

                                // move remaining data to the left
                                System.arraycopy(root.buffer, root.position + 1,
                                        root.buffer, lastAmpersandPosition + 1,
                                        root.filledToExclusive - root.position);

                                root.filledToExclusive -= 3; // Reduce filledToExclusive by 3 for &lt;
                                root.position = lastAmpersandPosition;
                                lastAmpersandPosition = -1; // Reset last ampersand position
                                return;
                            } else if (root.buffer[lastAmpersandPosition + 1] == 'g') {
                                root.buffer[lastAmpersandPosition] = RIGHT_ANGLE_BRACKET; // &gt;

                                // move remaining data to the left
                                System.arraycopy(root.buffer, root.position + 1,
                                        root.buffer, lastAmpersandPosition + 1,
                                        root.filledToExclusive - root.position);

                                root.filledToExclusive -= 3; // Reduce filledToExclusive by 3 for &gt;
                                root.position = lastAmpersandPosition;
                                lastAmpersandPosition = -1; // Reset last ampersand position
                                return;
                            }
                        }
                        break;
                    }
                    case 3: {
                        if (root.buffer[lastAmpersandPosition + 3] == 'p'
                                && root.buffer[lastAmpersandPosition + 2] == 'm'
                                && root.buffer[lastAmpersandPosition + 1] == 'a') {
                            root.buffer[lastAmpersandPosition] = AMPERSAND; // &amp;

                            // move remaining data to the left
                            System.arraycopy(root.buffer, root.position + 1,
                                    root.buffer, lastAmpersandPosition + 1,
                                    root.filledToExclusive - root.position);

                            root.filledToExclusive -= 4; // Reduce filledToExclusive by 4 for &amp;
                            root.position = lastAmpersandPosition;
                            lastAmpersandPosition = -1; // Reset last ampersand position
                            return;
                        }
                        break;
                    }
                    case 4: {
                        if (root.buffer[lastAmpersandPosition + 3] == 'o') {
                            if (root.buffer[lastAmpersandPosition + 1] == 'q'
                                    && root.buffer[lastAmpersandPosition + 2] == 'u'
                                    && root.buffer[lastAmpersandPosition + 4] == 't') {
                                root.buffer[lastAmpersandPosition] = DOUBLE_QUOTE; // &quot;

                                // move remaining data to the left
                                System.arraycopy(root.buffer, root.position + 1,
                                        root.buffer, lastAmpersandPosition + 1,
                                        root.filledToExclusive - root.position);

                                root.filledToExclusive -= 5; // Reduce filledToExclusive by 5 for &quot;
                                root.position = lastAmpersandPosition;
                                lastAmpersandPosition = -1; // Reset last ampersand position
                                return;
                            } else if (root.buffer[lastAmpersandPosition + 2] == 'p'
                                    && root.buffer[lastAmpersandPosition + 4] == 's'
                                    && root.buffer[lastAmpersandPosition + 1] == 'a') {
                                root.buffer[lastAmpersandPosition] = SINGLE_QUOTE; // &apos;

                                // move remaining data to the left
                                System.arraycopy(root.buffer, root.position + 1,
                                        root.buffer, lastAmpersandPosition + 1,
                                        root.filledToExclusive - root.position);

                                root.filledToExclusive -= 5; // Reduce filledToExclusive by 5 for &apos;
                                root.position = lastAmpersandPosition;
                                lastAmpersandPosition = -1; // Reset last ampersand position
                                return;
                            }
                        }
                        break;
                    }
                }
            }
        }
    }

    public boolean tryConsumeToStartOfAttributeValue() throws IOException {
        if (root.position >= root.filledToExclusive) {
            if (!root.tryFillFromInputStream()) {
                return false; // No more data to read
            }
        }
        while (root.position < root.filledToExclusive) {
            if (root.buffer[root.position] == DOUBLE_QUOTE) {
                return true;
            }
            if(!isWhitespace(root.buffer[root.position])) {
                // If we encounter a non-whitespace character before the quote, we stop
                // there is no need to call afterConsumeCurrent since we are not decoding here
                return false;
            }
            // do not consume whitespace, just skip it
            if (++root.position == root.filledToExclusive) {
                if (!root.tryFillFromInputStream()) {
                    return false; // No more data to read
                }
            }
        }
        return false; // Byte not found
    }
}

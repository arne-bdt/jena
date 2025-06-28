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

public class DecodingTextByteBuffer extends StreamBufferChild {
    protected int lastAmpersandPosition = -1; // Position of the last '&' character, used for decoding

    protected int containsSpecialCharacters = 0; // Bitmask to track which special characters are present

    public enum SpecialCharacter {
        AMPERSAND(1,"&", "&amp;"),
        LEFT_ANGLE_BRACKET(2, "<", "&lt;"),
        RIGHT_ANGLE_BRACKET(4, ">", "&gt;"),
        DOUBLE_QUOTE(8, "\"", "&quot;"),
        SINGLE_QUOTE(16, "'", "&apos;");

        private final int bit;
        private final String character;
        private final String entity;

        SpecialCharacter(int bit, String character, String entity) {
            this.bit = bit;
            this.character = character;
            this.entity = entity;
        }

        public String getCharacter() {
            return character;
        }

        public String getEntity() {
            return entity;
        }

        public int getBit() {
            return bit;
        }
    }



    public DecodingTextByteBuffer(StreamBufferRoot parent) {
        super(parent);
    }

    @Override
    public void reset() {
        super.reset();
        this.lastAmpersandPosition = -1;
        this.containsSpecialCharacters = 0; // Reset the special characters bitmask
    }

    private void afterConsumeCurrent(byte currentByte, byte[] buffer, int position) {
        switch (currentByte) {
            case AMPERSAND -> lastAmpersandPosition = position; // Store the position of the last '&'
            case SEMICOLON -> {
                var charsBetweenAmpersandAndSemicolon = position - lastAmpersandPosition - 1;
                switch (charsBetweenAmpersandAndSemicolon) {
                    case 2: {
                        if (buffer[lastAmpersandPosition + 2] == 't') {
                            if (buffer[lastAmpersandPosition + 1] == 'l') {
                                containsSpecialCharacters |= SpecialCharacter.LEFT_ANGLE_BRACKET.getBit(); // &lt;
                                return;
                            } else if (buffer[lastAmpersandPosition + 1] == 'g') {
                                containsSpecialCharacters |= SpecialCharacter.RIGHT_ANGLE_BRACKET.getBit(); // &gt;
                                return;
                            }
                        }
                        break;
                    }
                    case 3: {
                        if (buffer[lastAmpersandPosition + 3] == 'p'
                                && buffer[lastAmpersandPosition + 2] == 'm'
                                && buffer[lastAmpersandPosition + 1] == 'a') {
                            containsSpecialCharacters |= SpecialCharacter.AMPERSAND.getBit(); // &amp;
                        }
                        break;
                    }
                    case 4: {
                        if (buffer[lastAmpersandPosition + 3] == 'o') {
                            if (buffer[lastAmpersandPosition + 1] == 'q'
                                    && buffer[lastAmpersandPosition + 2] == 'u'
                                    && buffer[lastAmpersandPosition + 4] == 't') {
                                containsSpecialCharacters |= SpecialCharacter.DOUBLE_QUOTE.getBit(); // &quot;
                            } else if (buffer[lastAmpersandPosition + 2] == 'p'
                                    && buffer[lastAmpersandPosition + 4] == 's'
                                    && buffer[lastAmpersandPosition + 1] == 'a') {
                                containsSpecialCharacters |= SpecialCharacter.SINGLE_QUOTE.getBit(); // &apos;
                            }
                        }
                        break;
                    }
                }
            }
        }
    }

    public boolean tryConsumeToEndOfAttributeValue() throws IOException {
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
                if (currentByte == DOUBLE_QUOTE) {
                    this.endExclusive = pos++;
                    this.root.position = pos;
                    fillIfNeeded();
                    return true;
                }
                afterConsumeCurrent(currentByte, buffer, pos);
                pos++;
            }
            this.root.position = pos;
        }
    }

    public boolean tryConsumeToEndOfTextContent() throws IOException {
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
                if (currentByte == LEFT_ANGLE_BRACKET) {
                    this.root.position = pos;
                    return true;
                }
                afterConsumeCurrent(currentByte, buffer, pos);
                pos++;
            }
            root.position = pos;
        }
    }

    public boolean tryForwardToStartOfAttributeValue() throws IOException {
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
                if (currentByte == DOUBLE_QUOTE) {
                    this.start = ++pos;
                    this.root.position = pos;
                    fillIfNeeded();
                    return true;
                }
                if (!isWhitespace(currentByte)) {
                    // If we encounter a non-whitespace character before the quote, we stop
                    // there is no need to call afterConsumeCurrent since we are not decoding here
                    this.root.position = pos;
                    return false;
                }
                // do not consume whitespace, just skip it
                pos++;
            }
            this.root.position = pos; // Update the position in the root buffer
        }
    }

    @Override
    public String decodeToString() {
        var result = new String(this.getData(), this.offset(), this.length(), UTF_8);
        if (containsSpecialCharacters == 0) {
            return result; // No special characters to decode
        }
        for (SpecialCharacter specialCharacter : SpecialCharacter.values()) {
            if ((containsSpecialCharacters & specialCharacter.getBit()) != 0) {
                result = result.replace(specialCharacter.getEntity(), specialCharacter.getCharacter());
            }
        }
        return result;
    }
}
